/**
 * Copyright 2011-2017 Green Energy Corp.
 *
 * Licensed to Green Energy Corp (www.greenenergycorp.com) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. Green Energy
 * Corp licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.greenbus.edge.stream.filter

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.stream._

import scala.collection.mutable.ArrayBuffer

trait SequenceCache {
  def append(event: SequenceEvent): Unit
}

trait StreamCache {
  def handle(event: AppendEvent): Unit
  def resync(): Seq[AppendEvent]
}

object StreamCacheImpl {
  case class SessionContext(sessionId: PeerSessionId, context: SequenceCtx)

  sealed trait State
  case object Uninit extends State
  case class InitSessioned(ctx: SessionContext, current: Resync) extends State
  case object InitAbsent extends State
}
class StreamCacheImpl(appendLimitDefault: Int) extends StreamCache {
  import StreamCacheImpl._
  private var state: State = Uninit

  def handle(event: AppendEvent): Unit = {
    state match {
      case Uninit => {
        event match {
          case rs: ResyncSession =>
            state = InitSessioned(SessionContext(rs.sessionId, rs.context), rs.resync)
          case snap: ResyncSnapshot => throw new IllegalStateException(s"No resync session for unitialized cache")
          case sd: StreamDelta => throw new IllegalStateException(s"No resync session for unitialized cache")
          case StreamAbsent =>
            state = InitAbsent
        }
      }
      case st: InitSessioned => {
        event match {
          case rs: ResyncSession =>
            state = InitSessioned(SessionContext(rs.sessionId, rs.context), rs.resync)
          case snap: ResyncSnapshot =>
            state = InitSessioned(st.ctx, snap.resync)
          case sd: StreamDelta =>
            state = InitSessioned(st.ctx, PresequencedStreamOps.foldResync(st.current, sd.update, appendLimit = appendLimitDefault)) // TODO: infinite append!
          case StreamAbsent =>
            state = InitAbsent
        }
      }
      case InitAbsent => {
        event match {
          case rs: ResyncSession =>
            state = InitSessioned(SessionContext(rs.sessionId, rs.context), rs.resync)
          case snap: ResyncSnapshot => throw new IllegalStateException(s"No resync session for absent cache")
          case sd: StreamDelta => throw new IllegalStateException(s"No resync session for absent cache")
          case StreamAbsent =>
        }
      }
    }
  }

  def resync(): Seq[AppendEvent] = {
    state match {
      case Uninit => Seq()
      case InitSessioned(ctx, resync) => Seq(ResyncSession(ctx.sessionId, ctx.context, resync))
      case InitAbsent => Seq(StreamAbsent)
    }
  }
}

trait StreamQueue {
  def handle(event: AppendEvent): Unit
  def dequeue(): Seq[AppendEvent]
}

object StreamQueueImpl {
  case class SessionContext(sessionId: PeerSessionId, context: SequenceCtx)

  sealed trait State
  case object Uninit extends State
  case class Idle(ctx: SessionContext) extends State
  case class AccumulatingDeltas(ctx: SessionContext, current: Delta) extends State
  case class Resynced(ctx: SessionContext, current: Resync, prev: ArrayBuffer[AppendEvent]) extends State

  case object AbsentIdle extends State
  case class Absented(prev: Seq[AppendEvent]) extends State
}
class StreamQueueImpl(appendLimitDefault: Int) extends StreamQueue with LazyLogging {

  import StreamQueueImpl._

  private var state: State = Uninit

  def handle(event: AppendEvent): Unit = {
    state match {
      case Uninit => {
        event match {
          case rs: ResyncSession =>
            state = Resynced(SessionContext(rs.sessionId, rs.context), rs.resync, ArrayBuffer())
          case snap: ResyncSnapshot => throw new IllegalStateException(s"No resync session for unitialized queue")
          case sd: StreamDelta => throw new IllegalStateException(s"No resync session for unitialized queue")
          case StreamAbsent =>
            state = Absented(Seq())
        }
      }
      case st: Idle => {
        event match {
          case rs: ResyncSession =>
            state = Resynced(SessionContext(rs.sessionId, rs.context), rs.resync, ArrayBuffer())
          case snap: ResyncSnapshot =>
            state = Resynced(st.ctx, snap.resync, ArrayBuffer())
          case sd: StreamDelta =>
            state = AccumulatingDeltas(st.ctx, sd.update)
          case StreamAbsent =>
            state = Absented(Seq())
        }
      }
      case st: AccumulatingDeltas => {
        event match {
          case rs: ResyncSession =>
            state = Resynced(SessionContext(rs.sessionId, rs.context), rs.resync, ArrayBuffer(StreamDelta(st.current)))
          case snap: ResyncSnapshot =>
            state = Resynced(st.ctx, snap.resync, ArrayBuffer(StreamDelta(st.current)))
          case sd: StreamDelta =>
            state = AccumulatingDeltas(st.ctx, PresequencedStreamOps.foldDelta(st.current, sd.update)) // TODO: infinite append!
          case StreamAbsent =>
            state = Absented(Seq(StreamDelta(st.current)))
        }
      }
      case st: Resynced => {
        event match {
          case rs: ResyncSession =>
            st.prev += ResyncSession(st.ctx.sessionId, st.ctx.context, st.current)
            state = Resynced(SessionContext(rs.sessionId, rs.context), rs.resync, st.prev)
          case snap: ResyncSnapshot =>
            st.prev += ResyncSession(st.ctx.sessionId, st.ctx.context, st.current)
            state = Resynced(st.ctx, snap.resync, st.prev)
          case sd: StreamDelta =>
            state = Resynced(st.ctx, PresequencedStreamOps.foldResync(st.current, sd.update, appendLimitDefault), st.prev) // TODO: infinite append!
          case StreamAbsent =>
            state = Absented(st.prev :+ ResyncSession(st.ctx.sessionId, st.ctx.context, st.current))
        }
      }
      case AbsentIdle =>
        event match {
          case rs: ResyncSession =>
            state = Resynced(SessionContext(rs.sessionId, rs.context), rs.resync, ArrayBuffer())
          case snap: ResyncSnapshot => logger.warn(s"No resync session for absent queue")
          case sd: StreamDelta => logger.warn(s"No resync session for absent queue")
          case StreamAbsent =>
            state = Absented(Seq())
        }
      case st: Absented =>
        event match {
          case rs: ResyncSession =>
            state = Resynced(SessionContext(rs.sessionId, rs.context), rs.resync, ArrayBuffer(st.prev :+ StreamAbsent: _*))
          case snap: ResyncSnapshot => logger.warn(s"No resync session for absent queue")
          case sd: StreamDelta => logger.warn(s"No resync session for absent queue")
          case StreamAbsent =>
        }
    }
  }

  def dequeue(): Seq[AppendEvent] = {
    state match {
      case Uninit => Seq()
      case _: Idle => Seq()
      case st: AccumulatingDeltas =>
        state = Idle(st.ctx)
        Seq(StreamDelta(st.current))
      case st: Resynced =>
        state = Idle(st.ctx)
        st.prev.toVector ++ Seq(ResyncSession(st.ctx.sessionId, st.ctx.context, st.current))
      case AbsentIdle => Seq()
      case st: Absented =>
        state = AbsentIdle
        st.prev :+ StreamAbsent
    }
  }
}
