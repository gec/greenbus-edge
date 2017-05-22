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

import io.greenbus.edge.stream._
import io.greenbus.edge.stream.gateway.MapDiffCalc

import scala.collection.mutable.ArrayBuffer

/*

(abstract)
sequencer -> cache -> channel queue

sequencer -> mgr [cache, observable] -> channel queue

(really abstract)
source (synth | sequencer) -> mgr [cache, observable?] -> channel queue (<- pop)

THREE 1/2 PLACES:
- producer (gateway client)
- peer synthesis
  - synth keeps logs around until they become active, then is a filter
  - "retail cache"
- subscriber (volatile link to peer)
  - needs to do deep equality on session change


cache:
producer cache is always the same session/ctx
peer cache changes ctx; could keep multiple contexts around for history resync



 */

class Filter {

}

/*class AppendSequencerIsh[A] {
  private var sequence: Long = 0

  def append(values: A*): Iterable[(Long, A)] = {
    val start = sequence
    val results = values.toIterable.zipWithIndex.map {
      case (v, i) => (start + i, v)
    }
    sequence += values.size
    results
  }
}*/

trait SequenceCache {
  def append(event: SequenceEvent): Unit
}

/*
trait StreamCache {
  def handle(append: AppendEvent): Unit
  def sync(): ResyncSession
}
 */
trait StreamCache {
  def handle(event: AppendEvent): Unit
  def resync(): Seq[AppendEvent]
}

object StreamCacheImpl {
  case class SessionContext(sessionId: PeerSessionId, context: SequenceCtx)

  sealed trait State
  case object Uninit extends State
  case class Init(ctx: SessionContext, current: Resync) extends State
}
class StreamCacheImpl extends StreamCache {
  import StreamCacheImpl._
  private var state: State = Uninit

  def handle(event: AppendEvent): Unit = {
    state match {
      case Uninit => {
        event match {
          case rs: ResyncSession =>
            state = Init(SessionContext(rs.sessionId, rs.context), rs.resync)
          case snap: ResyncSnapshot => throw new IllegalStateException(s"No resync session for unitialized queue")
          case sd: StreamDelta => throw new IllegalStateException(s"No resync session for unitialized queue")
        }
      }
      case st: Init => {
        event match {
          case rs: ResyncSession =>
            state = Init(SessionContext(rs.sessionId, rs.context), rs.resync)
          case snap: ResyncSnapshot =>
            state = Init(st.ctx, snap.resync)
          case sd: StreamDelta =>
            state = Init(st.ctx, StreamTypes.foldResync(st.current, sd.update)) // TODO: infinite append!
        }
      }
    }
  }

  def resync(): Seq[AppendEvent] = {
    state match {
      case Uninit => Seq()
      case Init(ctx, resync) => Seq(ResyncSession(ctx.sessionId, ctx.context, resync))
    }
  }
}

object StreamQueueImpl {
  case class SessionContext(sessionId: PeerSessionId, context: SequenceCtx)

  sealed trait State
  case object Uninit extends State
  case class Idle(ctx: SessionContext) extends State
  case class AccumulatingDeltas(ctx: SessionContext, current: Delta) extends State
  case class Resynced(ctx: SessionContext, current: Resync, prev: ArrayBuffer[AppendEvent]) extends State
}
class StreamQueueImpl {

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
        }
      }
      case st: AccumulatingDeltas => {
        event match {
          case rs: ResyncSession =>
            state = Resynced(SessionContext(rs.sessionId, rs.context), rs.resync, ArrayBuffer(StreamDelta(st.current)))
          case snap: ResyncSnapshot =>
            state = Resynced(st.ctx, snap.resync, ArrayBuffer(StreamDelta(st.current)))
          case sd: StreamDelta =>
            state = AccumulatingDeltas(st.ctx, StreamTypes.foldDelta(st.current, sd.update)) // TODO: infinite append!
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
            state = Resynced(st.ctx, StreamTypes.foldResync(st.current, sd.update), st.prev) // TODO: infinite append!
        }
      }
    }
  }

  def pop(): Seq[AppendEvent] = {
    state match {
      case Uninit => Seq()
      case _: Idle => Seq()
      case st: AccumulatingDeltas =>
        state = Idle(st.ctx)
        Seq(StreamDelta(st.current))
      case st: Resynced =>
        state = Idle(st.ctx)
        st.prev.toVector ++ Seq(ResyncSession(st.ctx.sessionId, st.ctx.context, st.current))
    }
  }
}

/*
object GenSequenceQueue {
  sealed trait QueueState
  case object Uninit extends QueueState
  case class AccumulatingDeltas(current: Delta) extends QueueState
  case class Resynced(current: Resync) extends QueueState
}
class GenSequenceQueue {
  import GenSequenceQueue._

  private var state: QueueState = Uninit

  def delta(delta: Delta): Unit = {
    state = state match {
      case Uninit => AccumulatingDeltas(delta)
      case AccumulatingDeltas(current) => AccumulatingDeltas(StreamTypes.foldDelta(current, delta))
      case Resynced(current) => Resynced(StreamTypes.foldResync(current, delta))
    }
  }

  def resync(resync: Resync): Unit = {
    state = state match {
      case Uninit => Resynced(resync)
      case AccumulatingDeltas(current) => {
        current.diffs.lastOption.map(_.sequence) match {
          case Some(prev) => {
            if (prev.isLessThan(resync.sequence).contains(true)) {
              Resynced(resync)
            } else {
              state
            }
          }
          case None =>
            // Illegal state?
            Resynced(resync)
        }
      }
      case Resynced(current) => Resynced(StreamTypes.foldResync(current, resync))
    }
  }

  def dequeue(): Option[SequenceEvent] = {
    state match {
      case Uninit => None
      case AccumulatingDeltas(current) => Some(current)
      case Resynced(current) => Some(current)
    }
  }
}

trait SequenceFilter {
  def delta(delta: Delta): Option[Delta]
  def resync(resync: Resync): Option[Resync]
}

// TODO: this needs to start uninitialized... probably
class GenSequenceFilter(cid: String, startSequence: SequencedTypeValue) extends SequenceFilter with LazyLogging {

  private var sequence: SequencedTypeValue = startSequence

  def delta(delta: Delta): Option[Delta] = {

    logger.trace(s"$cid got $delta at $sequence")

    var seqVar = sequence

    val passed = delta.diffs.filter { diff =>
      if (seqVar.precedes(diff.sequence)) {
        seqVar = seqVar.next
        true
      } else {
        false
      }
    }

    sequence = seqVar

    if (passed.nonEmpty) {
      Some(Delta(passed))
    } else {
      None
    }
  }

  def resync(resync: Resync): Option[Resync] = {

    logger.debug(s"$cid got $resync at $sequence")

    if (resync.sequence == sequence) {
      None
    } else if (sequence.precedes(resync.sequence)) {
      // Translating sequential resyncs into a delta would go here
      sequence = resync.sequence
      Some(resync)
    } else if (sequence.isLessThan(resync.sequence).contains(true)) {
      sequence = resync.sequence
      Some(resync)
    } else {
      None
    }
  }
}

 */
