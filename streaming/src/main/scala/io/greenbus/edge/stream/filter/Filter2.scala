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

case class AppendPublish(key: TableRow, values: Seq[TypeValue])
case class SetPublish(key: TableRow, value: Set[TypeValue])
case class MapPublish(key: TableRow, value: Map[TypeValue, TypeValue])
case class PublishBatch(
  route: TypeValue,
  appendUpdates: Seq[AppendPublish],
  mapUpdates: Seq[MapPublish],
  setUpdates: Seq[SetPublish])

class RoutePublishSequencer(appends: Map[TableRow, AppendSequencer], sets: Map[TableRow, SetSequencer], maps: Map[TableRow, MapSequencer]) {

  def handleBatch(batch: PublishBatch): Seq[RowAppendEvent] = {

    val appendEvents = batch.appendUpdates.flatMap { update =>
      appends.get(update.key)
        .map(_.handle(update.values))
        .getOrElse(Seq())
        .map(ev => RowAppendEvent(update.key.toRowId(batch.route), ev))
    }
    val setEvents = batch.setUpdates.flatMap { update =>
      sets.get(update.key)
        .map(_.handle(update.value))
        .getOrElse(Seq())
        .map(ev => RowAppendEvent(update.key.toRowId(batch.route), ev))
    }
    val mapEvents = batch.mapUpdates.flatMap { update =>
      maps.get(update.key)
        .map(_.handle(update.value))
        .getOrElse(Seq())
        .map(ev => RowAppendEvent(update.key.toRowId(batch.route), ev))
    }

    appendEvents ++ setEvents ++ mapEvents
  }
}

class AppendSequencer(session: PeerSessionId, ctx: SequenceCtx) {
  private var sequence: Long = 0
  private var uninit = true

  def handle(values: Seq[TypeValue]): Seq[AppendEvent] = {
    assert(values.nonEmpty)

    val start = sequence
    val results = values.zipWithIndex.map {
      case (v, i) => (Int64Val(start + i), v)
    }
    sequence += values.size

    val diffs = results.map { case (i, v) => SequencedDiff(i, AppendValue(v)) }

    if (uninit) {
      val last = diffs.last
      val init = diffs.init
      val snap = AppendSnapshot(last, init)
      uninit = false
      Seq(ResyncSession(session, ctx, Resync(last.sequence, snap)))
    } else {
      Seq(StreamDelta(Delta(diffs)))
    }
  }
}

object SetSequencer {

  def diff(next: Set[TypeValue], prev: Set[TypeValue]): SetDiff = {
    val added = next -- prev
    val removed = prev -- next
    SetDiff(removes = removed, adds = added)
  }

  def toDelta(seq: Long, diff: SetDiff): Delta = {
    Delta(Seq(SequencedDiff(Int64Val(seq), diff)))
  }

  def toSnapshot(seq: Long, current: Set[TypeValue]): Resync = {
    Resync(Int64Val(seq), SetSnapshot(current))
  }
}
class SetSequencer(session: PeerSessionId, ctx: SequenceCtx) {
  import SetSequencer._

  private var sequence: Long = 0
  private var prevOpt = Option.empty[Set[TypeValue]]

  def handle(value: Set[TypeValue]): Seq[AppendEvent] = {
    val seq = sequence
    sequence += 1

    prevOpt match {
      case None =>
        Seq(ResyncSession(session, ctx, SetSequencer.toSnapshot(seq, value)))
      case Some(prev) => {
        Seq(StreamDelta(toDelta(seq, diff(value, prev))))
      }
    }
  }
}

object MapSequencer {

  def diff(next: Map[TypeValue, TypeValue], prev: Map[TypeValue, TypeValue]): MapDiff = {
    val (removed, added, modified) = MapDiffCalc.calculate(prev, next)
    MapDiff(removed, added, modified)
  }

  def toDelta(seq: Long, diff: MapDiff): Delta = {
    Delta(Seq(SequencedDiff(Int64Val(seq), MapDiff(diff.removes, diff.adds, diff.modifies))))
  }

  def toSnapshot(seq: Long, current: Map[TypeValue, TypeValue]): Resync = {
    Resync(Int64Val(seq), MapSnapshot(current))
  }
}
class MapSequencer(session: PeerSessionId, ctx: SequenceCtx) {
  import MapSequencer._

  private var sequence: Long = 0
  private var prevOpt = Option.empty[Map[TypeValue, TypeValue]]

  def handle(value: Map[TypeValue, TypeValue]): Seq[AppendEvent] = {
    val seq = sequence
    sequence += 1

    prevOpt match {
      case None =>
        Seq(ResyncSession(session, ctx, toSnapshot(seq, value)))
      case Some(prev) => {
        Seq(StreamDelta(toDelta(seq, diff(value, prev))))
      }
    }
  }
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
  def resync(): RowEvent
}

object StreamCacheImpl {
  case class SessionContext(sessionId: PeerSessionId, context: SequenceCtx)

  sealed trait State
  case object Uninit extends State
  case class Init(ctx: SessionContext, current: Resync) extends State
}
class StreamCacheImpl {
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
