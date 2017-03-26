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
package io.greenbus.edge.colset

import com.typesafe.scalalogging.LazyLogging

//import scala.collection.mutable

object StreamTypes extends LazyLogging {

  def filterSequencedDiffs(from: SequencedTypeValue, seq: Seq[SequencedDiff]): Seq[SequencedDiff] = {
    var seqVar = from
    seq.filter { diff =>
      if (seqVar.precedes(diff.sequence)) {
        seqVar = seqVar.next
        true
      } else {
        false
      }
    }
  }

  def foldDelta(left: Delta, right: Delta): Delta = {
    if (left.diffs.nonEmpty) {
      val lastOfLeft = left.diffs.last.sequence
      val appends = filterSequencedDiffs(lastOfLeft, right.diffs)
      Delta(left.diffs ++ appends)
    } else {
      right
    }
  }
  def foldResync(left: Resync, right: Delta): Resync = {
    left.snapshot match {
      case s: SetSnapshot => {
        val (snap, seq) = foldSetSnapshot(s, left.sequence, right)
        left.copy(sequence = seq, snapshot = snap)
      }
      case s: MapSnapshot => {
        val (snap, seq) = foldMapSnapshot(s, left.sequence, right)
        left.copy(sequence = seq, snapshot = snap)
      }
      case s: AppendSnapshot => {
        val result = foldAppendSnapshot(s, right)
        left.copy(sequence = result.current.sequence, snapshot = result)
      }
    }
  }
  def foldResync(left: Resync, right: Resync): Resync = {
    if (right.sequence == left.sequence) {
      left
    } else if (left.sequence.precedes(right.sequence)) {
      // Translating sequential resyncs into a delta would go here
      right
    } else if (left.sequence.isLessThan(right.sequence).contains(true)) {
      right
    } else {
      left
    }
  }

  def foldSetSnapshot(snap: SetSnapshot, current: SequencedTypeValue, right: Delta): (SetSnapshot, SequencedTypeValue) = {
    var seqVar = current
    var set = snap.snapshot
    right.diffs.foreach { seqDiff =>
      if (seqVar.precedes(seqDiff.sequence)) {
        seqDiff.diff match {
          case diff: SetDiff =>
            set = (set -- diff.removes) ++ diff.adds
            seqVar = seqVar.next
          case _ =>
            // TODO: exception?
            logger.error(s"Incorrect diff type for SetSnapshot")
        }
      }
    }
    (SetSnapshot(set), seqVar)
  }
  def foldMapSnapshot(snap: MapSnapshot, current: SequencedTypeValue, right: Delta): (MapSnapshot, SequencedTypeValue) = {
    var seqVar = current
    var set = snap.snapshot
    right.diffs.foreach { seqDiff =>
      if (seqVar.precedes(seqDiff.sequence)) {
        seqDiff.diff match {
          case diff: MapDiff =>
            set = (set -- diff.removes) ++ diff.adds ++ diff.modifies
            seqVar = seqVar.next
          case _ =>
            // TODO: exception?
            logger.error(s"Incorrect diff type for SetSnapshot")
        }
      }
    }
    (MapSnapshot(set), seqVar)
  }

  def foldAppendSnapshot(snap: AppendSnapshot, right: Delta): AppendSnapshot = {
    val appends = filterSequencedDiffs(snap.current.sequence, right.diffs)

    if (appends.nonEmpty) {
      val history = snap.previous ++ Vector(snap.current) ++ appends.init
      AppendSnapshot(appends.last, history)

      /*appends.last.diff match {
        case lastValue: AppendValue =>
          val history = snap.previous ++ Vector(snap.current) ++ appends.init
          val endSeq = appends.last.sequence
          AppendSnapshot(appends.last, history)
        case _ =>
          // TODO: exception?
          logger.error(s"Incorrect diff type for AppendSnapshot")
      }*/
    } else {
      snap
    }

  }

  def reduceSequenceEvent(params: StreamParams, event: SequenceEvent): SequenceEvent = {
    // Drop appends to window size
    // Modify set/map diffs?
    ???
  }
}

trait StreamCache {
  def handle(append: AppendEvent): Unit
  def sync(): ResyncSession
}

// TODO: the GenStandbyStreamQueue *is* a cache, do this reorg
class RetailStreamCache(cid: String, startInit: ResyncSession) extends StreamCache {
  private val queue = new GenStandbyStreamQueue(cid, startInit)

  def handle(append: AppendEvent): Unit = {
    queue.append(append)
  }

  def sync(): ResyncSession = {
    queue.dequeue()
  }
}

trait StreamQueue {
  def append(event: AppendEvent): Unit
  def dequeue(): Seq[AppendEvent]
}

trait StandbyStreamQueue {
  def append(event: AppendEvent): Unit
  def dequeue(): ResyncSession
}

/*object GenStandbyStreamQueue {
  sealed trait QueueState
  case object Uninit extends QueueState
  case class SequencingSession()
}*/
class GenStandbyStreamQueue(cid: String, startInit: ResyncSession) extends StandbyStreamQueue {

  private var session = startInit.sessionId
  private var ctx = startInit.context
  private var queue = new GenResyncSequenceQueue(startInit.resync)

  def append(event: AppendEvent): Unit = {
    event match {
      case StreamDelta(delta) => queue.delta(delta)
      case ResyncSnapshot(resync) => queue.resync(resync)
      case resyncSession: ResyncSession => {
        if (resyncSession.sessionId != session) {
          session = resyncSession.sessionId
          ctx = resyncSession.context
          queue = new GenResyncSequenceQueue(resyncSession.resync)
        } else {
          queue.resync(resyncSession.resync)
        }
      }
    }
  }

  def dequeue(): ResyncSession = {
    ResyncSession(session, ctx, queue.dequeue())
  }
}

trait StreamFilter {
  def handle(event: AppendEvent): Option[AppendEvent]
}

class GenInitializedStreamFilter(cid: String, startInit: ResyncSession) extends StreamFilter with LazyLogging {

  private var session = startInit.sessionId
  //private var ctx = startInit.init

  private var seqFilter = new GenSequenceFilter(cid, startInit.resync.sequence)

  def handle(event: AppendEvent): Option[AppendEvent] = {
    event match {
      case StreamDelta(delta) => seqFilter.delta(delta).map(StreamDelta)
      case ResyncSnapshot(resync) => seqFilter.resync(resync).map(ResyncSnapshot)
      case resyncSession: ResyncSession => {
        if (resyncSession.sessionId != session) {
          session = resyncSession.sessionId
          seqFilter = new GenSequenceFilter(cid, resyncSession.resync.sequence)

          Some(resyncSession)
        } else {
          seqFilter.resync(resyncSession.resync).map(up => resyncSession.copy(resync = up))
        }
      }
    }
  }

  /*private def handleSameSessionResync(resyncCtx: SequenceCtx, resync: Resync): Option[SequenceCtx] = {
    seqFilter.resync(resync) match {
      case FilterResyncResult.Previous => None
      case FilterResyncResult.Event(ev) => Some(init.copy(resync = ev))
      case FilterResyncResult.Same => {
        if (resyncCtx != ctx) {
          ctx = resyncCtx
          Some(init)
        } else {
          None
        }
      }
    }
  }*/

}

/*
class GenInitializedStreamFilter(cid: String, startInit: SequenceInit) extends StreamFilter {

  private var params = startInit.params
  private var meta = startInit.userMetadata

  private val seqFilter = new GenSequenceFilter(cid, startInit.resync.sequence)

  def handle(event: AppendEvent): Option[AppendEvent] = {
    event match {
      case StreamDelta(delta) => seqFilter.delta(delta).map(StreamDelta)
      case ResyncSnapshot(resync) => seqFilter.resync(resync).toEvent.map(ResyncSnapshot)
      //case init: Reinitialize => handleReinit(init.init).map(Reinitialize)
      case sess: ResyncSession => handleReinit(sess.init).map(ev => sess.copy(init = ev))
    }
  }

  private def handleReinit(init: SequenceInit): Option[SequenceInit] = {
    seqFilter.resync(init.resync) match {
      case FilterResyncResult.Previous => None
      case FilterResyncResult.Event(ev) => Some(init.copy(resync = ev))
      case FilterResyncResult.Same => {
        if (init.params != params || init.userMetadata != meta) {
          params = init.params
          meta = init.userMetadata
          Some(init)
        } else {
          None
        }
      }
    }
  }

}*/

trait SequenceCache {
  def sync(): Resync
}

trait SequenceQueue {
  def delta(delta: Delta): Unit
  def resync(resync: Resync): Unit
  def dequeue(): SequenceEvent
}

trait ResyncSequenceQueue {
  def delta(delta: Delta): Unit
  def resync(resync: Resync): Unit
  def dequeue(): Resync
}
class GenResyncSequenceQueue(init: Resync) extends ResyncSequenceQueue {
  private val queue = new GenSequenceQueue
  queue.resync(init)

  def delta(delta: Delta): Unit = {
    queue.delta(delta)
  }

  def resync(resync: Resync): Unit = {
    queue.resync(resync)
  }

  def dequeue(): Resync = {
    // TODO: fix this by abstracting gensequencequeue
    queue.dequeue() match {
      case None => init
      case Some(v) =>
        v match {
          case r: Resync => r
          case _ => init
        }
    }
  }
}

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

/*
sealed trait FilterResyncResult {
  def toEvent: Option[Resync]
}
object FilterResyncResult {
  case object Previous extends FilterResyncResult {
    def toEvent: Option[Resync] = None
  }
  case object Same extends FilterResyncResult {
    def toEvent: Option[Resync] = None
  }
  case class Event(ev: Resync) extends FilterResyncResult {
    def toEvent: Option[Resync] = Some(ev)
  }
}

// TODO: this needs to start uninitialized... probably
class GenSequenceFilter(cid: String, startSequence: SequencedTypeValue) extends SequenceFilter with LazyLogging {

  private var sequence: SequencedTypeValue = startSequence

  def delta(delta: Delta): Option[Delta] = {
    var seqVar = sequence

    val passed = delta.diffs.filter { diff =>
      if (seqVar.precedes(diff.sequence)) {
        seqVar = seqVar.next
        true
      } else {
        false
      }
    }

    if (passed.nonEmpty) {
      Some(Delta(passed))
    } else {
      None
    }
  }

  def resync(resync: Resync): FilterResyncResult = {
    if (resync.sequence == sequence) {
      FilterResyncResult.Same
    } else if (sequence.precedes(resync.sequence)) {
      // Translating sequential resyncs into a delta would go here
      sequence = resync.sequence
      FilterResyncResult.Event(resync)
    } else if (sequence.isLessThan(resync.sequence).contains(true)) {
      sequence = resync.sequence
      FilterResyncResult.Event(resync)
    } else {
      FilterResyncResult.Previous
    }
  }
}*/
