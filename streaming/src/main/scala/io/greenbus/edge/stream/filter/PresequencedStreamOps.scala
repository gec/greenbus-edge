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

object FilteringStreamOps extends LazyLogging {

  def mapDiffCalc[A, B](prev: Map[A, B], next: Map[A, B]): (Set[A], Set[(A, B)], Set[(A, B)]) = {
    val removed = prev.keySet -- next.keySet

    val added = Set.newBuilder[(A, B)]
    val modified = Set.newBuilder[(A, B)]
    next.foreach {
      case (k, v) =>
        prev.get(k) match {
          case None => added += (k -> v)
          case Some(lastV) =>
            if (lastV != v) {
              modified += (k -> v)
            }
        }
    }

    (removed, added.result(), modified.result())
  }

  def synthResyncSingleStep(current: SequenceSnapshot, resync: Resync): (SequenceSnapshot, SequenceEvent) = {
    current match {
      case l: SetSnapshot => {
        resync.snapshot match {
          case r: SetSnapshot =>
            val removes = l.snapshot -- r.snapshot
            val adds = r.snapshot -- l.snapshot
            (r, Delta(Seq(SequencedDiff(resync.sequence, SetDiff(removes, adds)))))
          case _ =>
            (resync.snapshot, resync)
        }
      }
      case s: MapSnapshot =>
        resync.snapshot match {
          case r: MapSnapshot =>
            val (removes, adds, modifies) = mapDiffCalc(s.snapshot, r.snapshot)
            (r, Delta(Seq(SequencedDiff(resync.sequence, MapDiff(removes, adds, modifies)))))
          case _ =>
            (resync.snapshot, resync)
        }
      case s: AppendSnapshot =>
        resync.snapshot match {
          case r: AppendSnapshot =>
            (r, Delta(Seq(r.current)))
        }
    }
  }

  def synthSequenceJump(sequence: SequencedTypeValue, current: SequenceSnapshot, resync: Resync): (SequenceSnapshot, SequenceEvent) = {
    current match {
      case l: SetSnapshot =>
        (resync.snapshot, resync)
      case s: MapSnapshot =>
        (resync.snapshot, resync)
      case s: AppendSnapshot =>
        resync.snapshot match {
          case r: AppendSnapshot => {
            val filtered = PresequencedStreamOps.filterSequencedDiffs(sequence, r.previous :+ r.current)
            (resync.snapshot, Resync(resync.sequence, AppendSnapshot(filtered.last, filtered.init)))
          }
          case _ =>
            (resync.snapshot, resync)
        }
    }
  }

  def filteredResyncFold(sequence: SequencedTypeValue, current: SequenceSnapshot, resync: Resync): (SequencedTypeValue, SequenceSnapshot, Option[SequenceEvent]) = {
    if (sequence == resync.sequence) {
      (sequence, current, None)
    } else if (sequence.precedes(resync.sequence)) {
      val (snap, event) = synthResyncSingleStep(current, resync)
      (resync.sequence, snap, Some(event))
    } else if (sequence.isLessThan(resync.sequence).contains(true)) {
      val (snap, event) = synthSequenceJump(sequence, current, resync)
      (resync.sequence, snap, Some(event))
    } else {
      (sequence, current, None)
    }
  }

  def filteredDeltaFold(sequence: SequencedTypeValue, current: SequenceSnapshot, delta: Delta, appendLimit: Int): (SequencedTypeValue, SequenceSnapshot, Option[Delta]) = {

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
      val endSeq = seqVar
      val passedDelta = Delta(passed)
      val snap = PresequencedStreamOps.foldResync(Resync(sequence, current), passedDelta, appendLimit)

      (endSeq, snap.snapshot, Some(passedDelta))
    } else {
      (sequence, current, None)
    }
  }

}

object PresequencedStreamOps extends LazyLogging {

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
  def foldResync(left: Resync, right: Delta, appendLimit: Int): Resync = {
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
        val result = foldAppendSnapshot(s, right, appendLimit)
        left.copy(sequence = result.current.sequence, snapshot = result)
      }
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

  def foldAppendSnapshot(snap: AppendSnapshot, right: Delta, limit: Int): AppendSnapshot = {
    val appends = filterSequencedDiffs(snap.current.sequence, right.diffs)

    if (appends.nonEmpty) {
      val history = snap.previous ++ Vector(snap.current) ++ appends.init
      val histSize = history.size
      val trimmed = if (histSize > limit) {
        history.drop(histSize - limit)
      } else {
        history
      }
      AppendSnapshot(appends.last, trimmed)
    } else {
      snap
    }

  }
}