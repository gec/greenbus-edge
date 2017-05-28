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
package io.greenbus.edge.stream.gateway2

import io.greenbus.edge.stream._
import io.greenbus.edge.stream.gateway.MapDiffCalc

class AppendSequencer(session: PeerSessionId, ctx: SequenceCtx) {
  private var sequence: Long = 0
  private var uninit = true

  def handle(values: Seq[TypeValue]): Seq[AppendEvent] = {
    if (values.nonEmpty) {
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
    } else {
      Seq()
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

    val results = prevOpt match {
      case None =>
        Seq(ResyncSession(session, ctx, SetSequencer.toSnapshot(seq, value)))
      case Some(prev) => {
        val setDiff = diff(value, prev)
        if (setDiff.adds.nonEmpty || setDiff.removes.nonEmpty) {
          Seq(StreamDelta(toDelta(seq, diff(value, prev))))
        } else {
          Seq()
        }
      }
    }

    prevOpt = Some(value)

    results
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

    val results = prevOpt match {
      case None =>
        Seq(ResyncSession(session, ctx, toSnapshot(seq, value)))
      case Some(prev) => {
        val mapDiff = diff(value, prev)
        if (mapDiff.adds.nonEmpty || mapDiff.modifies.nonEmpty || mapDiff.removes.nonEmpty) {
          Seq(StreamDelta(toDelta(seq, diff(value, prev))))
        } else {
          Seq()
        }
      }
    }

    prevOpt = Some(value)

    results
  }
}

