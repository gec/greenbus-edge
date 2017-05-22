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

trait StreamFilter {
  def handle(event: AppendEvent): Option[AppendEvent]
}

object StreamFilterImpl {
  case class SessionContext(sessionId: PeerSessionId, context: SequenceCtx)

  sealed trait State
  case object Uninit extends State
  case class Init(session: PeerSessionId, seqFilter: GenSequenceFilter) extends State
}
class StreamFilterImpl extends StreamFilter {
  import StreamFilterImpl._
  private var state: State = Uninit

  def handle(event: AppendEvent): Option[AppendEvent] = {
    state match {
      case Uninit => {
        event match {
          case rs: ResyncSession =>
            val filter = new GenSequenceFilter("filter", rs.resync.sequence)
            state = Init(rs.sessionId, filter)
            Some(rs)
          case snap: ResyncSnapshot => None
          case sd: StreamDelta => None
        }
      }
      case Init(session, filter) => {
        event match {
          case rs: ResyncSession =>
            if (rs.sessionId == session) {
              filter.resync(rs.resync).map(r => rs.copy(resync = r))
            } else {
              val filter = new GenSequenceFilter("filter", rs.resync.sequence)
              state = Init(rs.sessionId, filter)
              Some(rs)
            }
          case snap: ResyncSnapshot =>
            filter.resync(snap.resync).map(r => snap.copy(resync = r))
          case sd: StreamDelta =>
            filter.delta(sd.update).map(StreamDelta)
        }
      }
    }
  }
}

trait SequenceFilter {
  def delta(delta: Delta): Option[Delta]
  def resync(resync: Resync): Option[Resync]
}

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

