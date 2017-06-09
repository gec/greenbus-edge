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
class StreamFilterImpl extends StreamFilter with LazyLogging {
  import StreamFilterImpl._
  private var state: State = Uninit

  def handle(event: AppendEvent): Option[AppendEvent] = {
    state match {
      case Uninit => {
        event match {
          case rs: ResyncSession =>
            logger.debug(s"StreamFilter resync init: $rs")
            val filter = new GenSequenceFilter("filter", appendLimit = 1)
            filter.resync(rs.resync)
            state = Init(rs.sessionId, filter)
            Some(rs)
          case sd: StreamDelta => None
          case StreamAbsent =>
            Some(event)
        }
      }
      case Init(session, filter) => {
        event match {
          case rs: ResyncSession =>
            if (rs.sessionId == session) {
              logger.debug(s"StreamFilter resync same session: $rs")
              filter.resync(rs.resync).map {
                case r: Resync => rs.copy(resync = r)
                case d: Delta => StreamDelta(d)
              }
            } else {
              logger.debug(s"StreamFilter resync new session: $rs")
              val filter = new GenSequenceFilter("filter", appendLimit = 1)
              filter.resync(rs.resync)
              state = Init(rs.sessionId, filter)
              Some(rs)
            }
          case sd: StreamDelta =>
            filter.delta(sd.update).map(StreamDelta)
          case StreamAbsent =>
            Some(event)
        }
      }
    }
  }
}

trait SequenceFilter {
  def delta(delta: Delta): Option[Delta]
  def resync(resync: Resync): Option[SequenceEvent]
}

object GenSequenceFilter {
  sealed trait State
  case object Uninit extends State
  case class Init(var sequence: SequencedTypeValue, var snap: SequenceSnapshot) extends State
}
class GenSequenceFilter(cid: String, appendLimit: Int) extends SequenceFilter {
  import GenSequenceFilter._
  private var state: State = Uninit

  def delta(delta: Delta): Option[Delta] = {
    state match {
      case Uninit => None
      case st: Init =>
        val (endSeq, endSnap, eventOpt) = FilteringStreamOps.filteredDeltaFold(st.sequence, st.snap, delta, 1)
        st.sequence = endSeq
        st.snap = endSnap
        eventOpt
    }
  }

  def resync(resync: Resync): Option[SequenceEvent] = {
    state match {
      case Uninit =>
        state = Init(resync.sequence, resync.snapshot)
        Some(resync)
      case st: Init =>
        val (endSeq, endSnap, eventOpt) = FilteringStreamOps.filteredResyncFold(st.sequence, st.snap, resync)
        st.sequence = endSeq
        st.snap = endSnap
        eventOpt
    }
  }
}

