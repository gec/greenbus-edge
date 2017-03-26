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

sealed trait SequenceTypeDiff

case class SetDiff(removes: Set[TypeValue], adds: Set[TypeValue]) extends SequenceTypeDiff
case class MapDiff(removes: Set[TypeValue], adds: Set[(TypeValue, TypeValue)], modifies: Set[(TypeValue, TypeValue)]) extends SequenceTypeDiff
case class AppendValue(value: TypeValue) extends SequenceTypeDiff

sealed trait SequenceSnapshot

case class SetSnapshot(snapshot: Set[TypeValue]) extends SequenceSnapshot
case class MapSnapshot(snapshot: Map[TypeValue, TypeValue]) extends SequenceSnapshot

case class AppendSetValue(sequence: SequencedTypeValue, value: AppendValue)
case class AppendSnapshot(current: SequencedDiff, previous: Seq[SequencedDiff]) extends SequenceSnapshot

case class SequencedDiff(sequence: SequencedTypeValue, diff: SequenceTypeDiff)

sealed trait SequenceEvent
case class Delta(diffs: Seq[SequencedDiff]) extends SequenceEvent
case class Resync(sequence: SequencedTypeValue, snapshot: SequenceSnapshot) extends SequenceEvent

case class StreamParams()

object SequenceCtx {
  def empty: SequenceCtx = SequenceCtx(None, None)
}
case class SequenceCtx(params: Option[StreamParams], userMetadata: Option[TypeValue])

/*
CONTROL STREAM VS VALUE SEQUENCE

SequenceEvent(Delta or Resync)
ControlCtxChangeEvent(session, ctx, resync)

Event(controlOpt, sequenceEvent)

Control:

ResyncControl (full)
ResyncContext

 */
sealed trait AppendEvent
case class StreamDelta(update: Delta) extends AppendEvent
case class ResyncSnapshot(resync: Resync) extends AppendEvent
case class ResyncSession(sessionId: PeerSessionId, context: SequenceCtx, resync: Resync) extends AppendEvent

// This is "peer event", append event above is stream event
sealed trait StreamEvent {
  def routingKey: TypeValue // TODO: change to route?
}
sealed trait RowEvent extends StreamEvent {
  def rowId: RowId
}
case class RowAppendEvent(rowId: RowId, appendEvent: AppendEvent) extends RowEvent {
  def routingKey: TypeValue = rowId.routingKey
}

/*case class RowResolvedAbsent(rowId: RowId) extends RowEvent {
  def routingKey: TypeValue = rowId.routingKey
}*/
case class RouteUnresolved(routingKey: TypeValue) extends StreamEvent
