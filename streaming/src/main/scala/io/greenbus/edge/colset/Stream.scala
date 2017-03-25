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

import java.util.UUID

import io.greenbus.edge.collection.MapSetBuilder

case class PeerSessionId(persistenceId: UUID, instanceId: Long)

trait SetDelta
trait SetSnapshot

object Redux {

  sealed trait StreamParams

  sealed trait StreamDiff

  case class SetDiff(removes: Set[TypeValue], adds: Set[TypeValue])
  case class MapDiff(removes: Set[TypeValue], adds: Set[(TypeValue, TypeValue)], modifies: Set[(TypeValue, TypeValue)])
  case class AppendValue(value: TypeValue)

  sealed trait StreamSnapshot

  case class SetSnapshot(snapshot: Set[TypeValue]) extends StreamSnapshot
  case class MapSnapshot(snapshot: Map[TypeValue, TypeValue]) extends StreamSnapshot

  case class AppendSetValue(sequence: SequencedTypeValue, value: TypeValue)
  case class AppendSnapshot(current: AppendSetValue, previous: Seq[AppendSetValue]) extends StreamSnapshot

  case class SequencedDiff(sequence: SequencedTypeValue, diff: StreamDiff)
  case class Delta(diffs: Seq[SequencedDiff])

  case class Resync(sequence: SequencedTypeValue, params: StreamParams, userMetadata: TypeValue, snapshot: StreamSnapshot)

  sealed trait StreamEvent
  sealed trait RowEvent extends StreamEvent
  sealed trait RowAppendEvent extends RowEvent
  case class DeltaEvent(row: RowId, delta: Delta) extends RowAppendEvent
  case class ResyncEvent(row: RowId, resync: Resync) extends RowAppendEvent
  case class ResequenceEvent(row: RowId, sessionId: PeerSessionId, resync: Resync) extends RowAppendEvent
  case class RowResolvedAbsent(row: RowId) extends RowEvent
  case class RouteUnresolved(route: TypeValue) extends StreamEvent
}


case class ModifiedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[TypeValue]) extends SetDelta
case class ModifiedSetSnapshot(sequence: SequencedTypeValue, snapshot: Set[TypeValue]) extends SetSnapshot
case class ModifiedKeyedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[(TypeValue, TypeValue)], modifies: Set[(TypeValue, TypeValue)]) extends SetDelta
case class ModifiedKeyedSetSnapshot(sequence: SequencedTypeValue, snapshot: Map[TypeValue, TypeValue]) extends SetSnapshot
case class AppendSetValue(sequence: SequencedTypeValue, value: TypeValue)
case class AppendSetSequence(appends: Seq[AppendSetValue]) extends SetDelta with SetSnapshot

sealed trait AppendEvent
case class StreamDelta(update: SetDelta) extends AppendEvent
case class ResyncSnapshot(snapshot: SetSnapshot) extends AppendEvent
case class ResyncSession(sessionId: PeerSessionId, snapshot: SetSnapshot) extends AppendEvent

object RowId {
  def setToRouteMap(rows: Set[RowId]): Map[TypeValue, Set[TableRow]] = {
    val b = MapSetBuilder.newBuilder[TypeValue, TableRow]
    rows.foreach { row => b += (row.routingKey -> row.tableRow) }
    b.result()
  }
}
case class RowId(routingKey: TypeValue, table: String, rowKey: TypeValue) {
  def tableRow: TableRow = TableRow(table, rowKey)
}
case class TableRow(table: String, rowKey: TypeValue) {
  def toRowId(routingKey: TypeValue): RowId = RowId(routingKey, table, rowKey)
}

sealed trait StreamEvent {
  def routingKey: TypeValue
}

case class RowAppendEvent(rowId: RowId, appendEvent: AppendEvent) extends StreamEvent {
  def routingKey = rowId.routingKey
}

// TODO: rowunavailable event?
case class RouteUnresolved(routingKey: TypeValue) extends StreamEvent

