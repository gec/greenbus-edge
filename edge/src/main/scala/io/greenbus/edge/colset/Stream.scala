package io.greenbus.edge.colset

import io.greenbus.edge.collection.MapSetBuilder


trait SetDelta
trait SetSnapshot

case class ModifiedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[TypeValue]) extends SetDelta
case class ModifiedSetSnapshot(sequence: SequencedTypeValue, snapshot: Set[TypeValue]) extends SetSnapshot
case class ModifiedKeyedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[TypeValue], modifies: Set[(TypeValue, TypeValue)]) extends SetDelta
case class ModifiedKeyedSetSnapshot(sequence: SequencedTypeValue, snapshot: Map[TypeValue, TypeValue]) extends SetSnapshot
case class AppendSetValue(sequence: SequencedTypeValue, value: TypeValue)
case class AppendSetDelta(appends: Seq[AppendSetValue]) extends SetDelta with SetSnapshot
//case class AppendSetSnapshot(appends: Seq[AppendSetValue]) extends SetSnapshot


//sealed trait RowStreamEvent
sealed trait AppendEvent
case class StreamDelta(update: SetDelta) extends AppendEvent
case class ResyncSnapshot(snapshot: SetSnapshot) extends AppendEvent
case class ResyncSession(sessionId: PeerSessionId, snapshot: SetSnapshot) extends AppendEvent
//case class Inactive

object RowId {
  def setToRouteMap(rows: Set[RowId]): Map[TypeValue, Set[TableRow]] = {
    val b = MapSetBuilder.newBuilder[TypeValue, TableRow]
    rows.foreach { row => row.routingKeyOpt.foreach(routingKey => b += (routingKey -> row.tableRow)) }
    b.result()
  }
}
case class RowId(routingKeyOpt: Option[TypeValue], table: SymbolVal, rowKey: TypeValue) {
  def tableRow: TableRow = TableRow(table, rowKey)
}
case class TableRow(table: SymbolVal, rowKey: TypeValue)

sealed trait StreamEvent {
  def routingKey: TypeValue
}

case class RowAppendEvent(rowId: RowId, appendEvent: AppendEvent) extends StreamEvent {
  def routingKey = rowId.routingKeyOpt.get
}

// TODO: rowunavailable event?
case class RouteUnresolved(routingKey: TypeValue) extends StreamEvent

case class StreamEventBatch(events: Seq[StreamEvent])
case class StreamNotifications(batches: Seq[StreamEventBatch])

case class IndexSpecifier(key: TypeValue, value: Option[IndexableTypeValue])

case class StreamSubscriptionParams(rows: Seq[RowId])