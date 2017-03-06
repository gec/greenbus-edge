package io.greenbus.edge.colset

import io.greenbus.edge.collection.MapSetBuilder

/*

sealed trait SetSequenceElement
trait SetDelta extends SetSequenceElement
trait SetSnapshot extends SetSequenceElement
 */

trait SetDelta
trait SetSnapshot

case class ModifiedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[TypeValue]) extends SetDelta
case class ModifiedSetSnapshot(sequence: SequencedTypeValue, snapshot: Set[TypeValue]) extends SetSnapshot
case class ModifiedKeyedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[TypeValue], modifies: Set[(TypeValue, TypeValue)]) extends SetDelta
case class ModifiedKeyedSetSnapshot(sequence: SequencedTypeValue, snapshot: Map[TypeValue, TypeValue]) extends SetSnapshot
case class AppendSetValue(sequence: SequencedTypeValue, value: TypeValue)
case class AppendSetSequence(appends: Seq[AppendSetValue]) extends SetDelta with SetSnapshot
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
    rows.foreach { row => b += (row.routingKey -> row.tableRow) }
    b.result()
  }
}
case class RowId(routingKey: TypeValue, table: SymbolVal, rowKey: TypeValue) {
  def tableRow: TableRow = TableRow(table, rowKey)
}
case class TableRow(table: SymbolVal, rowKey: TypeValue) {
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

case class StreamEventBatch(events: Seq[StreamEvent])
case class StreamNotifications(batches: Seq[StreamEventBatch])

case class IndexSpecifier(key: TypeValue, value: Option[IndexableTypeValue])

case class StreamSubscriptionParams(rows: Seq[RowId])