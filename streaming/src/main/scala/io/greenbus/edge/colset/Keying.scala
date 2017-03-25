package io.greenbus.edge.colset

import java.util.UUID

import io.greenbus.edge.collection.MapSetBuilder


case class PeerSessionId(persistenceId: UUID, instanceId: Long)

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
