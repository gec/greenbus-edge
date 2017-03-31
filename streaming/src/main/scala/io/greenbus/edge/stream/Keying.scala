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
package io.greenbus.edge.stream

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
