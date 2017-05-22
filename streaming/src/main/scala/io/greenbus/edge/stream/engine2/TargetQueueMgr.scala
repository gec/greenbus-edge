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
package io.greenbus.edge.stream.engine2

import io.greenbus.edge.stream._

import scala.collection.mutable

case class RouteObservers(streamObserver: StreamObserver, rowObserverMap: Map[TableRow, KeyStreamObserver])

class SimpleRouteTargetQueues(route: TypeValue) {

  private val buffer = mutable.ArrayBuffer.empty[StreamEvent]

  private val streamObs = new StreamObserver {
    def handle(routeEvent: StreamEvent): Unit = {
      buffer += routeEvent
    }
  }

  private val keys = mutable.Map.empty[TableRow, KeyStreamObserver]

  private class SimpleKeyObserver(row: TableRow) extends KeyStreamObserver {
    def handle(event: AppendEvent): Unit = {
      buffer += RowAppendEvent(row.toRowId(route), event)
    }
  }

  def update(rows: Set[TableRow]): RouteObservers = {
    val prev = keys.keySet
    val removes = prev -- rows
    keys --= removes
    val rowObsMap = rows.map { row =>
      row -> keys.getOrElseUpdate(row, new SimpleKeyObserver(row))
    }.toMap
    RouteObservers(streamObs, rowObsMap)
  }

  def dequeue(): Seq[StreamEvent] = {
    val results = buffer.toVector
    buffer.clear()
    results
  }
}

// TODO: notify dirty
class TargetQueueMgr {

  private val routeMap = mutable.Map.empty[TypeValue, SimpleRouteTargetQueues]

  def subscriptionUpdate(rows: Set[RowId]): Map[TypeValue, RouteObservers] = {
    val routeToRows: Map[TypeValue, Set[TableRow]] = rows.groupBy(_.routingKey).mapValues(_.map(_.tableRow))
    val removes = routeMap.keySet -- routeToRows.keySet
    routeMap --= removes
    routeToRows.map {
      case (route, tableRows) =>
        val queues = routeMap.getOrElseUpdate(route, new SimpleRouteTargetQueues(route))
        route -> queues.update(tableRows)
    }
  }
  def dequeue(): Seq[StreamEvent] = {
    routeMap.flatMap(_._2.dequeue()).toSeq
  }
}
