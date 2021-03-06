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
package io.greenbus.edge.stream.gateway

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow.Sink
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.engine._

import scala.collection.mutable

sealed trait ProducerKeyEvent
case class AddRow(key: TableRow, ctx: SequenceCtx) extends ProducerKeyEvent
case class RowUpdate(key: TableRow, update: ProducerDataUpdate) extends ProducerKeyEvent
case class DropRow(key: TableRow) extends ProducerKeyEvent

sealed trait ProducerEvent

case class RouteServiceRequest(row: TableRow, value: TypeValue, respond: TypeValue => Unit)

case class RouteBatchEvent(route: TypeValue, events: Seq[ProducerKeyEvent]) extends ProducerEvent

case class RouteBindEvent(route: TypeValue,
  initialEvents: Seq[ProducerKeyEvent],
  dynamic: Map[String, DynamicTable],
  handler: Sink[RouteServiceRequest]) extends ProducerEvent

case class RouteUnbindEvent(route: TypeValue) extends ProducerEvent

class ProducerMgr(appendLimitDefault: Int) extends StreamTargetSubject[ProducerRouteMgr] with LazyLogging {

  protected val routeMap = mutable.Map.empty[TypeValue, ProducerRouteMgr]

  def routeSet: Set[TypeValue] = routeMap.keySet.toSet

  def handleEvent(event: ProducerEvent): Unit = {
    logger.debug(s"Producer event: $event")
    event match {
      case ev: RouteBindEvent => {
        val mgr = routeMap.getOrElseUpdate(ev.route, new ProducerRouteMgr(appendLimitDefault))
        mgr.bind(ev.initialEvents, ev.dynamic, ev.handler)
      }
      case ev: RouteBatchEvent => {
        routeMap.get(ev.route).foreach(_.batch(ev.events))
      }
      case ev: RouteUnbindEvent =>
        routeMap.get(ev.route).foreach { routeMgr =>
          routeMgr.unbind()
          if (!routeMgr.targeted()) {
            routeMap -= ev.route
          }
        }
    }
  }

  def handleRequests(requests: Seq[(TypeValue, RouteServiceRequest)]): Unit = {
    requests.foreach {
      case (route, req) =>
        routeMap.get(route).foreach(_.request(req))
    }
  }

  protected def buildRouteManager(route: TypeValue): ProducerRouteMgr = {
    new ProducerRouteMgr(appendLimitDefault)
  }
}

