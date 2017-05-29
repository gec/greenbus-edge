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
package io.greenbus.edge.stream.gateway3

import io.greenbus.edge.flow.Sink
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.engine2._
import io.greenbus.edge.stream.gateway.RouteServiceRequest
import io.greenbus.edge.stream.gateway2._

import scala.collection.mutable

sealed trait ProducerKeyEvent
case class AddRow(key: TableRow, ctx: SequenceCtx) extends ProducerKeyEvent
case class RowUpdate(key: TableRow, update: ProducerDataUpdate) extends ProducerKeyEvent
case class DrowRow(key: TableRow) extends ProducerKeyEvent

sealed trait ProducerEvent

case class RouteBatchEvent(route: TypeValue, events: Seq[ProducerKeyEvent]) extends ProducerEvent

case class RouteBindEvent(route: TypeValue,
  initialEvents: Seq[ProducerKeyEvent],
  dynamic: Map[String, DynamicTable],
  handler: Sink[RouteServiceRequest]) extends ProducerEvent

case class RouteUnbindEvent(route: TypeValue) extends ProducerEvent

class ProducerMgr extends StreamTargetSubject2[ProducerRouteMgr] {

  protected val routeMap = mutable.Map.empty[TypeValue, ProducerRouteMgr]

  def handleEvent(event: ProducerEvent): Unit = {
    event match {
      case ev: RouteBindEvent => {
        val mgr = routeMap.getOrElseUpdate(ev.route, new ProducerRouteMgr)
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
    new ProducerRouteMgr
  }
}

trait StreamTargetSubject2[A <: RouteTargetSubject] {

  protected val routeMap: mutable.Map[TypeValue, A]
  private val targetToRouteMap = mutable.Map.empty[StreamTarget, Map[TypeValue, RouteObservers]]

  protected def buildRouteManager(route: TypeValue): A

  def targetSubscriptionUpdate(target: StreamTarget, subscription: Map[TypeValue, RouteObservers]): Unit = {
    val prev = targetToRouteMap.getOrElse(target, Map())

    val removed = prev.keySet -- subscription.keySet
    removed.foreach { route =>
      prev.get(route).foreach { entry =>
        routeMap.get(route).foreach { routeStreams =>
          routeStreams.targetRemoved(entry.streamObserver)
          if (!routeStreams.targeted()) {
            routeMap -= route
          }
        }
      }
    }
    subscription.foreach {
      case (route, observers) =>
        val routeStreams = routeMap.getOrElseUpdate(route, buildRouteManager(route))
        routeStreams.targetUpdate(observers.streamObserver, observers.rowObserverMap)
    }
    targetToRouteMap.update(target, subscription)

    // TODO: flush?
    //target.flush()
  }
  def targetRemoved(target: StreamTarget): Unit = {
    val prev = targetToRouteMap.getOrElse(target, Map())
    prev.foreach {
      case (route, obs) =>
        routeMap.get(route).foreach { routeStreams =>
          routeStreams.targetRemoved(obs.streamObserver)
          if (!routeStreams.targeted()) {
            routeMap -= route
          }
        }
    }
    targetToRouteMap -= target
  }
}

