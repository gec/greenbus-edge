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

import io.greenbus.edge.collection.OneToManyUniquely
import io.greenbus.edge.flow._

import scala.collection.mutable

trait GatewayProxy extends ServiceConsumer {
  def subscriptions: Source[Set[RowId]]
  def events: Sink[GatewayEvents]
}
trait GatewayProxyChannel extends GatewayProxy with CloseableComponent

trait GatewayClientProxy extends ServiceProvider {
  def subscriptions: Sink[Set[RowId]]
  def events: Source[GatewayEvents]
}

trait GatewayClientProxyChannel extends GatewayClientProxy with CloseableComponent

case class GatewayEvents(routesUpdate: Option[Set[TypeValue]], events: Seq[StreamEvent])
class Gateway extends LocalGateway {

  private val clientToRoutes = OneToManyUniquely.empty[GatewayClientProxy, TypeValue]
  private val subscriptions = mutable.Map.empty[TypeValue, Set[TableRow]]

  private val eventDist = new QueuedDistributor[GatewayEvents]
  private val respDist = new QueuedDistributor[Seq[ServiceResponse]]

  def events: Source[GatewayEvents] = eventDist
  def responses: Source[Seq[ServiceResponse]] = respDist

  def handleClientOpened(proxy: GatewayClientProxy): Unit = {
    proxy.events.bind(ev => clientEvents(proxy, ev.routesUpdate, ev.events))
    proxy.responses.bind(resps => clientServiceResponses(proxy, resps))
  }

  def handleClientClosed(proxy: GatewayClientProxy): Unit = {
    val start = clientToRoutes.values
    clientToRoutes.removeAll(proxy)
    val end = clientToRoutes.values
    if (end != start) {
      eventDist.push(GatewayEvents(Some(end), Seq()))
    }
  }

  private def clientEvents(proxy: GatewayClientProxy, routeUpdate: Option[Set[TypeValue]], events: Seq[StreamEvent]): Unit = {
    val setUpdate = routeUpdate.flatMap { routes =>
      val gatewayRoutesBefore = clientToRoutes.values

      val proxyRoutesBefore = clientToRoutes.getFirst(proxy).getOrElse(Set())
      val subsBefore = proxyRoutesBefore.flatMap(r => subscriptions.getOrElse(r, Set()).map(_.toRowId(r)))

      routes.foreach(r => clientToRoutes.put(proxy, r))

      val proxyRoutesAfter = clientToRoutes.getFirst(proxy).getOrElse(Set())
      val subsAfter = proxyRoutesAfter.flatMap(r => subscriptions.getOrElse(r, Set()).map(_.toRowId(r)))
      if (subsBefore != subsAfter) {
        proxy.subscriptions.push(subsAfter)
      }

      val gatewayRoutesAfter = clientToRoutes.values

      if (gatewayRoutesAfter != gatewayRoutesBefore) {
        Some(gatewayRoutesAfter)
      } else {
        None
      }
    }

    eventDist.push(GatewayEvents(setUpdate, events))
  }

  private def clientServiceResponses(proxy: GatewayClientProxy, responses: Seq[ServiceResponse]): Unit = {
    respDist.push(responses)
  }

  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = {
    val existing = subscriptions.get(route)
    if (existing != rows) {
      subscriptions.put(route, rows)
      clientToRoutes.getSecond(route).foreach { proxy =>
        proxy.subscriptions.push(rows.map(_.toRowId(route)))
      }
    }
  }

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {
    requests.groupBy(_.row.routingKey).foreach {
      case (route, reqs) =>
        clientToRoutes.getSecond(route).foreach(p => p.requests.push(reqs))
    }
  }
}