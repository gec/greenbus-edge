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

import io.greenbus.edge.flow.{ CloseObservable, Closeable, Sink, Source }

trait ClientGatewaySource {
  //def routes: Sink[Set[TypeValue]]
}

case class GatewayClientEvents(routes: Option[Set[TypeValue]], events: Seq[StreamEvent])

trait ClientGatewaySourceProxy extends ServiceRequestTarget {
  def subscriptions: Sink[Set[RowId]]
  def events: Source[GatewayClientEvents]
}

trait ClientGatewaySourceProxyChannel extends ClientGatewaySourceProxy with Closeable with CloseObservable

class Gateway extends LocalGateway {

  /*private var handler = Option.empty[GatewayEventHandler]
  def bindHandler(eventHandler: GatewayEventHandler): Unit = {
    handler = Some(eventHandler)
  }*/

  def handleClientOpened(proxy: ClientGatewaySourceProxy): Unit = {
    proxy.events.bind(ev => clientEvents(proxy, ev.routes, ev.events))
    proxy.responses.bind(resps => clientServiceResponses(proxy, resps))
  }

  def handleClientClosed(proxy: ClientGatewaySourceProxy): Unit = {

  }

  private def clientEvents(proxy: ClientGatewaySourceProxy, routeUpdate: Option[Set[TypeValue]], events: Seq[StreamEvent]): Unit = {
    //handler.foreach(_.localGatewayEvents())
  }

  private def clientServiceResponses(proxy: ClientGatewaySourceProxy, responses: Seq[ServiceResponse]): Unit = {

  }

  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = ???

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = ???
}

trait RowSubscribable {
  def subscriptions: Sink[Set[RowId]]
}
trait ServiceRequestable {
  def requests: Sink[Seq[ServiceRequest]]
}
trait EventSource {
  def events: Source[Seq[StreamEvent]]
}
trait ResponseSource {
  def responses: Source[Seq[ServiceResponse]]
}

trait ServiceRequestTarget extends ServiceRequestable with ResponseSource
trait StreamSource extends RowSubscribable with EventSource

trait PeerLinkProxy extends StreamSource with ServiceRequestTarget

trait ServiceRequestSource {
  def requests: Source[Seq[ServiceRequest]]
  def responses: Sink[Seq[ServiceResponse]]
}

trait RoutesConsumer {
  def subscriptions: Source[Set[RowId]]
  def events: Sink[Seq[StreamEvent]]
}

trait SubscriberProxy extends RoutesConsumer

trait PeerLinkProxyChannel extends PeerLinkProxy with Closeable with CloseObservable

class PeerManager(logId: String, selfSession: PeerSessionId) {

  private val gateway = new Gateway
  private val streams = new PeerStreamEngine(logId, selfSession, gateway)
  private val services = new PeerServiceEngine(logId, streams)

  def peerOpened(peerSessionId: PeerSessionId, proxy: PeerLinkProxy): Unit = {
    streams.peerSourceConnected(peerSessionId, proxy)
    proxy.events.bind { events =>
      streams.peerSourceEvents(proxy, events)
    }
    proxy.responses.bind { resps =>
      services.handleResponses(resps)
    }
  }

  def peerClosed(proxy: PeerLinkProxy): Unit = {
    streams.sourceDisconnected(proxy)
  }

}
