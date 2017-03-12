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

import io.greenbus.edge.flow._

trait StreamSubscribable {
  def subscriptions: Sink[Set[RowId]]
}
trait ServiceRequestable {
  def requests: Sink[Seq[ServiceRequest]]
}
trait StreamEventSource {
  def events: Source[Seq[StreamEvent]]
}
trait ServiceResponseSource {
  def responses: Source[Seq[ServiceResponse]]
}

trait ServiceProvider extends ServiceRequestable with ServiceResponseSource
trait StreamProvider extends StreamSubscribable with StreamEventSource

trait PeerLinkProxy extends StreamProvider with ServiceProvider

trait StreamSubscriptionSource {
  def subscriptions: Source[Set[RowId]]
}
trait StreamEventSink {
  def events: Sink[Seq[StreamEvent]]
}
trait StreamConsumer extends StreamSubscriptionSource with StreamEventSink

trait ServiceRequestSource {
  def requests: Source[Seq[ServiceRequest]]
}
trait ServiceRespondable {
  def responses: Sink[Seq[ServiceResponse]]
}
trait ServiceConsumer extends ServiceRequestSource with ServiceRespondable

trait SubscriberProxy extends StreamConsumer with ServiceConsumer

trait PeerLinkProxyChannel extends PeerLinkProxy with CloseableComponent
trait SubscriberProxyChannel extends SubscriberProxy with CloseableComponent

class PeerLinkShim(proxy: PeerLinkProxy) extends PeerLink {
  def setSubscriptions(rows: Set[RowId]): Unit = proxy.subscriptions.push(rows)

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = proxy.requests.push(requests)
}

case class PeerLinkContext(
  proxy: PeerLinkProxyChannel,
  link: PeerLink)

class SubscriptionTargetShim(proxy: SubscriberProxy) extends SubscriptionTarget {
  def handleBatch(events: Seq[StreamEvent]): Unit = {
    proxy.events.push(events)
  }
}

class RequestIssuerShim(proxy: SubscriberProxy) extends ServiceIssuer {
  def handleResponses(responses: Seq[ServiceResponse]): Unit = {
    proxy.responses.push(responses)
  }
}

case class SubscriptionContext(proxy: SubscriberProxyChannel, target: SubscriptionTarget, issuer: ServiceIssuer)

class PeerChannelMachine(logId: String, selfSession: PeerSessionId) {

  private val gateway = new Gateway
  private val streams = new PeerStreamEngine(logId, selfSession, gateway)
  private val services = new PeerServiceEngine(logId, streams)

  def peerOpened(peerSessionId: PeerSessionId, proxy: PeerLinkProxyChannel): Unit = {
    val link = new PeerLinkShim(proxy)
    val ctx = PeerLinkContext(proxy, link)

    proxy.events.bind(events => peerEvents(ctx, events))
    proxy.responses.bind(events => peerResponses(ctx, events))
    proxy.onClose.subscribe(() => peerClosed(ctx))
    streams.peerSourceConnected(peerSessionId, link)
  }

  private def peerEvents(ctx: PeerLinkContext, events: Seq[StreamEvent]): Unit = {
    streams.peerSourceEvents(ctx.link, events)
    // TODO: flush subs
  }

  private def peerResponses(ctx: PeerLinkContext, responses: Seq[ServiceResponse]): Unit = {
    services.handleResponses(responses)
    // TODO: flush subs
  }

  private def peerClosed(ctx: PeerLinkContext): Unit = {
    streams.sourceDisconnected(ctx.link)
    // TODO: flush subs
  }

  def subscriberOpened(proxy: SubscriberProxyChannel): Unit = {
    val target = new SubscriptionTargetShim(proxy)
    val issuer = new RequestIssuerShim(proxy)
    val ctx = SubscriptionContext(proxy, target, issuer)
    proxy.requests.bind(reqs => subscriberRequests(ctx, reqs))
    proxy.onClose.subscribe(() => subscriberClosed(ctx))
  }

  private def subscriberRequests(ctx: SubscriptionContext, requests: Seq[ServiceRequest]): Unit = {
    services.requestsIssued(ctx.issuer, requests)
  }

  private def subscriberClosed(ctx: SubscriptionContext): Unit = {
    streams.subscriberRemoved(ctx.target)
    services.issuerClosed(ctx.issuer)
  }

  def gatewayClientOpened(clientProxy: GatewayClientProxyChannel): Unit = {
    clientProxy.onClose.subscribe(() => gatewayClientClosed(clientProxy))
    gateway.handleClientOpened(clientProxy)
  }

  private def gatewayClientClosed(clientProxy: GatewayClientProxyChannel): Unit = {
    gateway.handleClientClosed(clientProxy)
  }

}
