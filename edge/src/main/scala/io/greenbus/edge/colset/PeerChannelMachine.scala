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

import com.typesafe.scalalogging.LazyLogging
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
  link: PeerLink,
  session: PeerSessionId)

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

trait PeerChannelHandler {
  def peerOpened(peerSessionId: PeerSessionId, proxy: PeerLinkProxyChannel): Unit
  def subscriberOpened(proxy: SubscriberProxyChannel): Unit
  def gatewayClientOpened(clientProxy: GatewayClientProxyChannel): Unit
}

class PeerChannelMachine(logId: String, selfSession: PeerSessionId) extends PeerChannelHandler with LazyLogging {

  private val gateway = new Gateway(selfSession)
  private val streams = new PeerStreamEngine(logId, selfSession, gateway)
  private val services = new PeerServiceEngine(logId, streams)

  gateway.events.bind(ev => streams.localGatewayEvents(ev.routesUpdate, ev.events))
  gateway.responses.bind(resps => services.handleResponses(resps))

  def peerOpened(peerSessionId: PeerSessionId, proxy: PeerLinkProxyChannel): Unit = {
    logger.info(s"Peer link for $peerSessionId opened")
    val link = new PeerLinkShim(proxy)
    val ctx = PeerLinkContext(proxy, link, peerSessionId)

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
    logger.info(s"Peer link for ${ctx.session} closed")
    streams.sourceDisconnected(ctx.link)
    // TODO: flush subs
  }

  def subscriberOpened(proxy: SubscriberProxyChannel): Unit = {
    logger.debug(s"Subscriber opened")
    val target = new SubscriptionTargetShim(proxy)
    val issuer = new RequestIssuerShim(proxy)
    val ctx = SubscriptionContext(proxy, target, issuer)
    proxy.subscriptions.bind(rows => subscriberSetUpdate(ctx, rows))
    proxy.requests.bind(reqs => subscriberRequests(ctx, reqs))
    proxy.onClose.subscribe(() => subscriberClosed(ctx))
  }

  private def subscriberSetUpdate(ctx: SubscriptionContext, rows: Set[RowId]): Unit = {
    logger.info("Subscriber client updated subscriptions: " + rows)
    streams.subscriptionsRegistered(ctx.target, StreamSubscriptionParams(rows))
  }

  private def subscriberRequests(ctx: SubscriptionContext, requests: Seq[ServiceRequest]): Unit = {
    logger.info("Subscriber client opened")
    services.requestsIssued(ctx.issuer, requests)
  }

  private def subscriberClosed(ctx: SubscriptionContext): Unit = {
    logger.info("Subscriber client closed")
    streams.subscriberRemoved(ctx.target)
    services.issuerClosed(ctx.issuer)
  }

  def gatewayClientOpened(clientProxy: GatewayClientProxyChannel): Unit = {
    logger.info("Gateway client opened")
    clientProxy.onClose.subscribe(() => gatewayClientClosed(clientProxy))
    gateway.handleClientOpened(clientProxy)
  }

  private def gatewayClientClosed(clientProxy: GatewayClientProxyChannel): Unit = {
    logger.info("Gateway client closed")
    gateway.handleClientClosed(clientProxy)
  }

}
