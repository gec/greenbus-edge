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

trait PeerChannelHandler {
  def peerOpened(peerSessionId: PeerSessionId, proxy: PeerLinkProxyChannel): Unit
  def subscriberOpened(proxy: SubscriberProxyChannel): Unit
  def gatewayClientOpened(clientProxy: GatewayClientProxyChannel): Unit
}
