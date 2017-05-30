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

import io.greenbus.edge.flow.{ LatchSubscribable, Sink, Source }
import io.greenbus.edge.stream._

class PeerChannelManager(generic: GenericChannelEngine) extends PeerChannelHandler {
  def peerOpened(peerSessionId: PeerSessionId, proxy: PeerLinkProxyChannel): Unit = {
    val channel = new PeerLinkSourceChannel(peerSessionId, proxy)
    generic.sourceChannel(channel)
  }

  def subscriberOpened(proxy: SubscriberProxyChannel): Unit = {
    val target = new SubscriberProxyTarget(proxy)
    generic.targetChannel(target)
  }

  def gatewayClientOpened(clientProxy: GatewayClientProxyChannel): Unit = {
    val channel = new GatewaySourceChannel(clientProxy)
    generic.sourceChannel(channel)
  }
}

class SubscriberProxyTarget(proxy: SubscriberProxyChannel) extends GenericTargetChannel {
  def onClose: LatchSubscribable = proxy.onClose

  def subscriptions: Source[Set[RowId]] = proxy.subscriptions

  def events: Sink[Seq[StreamEvent]] = proxy.events

  def requests: Source[Seq[ServiceRequest]] = proxy.requests

  def responses: Sink[Seq[ServiceResponse]] = proxy.responses
}