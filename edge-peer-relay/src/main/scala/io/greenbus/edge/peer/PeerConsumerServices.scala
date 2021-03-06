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
package io.greenbus.edge.peer

import java.util.UUID

import io.greenbus.edge.api.stream.EdgePeer
import io.greenbus.edge.api.{ ConsumerService, EdgeSubscriptionClient, ServiceClient }
import io.greenbus.edge.stream.{ PeerLinkProxyChannel, PeerSessionId }
import io.greenbus.edge.thread.SchedulableCallMarshaller

class PeerConsumerServices(logId: String, eventThread: SchedulableCallMarshaller) extends ConsumerService with PeerLinkObserver {
  private val session = PeerSessionId(UUID.randomUUID(), 0)
  private val peer = new EdgePeer(logId, session, eventThread, remoteIo = true)

  def connected(session: PeerSessionId, channel: PeerLinkProxyChannel): Unit = {
    peer.connectRemotePeer(session, channel)
  }

  def subscriptionClient: EdgeSubscriptionClient = {
    peer.subscriptions
  }

  def queuingServiceClient: ServiceClient = peer.serviceClient
}
