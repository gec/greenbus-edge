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
package io.greenbus.edge.peer.impl2

import io.greenbus.edge.api.stream.peer.EdgePeer
import io.greenbus.edge.api.stream.subscribe2.SubShim
import io.greenbus.edge.api.{ EdgeSubscriptionClient, ServiceClient, ServiceClientChannel }
import io.greenbus.edge.flow.Source
import io.greenbus.edge.peer.ConsumerServices
import io.greenbus.edge.stream.{ PeerLinkProxyChannel, PeerSessionId }
import io.greenbus.edge.thread.CallMarshaller

class PeerConsumerServices(logId: String, eventThread: CallMarshaller) extends ConsumerServices {
  private val peer = new EdgePeer(logId, eventThread)

  def connected(session: PeerSessionId, channel: PeerLinkProxyChannel): Unit = {
    peer.connectRemotePeer(session, channel)
  }

  def subscriptionClient: EdgeSubscriptionClient = {
    new SubShim(peer.subscriptions)
  }

  def serviceChannelSource: Source[ServiceClientChannel] = ???

  def queuingServiceClient: ServiceClient = ???
}

/*

class StreamConsumerManagerImpl(eventThread: SchedulableCallMarshaller) extends StreamConsumerManager {

  private val streamSubMgr = new DynamicSubscriptionManager(eventThread)
  private val edgeSubMgr = new EdgeSubscriptionManager(eventThread, streamSubMgr)

  private val serviceDist = new RemoteBoundLatestSource[ServiceClientChannel](eventThread)
  private val requestQueuer = new QueuingServiceChannel(eventThread)

  def connected(session: PeerSessionId, channel: PeerLinkProxyChannel): Unit = {
    streamSubMgr.connected(session, channel)

    val streamServices = new StreamServiceClientImpl(channel, eventThread)
    val edgeServices = new ServiceClientImpl(streamServices)
    serviceDist.push(edgeServices)
    requestQueuer.connected(edgeServices)
  }

  def subscriptionClient: EdgeSubscriptionClient = edgeSubMgr

  def serviceChannelSource: Source[ServiceClientChannel] = serviceDist

  def queuingServiceClient: ServiceClient = requestQueuer

}

 */ 