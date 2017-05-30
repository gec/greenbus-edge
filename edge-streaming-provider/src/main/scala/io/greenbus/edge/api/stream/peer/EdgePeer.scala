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
package io.greenbus.edge.api.stream.peer

import io.greenbus.edge.api.stream.subscribe2.{ EdgeSubscriptionClient2, EdgeSubscriptionProvider }
import io.greenbus.edge.stream.{ PeerChannelHandler, PeerLinkProxyChannel, PeerSessionId }
import io.greenbus.edge.stream.peer.StreamPeer
import io.greenbus.edge.thread.CallMarshaller

class EdgePeer(logId: String, engineThread: CallMarshaller) {

  private val peer = new StreamPeer(logId: String, engineThread)
  private val subProvider = new EdgeSubscriptionProvider(peer)

  def connectRemotePeer(peerSessionId: PeerSessionId, channel: PeerLinkProxyChannel): Unit = {
    peer.connectRemotePeer(peerSessionId, channel)
  }

  def channelHandler: PeerChannelHandler = peer.channelHandler

  def subscriptions: EdgeSubscriptionClient2 = subProvider
}
