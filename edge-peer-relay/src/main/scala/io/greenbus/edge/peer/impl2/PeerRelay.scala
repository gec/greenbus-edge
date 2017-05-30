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

import io.greenbus.edge.amqp.AmqpService
import io.greenbus.edge.amqp.channel.AmqpChannelHandler
import io.greenbus.edge.amqp.impl.AmqpListener
import io.greenbus.edge.amqp.stream.ChannelParserImpl
import io.greenbus.edge.api.stream.peer.EdgePeer
import io.greenbus.edge.stream.PeerSessionId
import io.greenbus.edge.stream.channel.ChannelHandler
import io.greenbus.edge.stream.proto.provider.ProtoSerializationProvider

import scala.concurrent.Future

class PeerRelay(sessionId: PeerSessionId) {

  private val service = AmqpService.build(Some("server"))
  private val peer = new EdgePeer("peer", service.eventLoop)

  private val channelHandler = new ChannelHandler(peer.channelHandler)
  private val serialization = new ProtoSerializationProvider
  private val amqpHandler = new AmqpChannelHandler(service.eventLoop, new ChannelParserImpl(sessionId, serialization), serialization, channelHandler)

  def listen(host: String, port: Int): Future[AmqpListener] = {
    service.listen(host, port, amqpHandler)
  }

  def close(): Unit = {
    service.close()
    //indexThread.close()
  }
}
