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

import io.greenbus.edge.amqp.AmqpService
import io.greenbus.edge.amqp.channel.AmqpChannelHandler
import io.greenbus.edge.amqp.colset.ChannelParserImpl
import io.greenbus.edge.amqp.impl.AmqpListener
import io.greenbus.edge.colset.{ PeerChannelMachine, PeerSessionId }
import io.greenbus.edge.colset.channel.ChannelHandler
import io.greenbus.edge.colset.proto.provider.ProtoSerializationProvider

import scala.concurrent.Future

object PeerRelayServer {

  def main(args: Array[String]): Unit = {

    val sessionId = PeerSessionId(UUID.randomUUID(), 0)

    runRelay(sessionId, "127.0.0.1", 50001)

    System.in.read()

  }

  def runRelay(sessionId: PeerSessionId, host: String, port: Int): Future[AmqpListener] = {

    val sessionId = PeerSessionId(UUID.randomUUID(), 0)

    val peerChannelMachine = new PeerChannelMachine("Peer", sessionId)

    val channelHandler = new ChannelHandler(peerChannelMachine)

    val service = AmqpService.build(Some("server"))

    val serialization = new ProtoSerializationProvider
    val amqpHandler = new AmqpChannelHandler(service.eventLoop, new ChannelParserImpl(sessionId, serialization), serialization, channelHandler)

    service.listen(host, port, amqpHandler)
  }
}
