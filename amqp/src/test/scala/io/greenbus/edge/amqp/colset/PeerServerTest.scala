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
package io.greenbus.edge.amqp.colset

import java.util.UUID

import io.greenbus.edge.amqp.channel.AmqpChannelHandler
import io.greenbus.edge.amqp.impl2.AmqpService
import io.greenbus.edge.colset.channel.ChannelHandler
import io.greenbus.edge.colset.client.MultiChannelColsetClientImpl
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.proto.provider.ProtoSerializationProvider

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object PeerServerTest {

  def main(args: Array[String]): Unit = {

    val sessionId = PeerSessionId(UUID.randomUUID(), 0)

    val peerChannelMachine = new PeerChannelMachine("Peer", sessionId)

    val channelHandler = new ChannelHandler(peerChannelMachine)

    val service = AmqpService.build(Some("server"))

    val serialization = new ProtoSerializationProvider
    val amqpHandler = new AmqpChannelHandler(service.eventLoop, new ChannelParserImpl(sessionId, serialization), serialization, channelHandler)

    service.listen("127.0.0.1", 50001, amqpHandler)

    System.in.read()
  }
}

object GatewayTest {

  def main(args: Array[String]): Unit = {

    val service = AmqpService.build(Some("server"))

    val connection = Await.result(service.connect("127.0.0.1", 50001, 5000), 5000.milliseconds)

    val describer = new ChannelDescriberImpl
    val responseParser = new ClientResponseParser
    val serialization = new ProtoSerializationProvider

    val session = Await.result(connection.open(describer, responseParser, serialization), 5000.milliseconds)

    val client = new MultiChannelColsetClientImpl(session)

    val gateway = Await.result(client.openGatewayClient(), 5000.milliseconds)

    gateway.events.push(GatewayEvents(Some(Set(SymbolVal("my_route"))), Seq()))

    System.in.read()
  }
}