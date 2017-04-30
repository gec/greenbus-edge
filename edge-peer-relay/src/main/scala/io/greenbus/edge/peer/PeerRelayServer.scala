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

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.amqp.AmqpService
import io.greenbus.edge.amqp.channel.AmqpChannelHandler
import io.greenbus.edge.amqp.stream.ChannelParserImpl
import io.greenbus.edge.amqp.impl.AmqpListener
import io.greenbus.edge.api.stream.index.IndexProducer
import io.greenbus.edge.flow._
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.channel.ChannelHandler
import io.greenbus.edge.stream.gateway.GatewayRouteSource
import io.greenbus.edge.stream.proto.provider.ProtoSerializationProvider
import io.greenbus.edge.thread.{ CallMarshaller, EventThreadService }

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.{ Success, Try }

object PeerRelayServer {

  def main(args: Array[String]): Unit = {

    val baseDir = Option(System.getProperty("io.greenbus.config.base")).getOrElse("")
    val amqpConfigPath = Option(System.getProperty("io.greenbus.edge.config.peer")).map(baseDir + _).getOrElse(baseDir + "io.greenbus.edge.peer.relay.cfg")
    val settings = PeerRelaySettings.load(amqpConfigPath)

    val sessionId = PeerSessionId(UUID.randomUUID(), 0)

    runRelay(sessionId, settings.host, settings.port)

    System.in.read()

  }

  def runRelay(sessionId: PeerSessionId, host: String, port: Int): Future[AmqpListener] = {

    val peerChannelMachine = new PeerChannelMachine("Peer", sessionId)

    val channelHandler = new ChannelHandler(peerChannelMachine)

    val service = AmqpService.build(Some("server"))

    val exe = EventThreadService.build(s"indexer")

    connectIndexer(exe, service.eventLoop, sessionId, peerChannelMachine)

    val serialization = new ProtoSerializationProvider
    val amqpHandler = new AmqpChannelHandler(service.eventLoop, new ChannelParserImpl(sessionId, serialization), serialization, channelHandler)

    service.listen(host, port, amqpHandler)
  }

  def connectIndexer(indexerEventThread: CallMarshaller, peerThread: CallMarshaller, session: PeerSessionId, machine: PeerChannelHandler): Unit = {

    val gatewaySource = GatewayRouteSource.build(indexerEventThread)
    val indexer = new IndexProducer(indexerEventThread, gatewaySource)

    val localChannelSource = new LocalChannelSource(machine)

    val gateway = localChannelSource.gateway(indexerEventThread, peerThread)
    val sub = localChannelSource.subscribe(indexerEventThread, peerThread)

    gatewaySource.connect(gateway)
    indexer.connected(session, sub)
  }
}

class PeerRelay(sessionId: PeerSessionId) {

  private val peerChannelMachine = new PeerChannelMachine("Peer", sessionId)

  private val channelHandler = new ChannelHandler(peerChannelMachine)

  private val service = AmqpService.build(Some("server"))

  private val indexThread = EventThreadService.build(s"indexer")

  PeerRelayServer.connectIndexer(indexThread, service.eventLoop, sessionId, peerChannelMachine)

  private val serialization = new ProtoSerializationProvider
  private val amqpHandler = new AmqpChannelHandler(service.eventLoop, new ChannelParserImpl(sessionId, serialization), serialization, channelHandler)

  def listen(host: String, port: Int): Future[AmqpListener] = {
    service.listen(host, port, amqpHandler)
  }

  def close(): Unit = {
    service.close()
    indexThread.close()
  }
}
