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
import java.util.concurrent.{ExecutorService, Executors}

import io.greenbus.edge.CallMarshaller
import io.greenbus.edge.amqp.channel.AmqpChannelHandler
import io.greenbus.edge.amqp.impl2.AmqpService
import io.greenbus.edge.colset.channel.ChannelHandler
import io.greenbus.edge.colset.client.{ColsetClient, MultiChannelColsetClientImpl}
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.gateway.GatewayRouteSource
import io.greenbus.edge.colset.proto.provider.ProtoSerializationProvider

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Common {

  def client(service: AmqpService, host: String, port: Int): ColsetClient = {

    val connection = Await.result(service.connect(host, port, 5000), 5000.milliseconds)

    val describer = new ChannelDescriberImpl
    val responseParser = new ClientResponseParser
    val serialization = new ProtoSerializationProvider

    val session = Await.result(connection.open(describer, responseParser, serialization), 5000.milliseconds)

    new MultiChannelColsetClientImpl(session)
  }
}

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


object SecondPeerServerTest {

  def main(args: Array[String]): Unit = {

    val sessionId = PeerSessionId(UUID.randomUUID(), 0)

    val peerChannelMachine = new PeerChannelMachine("SecondPeer", sessionId)

    val channelHandler = new ChannelHandler(peerChannelMachine)

    val service = AmqpService.build(Some("server"))

    val serialization = new ProtoSerializationProvider
    val amqpHandler = new AmqpChannelHandler(service.eventLoop, new ChannelParserImpl(sessionId, serialization), serialization, channelHandler)

    service.listen("127.0.0.1", 50002, amqpHandler)

    val client = Common.client(service, "127.0.0.1", 50001)

    val (peerSession, channel) = Await.result(client.openPeerLinkClient(), 5000.milliseconds)

    peerChannelMachine.peerOpened(peerSession, channel)

    System.in.read()
  }
}

object GatewayTest {

  def main(args: Array[String]): Unit = {

    val service = AmqpService.build(Some("gateway"))

    val client = Common.client(service, "127.0.0.1", 50001)

    val channel = Await.result(client.openGatewayChannel(), 5000.milliseconds)

    val exe = new ExecutorEventThread
    val gatewaySource = GatewayRouteSource.build(exe)

    val route = gatewaySource.route(SymbolVal("my_route"))

    var setElems: Vector[Long] = Vector(5, 11)
    def toSet: Set[TypeValue] = setElems.map(Int64Val).toSet

    val setRow1 = route.setRow(TableRow("set_table", SymbolVal("row1")))
    setRow1.update(Set(Int64Val(5), Int64Val(11)))

    val appRow1 = route.appendSetRow(TableRow("append_table", SymbolVal("row1")), 30)
    appRow1.append(Int64Val(22), Int64Val(23))

    route.requests.bind { requests =>
      requests.foreach { req =>
        println("Got service request: " + req)
        req.respond(BoolVal(true))
      }
    }

    route.flushEvents()

    gatewaySource.connect(channel)

    var i = 0
    while (true) {
      setElems = setElems.drop(1) :+ (12L + i * 3)
      setRow1.update(toSet)
      appRow1.append(Int64Val(44L + i * 4))
      route.flushEvents()

      i += 1
      Thread.sleep(2000)
    }

    System.in.read()
  }
}

object SubscriberTest {

  def main(args: Array[String]): Unit = {

    val service = AmqpService.build(Some("subscriber"))

    val client = Common.client(service, "127.0.0.1", 50002)

    val (peerSess, subChannel) = Await.result(client.openPeerLinkClient(), 5000.milliseconds)

    subChannel.subscriptions.push(Set(
      RowId(SymbolVal("my_route"), "set_table", SymbolVal("row1")),
      RowId(SymbolVal("my_route"), "append_table", SymbolVal("row1"))))

    subChannel.events.bind { events =>
      println(events)
    }

    subChannel.responses.bind(responses => println(responses))

    var i = 0
    while (true) {
      val correlation = i
      subChannel.requests.push(Seq(ServiceRequest(RowId(SymbolVal("my_route"), "request_table", SymbolVal("req1")), Int64Val(55 + i), Int64Val(correlation))))
      i += 1
      Thread.sleep(2000)
    }

  }
}

class ExecutorEventThread extends CallMarshaller {

  private val s = Executors.newSingleThreadExecutor()

  def marshal(f: => Unit): Unit = {
    s.execute(new Runnable {
      def run(): Unit = f
    })
  }

  def close(): Unit = {
    s.shutdown()
  }
}