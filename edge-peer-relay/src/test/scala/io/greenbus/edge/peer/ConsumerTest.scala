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

import java.util.concurrent.{ Executors, ThreadFactory }

import io.greenbus.edge.amqp.colset.{ ChannelDescriberImpl, ClientResponseParser }
import io.greenbus.edge.amqp.impl2.AmqpService
import io.greenbus.edge.api.{ EndpointId, EndpointPath, Path, ValueString }
import io.greenbus.edge.api.stream.{ EdgeSubscriptionManager, EndpointProviderBuilderImpl, StreamProviderFactory, SubscriptionBuilder }
import io.greenbus.edge.colset.{ SymbolVal, TextVal }
import io.greenbus.edge.colset.client.{ ColsetClient, MultiChannelColsetClientImpl }
import io.greenbus.edge.colset.gateway.GatewayRouteSource
import io.greenbus.edge.colset.proto.provider.ProtoSerializationProvider
import io.greenbus.edge.colset.subscribe.SubscriptionManager
import io.greenbus.edge.thread.CallMarshaller

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ConsumerTest {

  def main(args: Array[String]): Unit = {

    val service = AmqpService.build(Some("gateway"))

    val client = Common.client(service, "127.0.0.1", 50001)

    val (session, channel) = Await.result(client.openPeerLinkClient(), 5000.milliseconds)

    val exe = new Common.ExecutorEventThread("event")

    val streamingSubMgr = new SubscriptionManager(exe)

    streamingSubMgr.connected(channel)

    val edgeSubMgr = new EdgeSubscriptionManager(exe, streamingSubMgr)

    val sub = SubscriptionBuilder.newBuilder
      .series(EndpointPath(EndpointId(Path("my-endpoint")), Path("series-double-1")))
      .keyValue(EndpointPath(EndpointId(Path("my-endpoint")), Path("kv-1")))
      .topicEvent(EndpointPath(EndpointId(Path("my-endpoint")), Path("event-1")))
      .build()

    val subscription = edgeSubMgr.subscribe(sub)

    subscription.updates.bind { batch =>
      println("Got batch: ")
      batch.foreach(up => println("\t" + up))
    }

    System.in.read()
  }
}

object ProviderTest {

  def main(args: Array[String]): Unit = {

    val service = AmqpService.build(Some("gateway"))

    val client = Common.client(service, "127.0.0.1", 50001)

    val channel = Await.result(client.openGatewayChannel(), 5000.milliseconds)

    val exe = new Common.ExecutorEventThread("event")
    val gatewaySource = GatewayRouteSource.build(exe)

    gatewaySource.connect(channel)

    val factory = new StreamProviderFactory(gatewaySource)

    val builder = new EndpointProviderBuilderImpl(EndpointId(Path("my-endpoint")))

    val series1 = builder.seriesDouble(Path("series-double-1"))
    val kv1 = builder.latestKeyValue(Path("kv-1"))
    val event1 = builder.topicEventValue(Path("event-1"))

    val buffer = factory.bindEndpoint(builder.build(), seriesBuffersSize = 100, eventBuffersSize = 100)

    var i = 0
    while (true) {
      val now = System.currentTimeMillis()
      series1.update(2.33 + i, now)
      kv1.update(ValueString("a value " + i))
      event1.update(Path(Seq("an", "event", "topic")), ValueString("an event string: " + i), now)
      buffer.flush()

      Thread.sleep(2000)
      i += 1
    }
  }
}

object Common {

  def client(service: AmqpService, host: String, port: Int): ColsetClient = {

    val connection = Await.result(service.connect(host, port, 5000), 5000.milliseconds)

    val describer = new ChannelDescriberImpl
    val responseParser = new ClientResponseParser
    val serialization = new ProtoSerializationProvider

    val session = Await.result(connection.open(describer, responseParser, serialization), 5000.milliseconds)

    new MultiChannelColsetClientImpl(session)
  }

  class ExecutorEventThread(id: String) extends CallMarshaller {

    private val s = Executors.newSingleThreadExecutor(new ThreadFactory {
      def newThread(runnable: Runnable): Thread = {
        new Thread(runnable, id)
      }
    })

    def marshal(f: => Unit): Unit = {
      s.execute(new Runnable {
        def run(): Unit = f
      })
    }

    def close(): Unit = {
      s.shutdown()
    }
  }
}