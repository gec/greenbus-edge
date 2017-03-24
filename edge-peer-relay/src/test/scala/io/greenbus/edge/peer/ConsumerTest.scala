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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ThreadFactory }

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.amqp.colset.{ ChannelDescriberImpl, ClientResponseParser }
import io.greenbus.edge.amqp.impl2.AmqpService
import io.greenbus.edge.api._
import io.greenbus.edge.api.stream._
import io.greenbus.edge.api.stream.index.IndexProducer
import io.greenbus.edge.colset.{ SymbolVal, TextVal }
import io.greenbus.edge.colset.client.{ ColsetClient, MultiChannelColsetClientImpl }
import io.greenbus.edge.colset.gateway.GatewayRouteSource
import io.greenbus.edge.colset.proto.provider.ProtoSerializationProvider
import io.greenbus.edge.colset.subscribe.{ DynamicSubscriptionManager, StreamServiceClientImpl, SubscriptionManager }
import io.greenbus.edge.flow
import io.greenbus.edge.flow.Responder
import io.greenbus.edge.thread.CallMarshaller

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

object ConsumerTest extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val service = AmqpService.build(Some("gateway"))

    val client = Common.client(service, "127.0.0.1", 50001)

    val (session, channel) = Await.result(client.openPeerLinkClient(), 5000.milliseconds)

    val exe = new Common.ExecutorEventThread("event")

    val streamingSubMgr = new DynamicSubscriptionManager(exe)

    streamingSubMgr.connected(session, channel)

    val edgeSubMgr = new EdgeSubscriptionManager(exe, streamingSubMgr)

    val outputKey = EndpointPath(EndpointId(Path("my-endpoint")), Path("out-1"))

    val sub = SubscriptionBuilder.newBuilder
      /*.series(EndpointPath(EndpointId(Path("my-endpoint")), Path("series-double-1")))
      .keyValue(EndpointPath(EndpointId(Path("my-endpoint")), Path("kv-1")))
      .topicEvent(EndpointPath(EndpointId(Path("my-endpoint")), Path("event-1")))
      .outputStatus(outputKey)*/
      .dataKeyIndex(IndexSpecifier(Path("index1"), None))
      .build()

    logger.debug("Subscribing...")
    val subscription = edgeSubMgr.subscribe(sub)

    subscription.updates.bind { batch =>
      logger.info("Got batch: ")
      batch.foreach(up => logger.info("\t" + up))
    }

    val streamServices = new StreamServiceClientImpl(channel, exe)
    val edgeServices = new ServiceClientImpl(streamServices)

    var i = 0
    /*while (true) {

      def handleResponse(result: Try[OutputResult]): Unit = {
        println("result: " + result)
      }

      edgeServices.send(OutputRequest(outputKey, OutputParams(outputValueOpt = Some(ValueString("an output string " + i)))), handleResponse)

      i += 1
      Thread.sleep(2000)
    }*/

    System.in.read()
  }
}

object ProducerTest extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val outSession = UUID.randomUUID()

    val service = AmqpService.build(Some("gateway"))

    val client = Common.client(service, "127.0.0.1", 50001)

    val channel = Await.result(client.openGatewayChannel(), 5000.milliseconds)

    val exe = new Common.ExecutorEventThread("event")
    val gatewaySource = GatewayRouteSource.build(exe)

    gatewaySource.connect(channel)

    val factory = new StreamProviderFactory(gatewaySource)

    val builder = new EndpointProducerBuilderImpl(EndpointId(Path("my-endpoint")), exe)

    val series1 = builder.seriesDouble(Path("series-double-1"), CommonMetadata(indexes = Map(Path("index1") -> ValueString("value 1"))))
    val kv1 = builder.latestKeyValue(Path("kv-1"), CommonMetadata(indexes = Map(Path("index1") -> ValueString("value 2"))))
    val event1 = builder.topicEventValue(Path("event-1"))

    val out1 = builder.outputStatus(Path("out-1"))

    /* val outHandler = new flow.Responder[OutputParams, OutputResult] {
      var i = 0
      def handle(obj: OutputParams, respond: (OutputResult) => Unit): Unit = {
        logger.info("Output params: " + obj)
        i += 1
        println(i)
        out1.update(OutputKeyStatus(outSession, i, None))
        respond(OutputSuccess(Some(ValueString("we did it"))))
      }
    }

    builder.outputRequests(Path("out-1"), outHandler)*/

    val out1Rcv = builder.registerOutput(Path("out-1"))

    val buffer = factory.bindEndpoint(builder.build(), seriesBuffersSize = 100, eventBuffersSize = 100)

    val outputInc = new AtomicInteger(0)

    out1Rcv.bind(new Responder[OutputParams, OutputResult] {
      def handle(obj: OutputParams, respond: (OutputResult) => Unit): Unit = {
        logger.info("Output params: " + obj)
        val i = outputInc.incrementAndGet()
        println(i)
        out1.update(OutputKeyStatus(outSession, i, None))
        respond(OutputSuccess(Some(ValueString("we did it"))))
        buffer.flush()
      }
    })

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

object IndexerTest extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val service = AmqpService.build(Some("indexer"))

    val client = Common.client(service, "127.0.0.1", 50001)

    val channel = Await.result(client.openGatewayChannel(), 5000.milliseconds)

    val exe = new Common.ExecutorEventThread("event")
    val gatewaySource = GatewayRouteSource.build(exe)

    gatewaySource.connect(channel)

    val (session, subChannel) = Await.result(client.openPeerLinkClient(), 5000.milliseconds)

    val producer = new IndexProducer(exe, gatewaySource)

    producer.connected(session, subChannel)

    System.in.read()
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