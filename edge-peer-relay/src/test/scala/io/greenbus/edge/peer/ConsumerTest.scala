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
import io.greenbus.edge.amqp.AmqpService
import io.greenbus.edge.amqp.stream.{ ChannelDescriberImpl, ClientResponseParser }
import io.greenbus.edge.api._
import io.greenbus.edge.api.stream._
import io.greenbus.edge.api.stream.index.{ IndexProducer, IndexingSubscriptionManager }
import io.greenbus.edge.stream.client.{ MultiChannelStreamClientImpl, StreamClient }
import io.greenbus.edge.stream.gateway.GatewayRouteSource
import io.greenbus.edge.stream.proto.provider.ProtoSerializationProvider
import io.greenbus.edge.flow.Responder
import io.greenbus.edge.thread.CallMarshaller

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

object ConsumerTest extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val services = AmqpEdgeService.build("127.0.0.1", 50001, 10000)
    services.start()
    val consumerServices = services.consumer

    val subClient = consumerServices.subscriptionClient

    val outputKey = EndpointPath(EndpointId(Path("my-endpoint")), Path("out-1"))

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        series = Seq(EndpointPath(EndpointId(Path("my-endpoint")), Path("series-double-1"))),
        keyValues = Seq(EndpointPath(EndpointId(Path("my-endpoint")), Path("kv-1"))),
        topicEvent = Seq(EndpointPath(EndpointId(Path("my-endpoint")), Path("event-1")))),
      outputKeys = Seq(outputKey),
      indexing = IndexSubscriptionParams(
        endpointPrefixes = Seq(Path(Seq())),
        dataKeyIndexes = Seq(IndexSpecifier(Path("index1"), None)),
        outputKeyIndexes = Seq(IndexSpecifier(Path("outIndex1"), None))))

    logger.debug("Subscribing...")
    val subscription = subClient.subscribe(params)

    subscription.updates.bind { batch =>
      logger.info("Got batch: ")
      batch.foreach(up => logger.info("\t" + up))
    }

    val serviceClient = consumerServices.queuingServiceClient

    var i = 0
    while (true) {

      def handleResponse(result: Try[OutputResult]): Unit = {
        println("result: " + result)
      }

      serviceClient.send(OutputRequest(outputKey, OutputParams(outputValueOpt = Some(ValueString("an output string " + i)))), handleResponse)

      i += 1
      Thread.sleep(2000)
    }

    System.in.read()
  }
}

object DoubleConsumerTest extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val services = AmqpEdgeService.build("127.0.0.1", 50001, 10000)
    services.start()
    val consumerServices = services.consumer

    val subClient = consumerServices.subscriptionClient

    val outputKey = EndpointPath(EndpointId(Path("my-endpoint")), Path("out-1"))

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        series = Seq(EndpointPath(EndpointId(Path("my-endpoint")), Path("series-double-1")))))

    logger.debug("Subscribing...")
    val subscription = subClient.subscribe(params)

    subscription.updates.bind { batch =>
      logger.info("Got batch: ")
      batch.foreach(up => logger.info("\t" + up))
    }

    Thread.sleep(2000)

    val sub2 = subClient.subscribe(params)
    sub2.updates.bind { batch =>
      logger.info(s"BATCH TWO: ")
      batch.foreach(up => logger.info("\t" + up))
    }

    System.in.read()
  }
}

object ProducerTest extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val outSession = UUID.randomUUID()

    val services = AmqpEdgeService.build("127.0.0.1", 50001, 10000)
    services.start()
    val producerServices = services.producer

    val builder = producerServices.endpointBuilder(EndpointId(Path("my-endpoint")))

    builder.setIndexes(Map(Path("endIndex1") -> ValueString("value 1")))

    val series1 = builder.seriesValue(Path("series-double-1"), KeyMetadata(indexes = Map(Path("index1") -> ValueString("value 1"))))
    val kv1 = builder.latestKeyValue(Path("kv-1"), KeyMetadata(indexes = Map(Path("index1") -> ValueString("value 2"))))
    val event1 = builder.topicEventValue(Path("event-1"))

    val out1 = builder.outputStatus(Path("out-1"), KeyMetadata(indexes = Map(Path("outIndex1") -> ValueString("value 1"))))

    val out1Rcv = builder.registerOutput(Path("out-1"))

    val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

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
      series1.update(ValueDouble(2.33 + i), now)
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

  def client(service: AmqpService, host: String, port: Int): StreamClient = {

    val connection = Await.result(service.connect(host, port, 5000), 5000.milliseconds)

    val describer = new ChannelDescriberImpl
    val responseParser = new ClientResponseParser
    val serialization = new ProtoSerializationProvider

    val session = Await.result(connection.open(describer, responseParser, serialization), 5000.milliseconds)

    new MultiChannelStreamClientImpl(session)
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