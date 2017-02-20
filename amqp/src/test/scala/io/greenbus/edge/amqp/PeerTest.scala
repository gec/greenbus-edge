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
package io.greenbus.edge.amqp

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge._
import io.greenbus.edge.amqp.impl.AmqpIoImpl
import io.greenbus.edge.client._
import io.greenbus.edge.proto.provider.EdgeProtobufProvider

import scala.concurrent.Await
import scala.concurrent.duration._

object PeerTest extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("Starting")
    val service = new AmqpIoImpl()

    val dataSource = new DefaultStreamSource
    val peer = new Peer(service.eventLoop, dataSource)
    val edgeHandler = new EdgeServerChannelHandlerImpl(peer.channelSetHandler)
    val amqpHandler = new EdgeAmqpChannelHandlerImpl(service.eventLoop, new EdgeProtobufProvider, edgeHandler)

    service.listen("127.0.0.1", 50001, amqpHandler)

    System.in.read()
  }

}

object ClientTest extends LazyLogging {
  import PublisherCommon._

  def runClient(conn: EdgeConnection, eventThread: CallMarshaller): Unit = {

    val id = testEndpointId

    val params = ClientSubscriptionParams(
      endpointSetPrefixes = Seq(Path(Seq())),
      infoSubscriptions = Seq(id),
      dataSubscriptions = Seq(EndpointPath(id, Path("key01")), EndpointPath(id, Path("key02")), EndpointPath(id, Path("key03")), EndpointPath(id, Path("key04"))),
      outputSubscriptions = Seq(EndpointPath(id, Path("outKey01")), EndpointPath(id, Path("outKey02"))))

    val subFut = conn.openSubscription(params)
    val sub = Await.result(subFut, 5000.milliseconds)

    sub.notifications.bind { notification =>
      println("GOT DATA NOTIFICATION: " + notification)
    }

    val outputClientFut = conn.openOutputClient()
    val outputClient = Await.result(outputClientFut, 5000.milliseconds)

    val outputFut = outputClient.issueOutput(EndpointPath(id, Path("outKey01")), ClientOutputParams())
    val output = Await.result(outputFut, 5000.milliseconds)
    println("output response: " + output)

    System.in.read()
  }

  def main(args: Array[String]): Unit = {
    val service = new AmqpIoImpl()

    val connFut = service.connect("127.0.0.1", 50001, 10000)

    val conn = Await.result(connFut, 5000.milliseconds)

    val sessionFut = conn.open()

    val session = Await.result(sessionFut, 5000.milliseconds)

    val edgeConnection = new EdgeConnectionImpl(service.eventLoop, session)

    runClient(edgeConnection, service.eventLoop)
  }

}

object PublisherTest extends LazyLogging {
  import PublisherCommon._

  def runPublisher(conn: EdgeConnection, eventThread: CallMarshaller): Unit = {

    val desc = buildDesc(1)
    val sessionId = PersistenceSessionId(UUID.randomUUID(), 0)
    val pub = new EndpointPublisherImpl(eventThread, testEndpointId, desc)

    val pubConnFut = conn.connectPublisher(testEndpointId, sessionId, pub)
    val publisherConnected = Await.result(pubConnFut, 5000.milliseconds)

    logger.info("Publisher connected?")

    pub.keyValueStreams.get(Path("key01")).foreach(_.push(ValueString("the updated value")))
    pub.timeSeriesStreams.get(Path("key02")).foreach(_.push(TimeSeriesSample(System.currentTimeMillis(), ValueDouble(55.3))))

    pub.flush()

    pub.outputReceiver.bind { batch =>
      batch.requests.foreach { req =>
        println(s"Got request ${req.key}, ${req.outputRequest}")
        req.resultAsync(OutputSuccess(None))
      }
    }

    var i = 0
    while (true) {

      pub.eventStreams.get(Path("key03")).foreach(_.push(TopicEvent(Path(Seq("my", "topic")), Some(ValueString("a string " + i)))))

      pub.activeSetStreams.get(Path("key04")).foreach { h =>
        h.add(ValueString("blah " + i))
      }

      pub.flush()
      Thread.sleep(2000)
      i += 1
    }

    System.in.read()
  }

  def main(args: Array[String]): Unit = {
    val service = new AmqpIoImpl()

    val connFut = service.connect("127.0.0.1", 50001, 10000)

    val conn = Await.result(connFut, 5000.milliseconds)

    val sessionFut = conn.open()

    val session = Await.result(sessionFut, 5000.milliseconds)

    val edgeConnection = new EdgeConnectionImpl(service.eventLoop, session)

    runPublisher(edgeConnection, service.eventLoop)
  }

}

object PublisherCommon {
  val testEndpointId = NamedEndpointId("my-test-endpoint")

  def buildDesc(n: Int): ClientEndpointPublisherDesc = {

    val indexes = Map(Path("index01") -> ValueDouble(3.14d * n))
    val meta = Map(Path("meta01") -> ValueInt64(-12345 * n))
    val latestKvs = Map(
      Path("key01") -> LatestKeyValueEntry(
        ValueString("the current value"),
        MetadataDesc(
          Map(Path("keyIndex01") -> ValueSimpleString("a string")),
          Map(Path("keyMeta01") -> ValueBool(false)))))

    val timeSeries = Map(
      Path("key02") -> TimeSeriesValueEntry(
        TimeSeriesSample(System.currentTimeMillis(), ValueDouble(3.14 * n)),
        MetadataDesc(
          Map(Path("keyIndex02") -> ValueSimpleString("a string 2")),
          Map(Path("keyMeta02") -> ValueBool(false)))))

    val eventTopic = Map(
      Path("key03") -> EventEntry(MetadataDesc(Map(), Map())))

    val activeSet = Map(
      Path("key04") -> ActiveSetConfigEntry(MetadataDesc(Map(), Map())))

    ClientEndpointPublisherDesc(indexes, meta, latestKvs, timeSeries, eventTopic, activeSet, Map())
  }
}
