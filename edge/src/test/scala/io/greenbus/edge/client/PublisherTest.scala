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
package io.greenbus.edge.client

import java.util.UUID

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.greenbus.edge._
import io.greenbus.edge.channel.actor.ActorChannelEngine
import io.greenbus.edge.channel.{ Handler, Responder }
import io.greenbus.edge.thread.CallMarshaller

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

class SameThreadMarshaller extends CallMarshaller {
  def marshal(f: => Unit): Unit = f
}

object PublisherCommon {

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

    ClientEndpointPublisherDesc(indexes, meta, latestKvs, timeSeries, Map(), Map(), Map())
  }
}

object PublisherTest {
  import PublisherCommon._

  def main(args: Array[String]): Unit = {

    val marshaller = new SameThreadMarshaller

    val desc = buildDesc(1)
    val id = NamedEndpointId(Path("my-test-endpoint"))
    val pub = new EndpointPublisherImpl(marshaller, id, desc)

    val updateReceiver = pub.subscribeToUpdates()

    updateReceiver.source.bind(new Handler[EndpointPublishMessage] {
      def handle(obj: EndpointPublishMessage): Unit = {
        println("got update: " + obj)
      }
    })

    pub.keyValueStreams.get(Path("key01")).foreach(_.push(ValueString("the updated value")))
    pub.timeSeriesStreams.get(Path("key02")).foreach(_.push(TimeSeriesSample(System.currentTimeMillis(), ValueDouble(55.3))))

    pub.flush()
  }
}

object PublisherConnectTest {
  import PublisherCommon._

  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load()
    val slf4jConfig = ConfigFactory.parseString("""akka { loggers = ["akka.event.slf4j.Slf4jLogger"] }""")
    val akkaConfig = slf4jConfig.withFallback(rootConfig)
    val system = ActorSystem("brokerTest", akkaConfig)

    val setHandler = new EdgeServerChannelSetHandler {
      def handle(channelSet: EdgeChannelSet): Unit = {
        println(s"got set: $channelSet")

        import EdgeChannels._

        channelSet match {
          case pubSet: PublisherChannelSet =>
            pubSet.publishChannel.receiver.bind(new Responder[EndpointPublishMessage, Boolean] {
              def handle(obj: EndpointPublishMessage, promise: Promise[Boolean]): Unit = {
                println("server got update: " + obj)
                promise.success(true)
              }
            })

          case _ => println("did not recognize set")
        }
      }
    }

    val handler = new EdgeServerChannelHandlerImpl(setHandler)

    val server = new ActorChannelEngine(system, _ => handler)

    val (client, eventThread) = server.clientAndThread()

    val conn = new EdgeConnectionImpl(eventThread, client)

    val desc = buildDesc(1)
    val sessionId = PersistenceSessionId(UUID.randomUUID(), 0)
    val id = NamedEndpointId(Path("my-test-endpoint"))
    val pub = new EndpointPublisherImpl(eventThread, id, desc)

    val connFut = conn.connectPublisher(id, sessionId, pub)

    val connected = Await.result(connFut, 5000.milliseconds)

    pub.keyValueStreams.get(Path("key01")).foreach(_.push(ValueString("the updated value")))
    pub.timeSeriesStreams.get(Path("key02")).foreach(_.push(TimeSeriesSample(System.currentTimeMillis(), ValueDouble(55.3))))

    pub.flush()

  }
}

object PublisherToPeerTest {
  import PublisherCommon._

  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load()
    val slf4jConfig = ConfigFactory.parseString("""akka { loggers = ["akka.event.slf4j.Slf4jLogger"] }""")
    val akkaConfig = slf4jConfig.withFallback(rootConfig)
    val system = ActorSystem("brokerTest", akkaConfig)

    val dataSource = new DefaultStreamSource

    def buildServer(m: CallMarshaller) = {
      val peer = new Peer(m, dataSource)
      new EdgeServerChannelHandlerImpl(peer.channelSetHandler)
    }

    val server = new ActorChannelEngine(system, buildServer)

    val (client, eventThread) = server.clientAndThread()

    val conn = new EdgeConnectionImpl(eventThread, client)

    val desc = buildDesc(1)
    val sessionId = PersistenceSessionId(UUID.randomUUID(), 0)
    val id = NamedEndpointId(Path("my-test-endpoint"))
    val pub = new EndpointPublisherImpl(eventThread, id, desc)

    val connFut = conn.connectPublisher(id, sessionId, pub)

    val connected = Await.result(connFut, 5000.milliseconds)

    val params = ClientSubscriptionParams(
      infoSubscriptions = Seq(id),
      dataSubscriptions = Seq(EndpointPath(id, Path("key01")), EndpointPath(id, Path("key02"))),
      outputSubscriptions = Seq(EndpointPath(id, Path("outKey01")), EndpointPath(id, Path("outKey02"))))

    val subFut = conn.openSubscription(params)
    val sub = Await.result(subFut, 5000.milliseconds)

    sub.notifications.bind { notification =>
      println("GOT DATA NOTIFICATION: " + notification)
    }

    pub.keyValueStreams.get(Path("key01")).foreach(_.push(ValueString("the updated value")))
    pub.timeSeriesStreams.get(Path("key02")).foreach(_.push(TimeSeriesSample(System.currentTimeMillis(), ValueDouble(55.3))))

    pub.flush()

    Thread.sleep(500)

    connected.close()

    Thread.sleep(500)
    system.terminate()
  }
}

object PublisherToPeerWithControlTest {
  import PublisherCommon._

  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load()
    val slf4jConfig = ConfigFactory.parseString("""akka { loggers = ["akka.event.slf4j.Slf4jLogger"] }""")
    val akkaConfig = slf4jConfig.withFallback(rootConfig)
    val system = ActorSystem("brokerTest", akkaConfig)

    val dataSource = new DefaultStreamSource

    def buildServer(m: CallMarshaller) = {
      val peer = new Peer(m, dataSource)
      new EdgeServerChannelHandlerImpl(peer.channelSetHandler)
    }

    val server = new ActorChannelEngine(system, buildServer)

    val (client, eventThread) = server.clientAndThread()

    val conn = new EdgeConnectionImpl(eventThread, client)

    val desc = buildDesc(1)
    val sessionId = PersistenceSessionId(UUID.randomUUID(), 0)
    val id = NamedEndpointId(Path("my-test-endpoint"))
    val pub = new EndpointPublisherImpl(eventThread, id, desc)

    val connFut = conn.connectPublisher(id, sessionId, pub)

    val connected = Await.result(connFut, 5000.milliseconds)

    val params = ClientSubscriptionParams(
      infoSubscriptions = Seq(id),
      dataSubscriptions = Seq(EndpointPath(id, Path("key01")), EndpointPath(id, Path("key02"))),
      outputSubscriptions = Seq(EndpointPath(id, Path("outKey01")), EndpointPath(id, Path("outKey02"))))

    val subFut = conn.openSubscription(params)
    val sub = Await.result(subFut, 5000.milliseconds)

    sub.notifications.bind { notification =>
      println("GOT DATA NOTIFICATION: " + notification)
    }

    pub.keyValueStreams.get(Path("key01")).foreach(_.push(ValueString("the updated value")))
    pub.timeSeriesStreams.get(Path("key02")).foreach(_.push(TimeSeriesSample(System.currentTimeMillis(), ValueDouble(55.3))))

    pub.flush()

    pub.outputReceiver.bind { batch =>
      batch.requests.foreach { req =>
        println(s"Got request ${req.key}, ${req.outputRequest}")
        req.resultAsync(OutputSuccess(None))
      }
    }

    val outputClientFut = conn.openOutputClient()
    val outputClient = Await.result(outputClientFut, 5000.milliseconds)

    val outputFut = outputClient.issueOutput(EndpointPath(id, Path("outKey01")), ClientOutputParams())
    val output = Await.result(outputFut, 5000.milliseconds)
    println("output response: " + output)

    Thread.sleep(500)

    connected.close()

    Thread.sleep(500)
    system.terminate()
  }
}
