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
package io.greenbus.edge

import java.util.UUID

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.channel.actor.ActorChannelEngine
import io.greenbus.edge.channel.{ TransferChannelReceiver, TransferChannelSender }

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

object ChannelTest extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load()
    val slf4jConfig = ConfigFactory.parseString("""akka { loggers = ["akka.event.slf4j.Slf4jLogger"] }""")
    val akkaConfig = slf4jConfig.withFallback(rootConfig)
    val system = ActorSystem("brokerTest", akkaConfig)

    val setHandler = new EdgeServerChannelSetHandler {
      def handle(set: EdgeChannelSet): Unit = {
        println(s"got set: $set")
      }
    }

    val handler = new EdgeServerChannelHandlerImpl(setHandler)

    val server = new ActorChannelEngine(system, _ => handler)

    val client = server.client()

    val sessionId = PersistenceSessionId(UUID.randomUUID(), 0)
    val sourceId = ClientSessionSourceId(sessionId)
    val endpointId = NamedEndpointId("my-test-endpoint")
    val pubId = EndpointPublisherId(sourceId, sessionId, endpointId)

    import EdgeChannels._
    val pubFut = client.openSender[EndpointPublishMessage, EndpointPublishDesc](EndpointPublishDesc(pubId))
    val outFut = client.openReceiver[PublisherOutputRequestMessage, PublisherOutputRequestDesc](PublisherOutputRequestDesc(pubId))
    val outRespFut = client.openSender[PublisherOutputResponseMessage, PublisherOutputResponseDesc](PublisherOutputResponseDesc(pubId))

    val allFut = pubFut.zip(outFut).zip(outRespFut)

    println(Await.result(allFut, 5000.milliseconds))
  }
}