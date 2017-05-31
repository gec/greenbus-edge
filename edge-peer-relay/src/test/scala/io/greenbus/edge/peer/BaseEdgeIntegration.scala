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
import io.greenbus.edge.amqp.impl.AmqpListener
import io.greenbus.edge.api.IdentifiedEdgeUpdate
import io.greenbus.edge.flow.Closeable
import io.greenbus.edge.stream.PeerSessionId
import io.greenbus.edge.thread.EventThreadService
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object EdgeSubHelpers extends TypedSubHelpers[IdentifiedEdgeUpdate]

trait BaseEdgeIntegration {
  self: BeforeAndAfterEach =>

  private var relayOpt = Option.empty[PeerRelay]
  private var serverOpt = Option.empty[AmqpListener]
  private var executors = Vector.empty[EventThreadService]
  private var closeables = Vector.empty[Closeable]

  protected def buildConsumer(name: String = "consumer"): StreamConsumerManager = {
    val exe = EventThreadService.build(name)
    executors :+= exe
    StreamConsumerManager.build(exe)
  }

  protected def buildProducer(name: String = "producer"): ProducerManager = {
    val exe = EventThreadService.build(name)
    executors :+= exe
    new ProducerManager(exe)
  }

  protected def connectConsumer(consumer: StreamConsumerManager): Closeable = {
    val service = AmqpService.build()
    val result = Await.result(RetryingConnector.client(service, "127.0.0.1", 50555, 5000), 5000.milliseconds)
    val (sess, link) = Await.result(result.client.openPeerLinkClient(), 5000.milliseconds)
    consumer.connected(sess, link)
    val closeable = new Closeable {
      private var already = false
      def close(): Unit = {
        if (!already) {
          link.close()
          result.close()
          service.close()
          already = true
        }
      }
    }
    closeables :+= closeable
    closeable
  }

  protected def connectProducer(producer: ProducerManager): Closeable = {
    val service = AmqpService.build()
    val result = Await.result(RetryingConnector.client(service, "127.0.0.1", 50555, 5000), 5000.milliseconds)
    val gatewayChannel = Await.result(result.client.openGatewayChannel(), 5000.milliseconds)
    producer.connected(gatewayChannel)
    val closeable = new Closeable {
      private var already = false
      def close(): Unit = {
        if (!already) {
          gatewayChannel.close()
          result.close()
          service.close()
          already = true
        }
      }
    }
    closeables :+= closeable
    closeable
  }

  override protected def beforeEach(): Unit = {
    //startRelay()
  }

  override protected def afterEach(): Unit = {
    stopRelay()
    executors.foreach(_.close())
    executors = Vector()
    closeables.foreach(_.close())
    closeables = Vector()
  }

  protected def startRelay(): Unit = {
    val sessionId = PeerSessionId(UUID.randomUUID(), 0)
    val relay = new PeerRelay(sessionId)
    relayOpt = Some(relay)
    val server = Await.result(relay.listen("127.0.0.1", 50555), 5000.milliseconds)
    serverOpt = Some(server)
  }

  protected def stopRelay(): Unit = {
    serverOpt.foreach(_.close())
    serverOpt = None
    relayOpt.foreach(_.close())
    relayOpt = None
  }
}
