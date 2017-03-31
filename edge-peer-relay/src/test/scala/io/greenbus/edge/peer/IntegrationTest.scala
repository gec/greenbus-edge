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
import io.greenbus.edge.amqp.impl.AmqpListener
import io.greenbus.edge.api.stream.KeyMetadata
import io.greenbus.edge.api._
import io.greenbus.edge.stream.PeerSessionId
import io.greenbus.edge.stream.subscribe.ValueSync
import io.greenbus.edge.peer.DoubleConsumerTest.logger
import io.greenbus.edge.peer.TestHelpers.TypedEventQueue
import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterEach, FunSuite, Matchers }
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class IntegrationTest extends FunSuite with Matchers with BeforeAndAfterEach with LazyLogging {

  private var serverOpt = Option.empty[AmqpListener]
  private var serviceConnections = Vector.empty[EdgeServices]

  protected def services(): EdgeServices = {
    val services = AmqpEdgeService.build("127.0.0.1", 50555, 10000)
    serviceConnections :+= services
    services
  }

  override protected def beforeEach(): Unit = {
    val sessionId = PeerSessionId(UUID.randomUUID(), 0)
    val server = Await.result(PeerRelayServer.runRelay(sessionId, "127.0.0.1", 50555), 5000.milliseconds)
    serverOpt = Some(server)
  }

  override protected def afterEach(): Unit = {
    serverOpt.foreach(_.close())
    serviceConnections.foreach(_.shutdown())
    serverOpt = None
    serviceConnections = Vector()
  }

  test("Double consumer") {

    val servicesForPublisher = services()
    servicesForPublisher.start()
    val endpointId = EndpointId(Path("my-endpoint"))
    val builder = servicesForPublisher.producer.endpointBuilder(endpointId)

    val dataKey = Path("series-double-1")
    val endDataKey = EndpointPath(endpointId, dataKey)

    val series1 = builder.seriesValue(Path("series-double-1"), KeyMetadata(indexes = Map(Path("index1") -> ValueString("value 1"))))
    val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

    val now = 55
    series1.update(ValueDouble(2.33), now)
    buffer.flush()

    val servicesForConsumer = services()
    servicesForConsumer.start()

    val subClient = servicesForConsumer.consumer.subscriptionClient

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        series = Seq(EndpointPath(EndpointId(Path("my-endpoint")), Path("series-double-1")))))

    val subscription = subClient.subscribe(params)

    val sub1Q = new TypedEventQueue[Seq[IdentifiedEdgeUpdate]]

    def matchFirst(batch: Seq[IdentifiedEdgeUpdate]): Boolean = {
      if (batch.nonEmpty) {
        batch.head match {
          case IdDataKeyUpdate(k, status) =>
            if (k == endDataKey) {
              status match {
                case ResolvedValue(value) =>
                  value.descriptor.nonEmpty && value.value == SeriesUpdate(ValueDouble(2.33), now)
                case _ => false
              }
            } else {
              false
            }
          case _ => false
        }
      } else {
        false
      }
    }

    val fut = sub1Q.listen { batches =>
      batches.exists(matchFirst)
    }

    subscription.updates.bind(sub1Q.received)

    Await.result(fut, 5000.milliseconds)

    val sub2Q = new TypedEventQueue[Seq[IdentifiedEdgeUpdate]]

    val fut2 = sub2Q.listen { batches =>
      batches.exists(matchFirst)
    }

    val sub2 = subClient.subscribe(params)
    sub2.updates.bind(sub2Q.received)

    Await.result(fut2, 5000.milliseconds)
  }

}