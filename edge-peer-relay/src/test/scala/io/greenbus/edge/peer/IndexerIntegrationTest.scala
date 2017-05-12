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
import io.greenbus.edge.api._
import io.greenbus.edge.data.ValueDouble
import io.greenbus.edge.peer.EdgeSubHelpers.{ FlatQueue, fixed, prefixMatcher }
import io.greenbus.edge.stream.PeerSessionId
import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterEach, FunSuite, Matchers }
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class IndexerIntegrationTest extends FunSuite with Matchers with BeforeAndAfterEach with BaseEdgeIntegration with LazyLogging {

  logger.debug("Test start")

  import TestModel._

  test("Indexer") {

    val consumer = buildConsumer()
    val subClient = consumer.subscriptionClient

    val params = SubscriptionParams(indexing = IndexSubscriptionParams(endpointPrefixes = Seq(Path(Seq()))))

    val subscription = subClient.subscribe(params)

    val flatQueue = new FlatQueue
    subscription.updates.bind(flatQueue.received)

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdEndpointPrefixUpdate => up.data == Pending
        },
        fixed {
          case up: IdEndpointPrefixUpdate => up.data == Disconnected
        }), 5000)

    startRelay()
    connectConsumer(consumer)

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdEndpointPrefixUpdate =>
            up.data match {
              case ResolvedValue(v) =>
                v match {
                  case v: EndpointSetUpdate =>
                    v.set == Set() && v.adds == Set() && v.removes == Set()

                  case _ => false
                }
              case _ => false
            }
        }), 5000)

    val producerMgr = buildProducer()
    val producer = new Producer1(producerMgr)
    connectProducer(producerMgr)

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdEndpointPrefixUpdate =>
            up.data match {
              case ResolvedValue(v) =>
                v match {
                  case v: EndpointSetUpdate =>
                    v.set == Set(producer.endpointId) && v.adds == Set(producer.endpointId) && v.removes == Set()
                  case _ => false
                }
              case _ => false
            }
        }), 5000)

    stopRelay()

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdEndpointPrefixUpdate => up.data == Disconnected
        }), 5000)
  }
}
