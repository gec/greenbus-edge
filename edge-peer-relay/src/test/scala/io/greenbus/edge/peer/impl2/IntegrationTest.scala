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
package io.greenbus.edge.peer.impl2

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.data.ValueDouble
import io.greenbus.edge.peer.TestModel.Producer1
import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterEach, FunSuite, Matchers }
import org.scalatest.junit.JUnitRunner

object EdgeMatchers {
  import EdgeSubHelpers._

  def idDataKeyResolved(endPath: EndpointPath)(f: DataKeyUpdate => Boolean): PartialFunction[IdentifiedEdgeUpdate, Boolean] = {
    case up: IdDataKeyUpdate =>
      val valueMatched = up.data match {
        case ResolvedValue(v) =>
          v match {
            case v: DataKeyUpdate => f(v)
            case _ => false
          }
        case _ => false
      }

      up.id == endPath && valueMatched
  }

  def dataKeyResolved(f: DataKeyUpdate => Boolean): PartialFunction[IdentifiedEdgeUpdate, Boolean] = {
    case up: IdDataKeyUpdate =>
      up.data match {
        case ResolvedValue(v) =>
          v match {
            case v: DataKeyUpdate => f(v)
            case _ => false
          }
        case _ => false
      }
  }

  def matchSeriesUpdates(seq: Seq[(Double, Long)]) = {
    seq.map {
      case (v, t) => fixed {
        dataKeyResolved { up: DataKeyUpdate => up.value == SeriesUpdate(ValueDouble(v), t) }
      }
    }
  }
}

//@RunWith(classOf[JUnitRunner])
class IntegrationTest extends FunSuite with Matchers with BeforeAndAfterEach with BaseEdgeIntegration with LazyLogging {
  import EdgeMatchers._

  test("Producer comes up after consumer then quits first") {
    import EdgeSubHelpers._

    val consumer = buildConsumer()
    val subClient = consumer.subscriptionClient

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        series = Seq(EndpointPath(EndpointId(Path("my-endpoint")), Path("series-double-1")))))

    val subscription = subClient.subscribe(params)

    val flatQueue = new FlatQueue
    subscription.updates.bind(flatQueue.received)

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.data == Disconnected
        }), 5000)

    startRelay()

    connectConsumer(consumer)

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)

    val producerMgr = buildProducer()
    val producer = new Producer1(producerMgr)
    connectProducer(producerMgr)

    producer.updateAndFlush(2.33, 5)

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          dataKeyResolved { v: DataKeyUpdate =>
            v.value == SeriesUpdate(ValueDouble(2.33), 5)
          }
        }), 5000)

    producer.updateAndFlush(4.33, 6)

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          dataKeyResolved { v: DataKeyUpdate =>
            v.value == SeriesUpdate(ValueDouble(4.33), 6)
          }
        }), 5000)

    producer.close()

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)

    logger.info("stopping relay")
    stopRelay()

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == Disconnected
        }), 5000)
  }
}

