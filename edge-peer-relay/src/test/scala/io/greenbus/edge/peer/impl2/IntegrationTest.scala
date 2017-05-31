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
import io.greenbus.edge.flow.Closeable
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

@RunWith(classOf[JUnitRunner])
class IntegrationTest extends FunSuite with Matchers with BeforeAndAfterEach with BaseEdgeIntegration with LazyLogging {
  import EdgeMatchers._
  import EdgeSubHelpers._
  import io.greenbus.edge.peer.TestModel._

  class TestConsumer(params: SubscriptionParams) {
    val consumer = buildConsumer()

    val subClient = consumer.subscriptionClient

    val subscription = subClient.subscribe(params)

    val queue = new FlatQueue
    subscription.updates.bind(queue.received)

    private var connectionOpt = Option.empty[Closeable]

    def connect(): Unit = {
      connectionOpt = Some(connectConsumer(consumer))
    }

    def unsubscribe(): Unit = {
      subscription.close()
    }

    def disconnect(): Unit = {
      connectionOpt.foreach(_.close())
    }
  }

  class TestProducer {
    val producerMgr = buildProducer()
    //val producer = new Producer1(producerMgr)

    private var connectionOpt = Option.empty[Closeable]

    def connect(): Unit = {
      connectionOpt = Some(connectProducer(producerMgr))
    }

    def disconnect(): Unit = {
      connectionOpt.foreach(_.close())
    }
  }

  test("Producer comes up after consumer then quits first") {

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
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)

    startRelay()

    connectConsumer(consumer)

    /*flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)*/

    val producerMgr = buildProducer()
    val producer = new Producer1(producerMgr)
    connectProducer(producerMgr)

    logger.info("UPDATE:")
    producer.updateAndFlush(2.33, 5)

    logger.info("WAITING:")
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

    /*flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == Disconnected
        }), 5000)*/
  }

  ignore("Two consumers, producer first, one unsubscribes") {
    import EdgeSubHelpers._

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        series = Seq(EndpointPath(EndpointId(Path("my-endpoint")), Path("series-double-1")))))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)

    startRelay()

    consA.connect()

    /*consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)*/

    val producerA = new TestProducer
    val producer = new Producer1(producerA.producerMgr)
    producerA.connect()

    producer.updateAndFlush(2.33, 5)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          dataKeyResolved { v: DataKeyUpdate =>
            v.value == SeriesUpdate(ValueDouble(2.33), 5)
          }
        }), 5000)

    val updates2 = Seq[(Double, Long)]((4.33, 7), (6.33, 9), (8.33, 11))

    producer.updateAndFlush(updates2)

    consA.queue.awaitListen(
      prefixMatcher(
        matchSeriesUpdates(updates2): _*), 5000)

    val consB = new TestConsumer(params)

    consB.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)

    consB.connect()

    consB.queue.awaitListen(
      prefixMatcher(
        matchSeriesUpdates(Seq((2.33, 5), (4.33, 7), (6.33, 9), (8.33, 11))): _*), 5000)

    val updates3 = Seq[(Double, Long)]((10.33, 13), (12.33, 15))
    producer.updateAndFlush(updates3)

    consB.queue.awaitListen(
      prefixMatcher(
        matchSeriesUpdates(updates3): _*), 5000)

    consA.queue.awaitListen(
      prefixMatcher(
        matchSeriesUpdates(updates3): _*), 5000)

    consA.unsubscribe()

    val updates4 = Seq[(Double, Long)]((14.33, 17), (16.33, 19))
    producer.updateAndFlush(updates4)

    consB.queue.awaitListen(
      prefixMatcher(
        matchSeriesUpdates(updates4): _*), 5000)
  }
}

