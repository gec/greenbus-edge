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

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.data.{ ValueDouble, ValueString, ValueUInt32 }
import io.greenbus.edge.flow.Closeable
import io.greenbus.edge.peer.TestModel.{ Producer1, Producer2, TypesProducer }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ BeforeAndAfterEach, FunSuite, Matchers }

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

  }

  test("Producer comes up after consumer, relay reboots, consumer connects before producer") {
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
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)

    startRelay()

    connectConsumer(consumer)

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

    stopRelay()

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)

    producer.updateAndFlush(4.11, 7)

    startRelay()

    logger.info("connecting consumer")
    connectConsumer(consumer)

    logger.info("connecting producer")
    connectProducer(producerMgr)

    flatQueue.awaitListen(
      prefixMatcher(
        fixed {
          dataKeyResolved { v: DataKeyUpdate =>
            v.value == SeriesUpdate(ValueDouble(4.11), 7)
          }
        }), 5000)
  }

  test("Resolved absent lifecycle") {
    import EdgeSubHelpers._

    startRelay()

    val producerA = new TestProducer
    val producer1 = new Producer1(producerA.producerMgr)

    val nonexistentEndPath = EndpointPath(producer1.endpointId, Path("nonexistent"))

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        series = Seq(nonexistentEndPath)))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.id == nonexistentEndPath && up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.id == nonexistentEndPath && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    producerA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.id == nonexistentEndPath && up.data == ResolvedAbsent
        }), 5000)
  }

  test("Two consumers, producer first, one unsubscribes") {
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

    logger.info(s"Connecting B")
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

  test("Subscription reconnect") {
    import EdgeSubHelpers._

    startRelay()

    val producerA = new TestProducer
    val producer = new Producer1(producerA.producerMgr)
    producerA.connect()

    producer.updateAndFlush(2.33, 5)

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

    consA.connect()

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

    consA.disconnect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.data == DataUnresolved
        }), 5000)

    val updates3 = Seq[(Double, Long)]((10.33, 13), (12.33, 15))
    producer.updateAndFlush(updates3)

    val updates4 = Seq[(Double, Long)]((14.33, 17), (16.33, 19))
    producer.updateAndFlush(updates4)

    Thread.sleep(500)
    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        matchSeriesUpdates(updates3 ++ updates4): _*), 5000)
  }

  test("Two producers") {
    import EdgeSubHelpers._

    startRelay()

    val producerA = new TestProducer
    val producer1 = new Producer1(producerA.producerMgr)
    producerA.connect()

    val producerB = new TestProducer
    val producer2 = new Producer2(producerB.producerMgr)

    producer1.updateAndFlush(2.33, 5)

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        series = Seq(producer1.seriesEndPath, producer2.seriesEndPath)))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.id == producer1.seriesEndPath && up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.id == producer2.seriesEndPath && up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.id == producer2.seriesEndPath && up.data == DataUnresolved
        },
        fixed {
          case up: IdDataKeyUpdate => up.id == producer1.seriesEndPath && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer1.seriesEndPath) { v: DataKeyUpdate =>
            v.value == SeriesUpdate(ValueDouble(2.33), 5)
          }
        }), 5000)

    producerB.connect()

    producer2.updateAndFlush(3.66, 6)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer2.seriesEndPath) { v: DataKeyUpdate =>
            v.value == SeriesUpdate(ValueDouble(3.66), 6)
          }
        }), 5000)

    producer1.updateAndFlush(4.33, 7)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer1.seriesEndPath) { v: DataKeyUpdate =>
            v.value == SeriesUpdate(ValueDouble(4.33), 7)
          }
        }), 5000)

    producerA.disconnect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.id == producer1.seriesEndPath && up.data == DataUnresolved
        }), 5000)

    producer2.updateAndFlush(5.66, 8)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer2.seriesEndPath) { v: DataKeyUpdate =>
            v.value == SeriesUpdate(ValueDouble(5.66), 8)
          }
        }), 5000)
  }

  test("Kv subscribe before first publish/flush") {
    import EdgeSubHelpers._

    startRelay()

    val producerA = new TestProducer
    val producer = new TypesProducer(producerA.producerMgr, "type01")
    producerA.connect()

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        keyValues = Seq(producer.kv1.endpointPath)))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.id == producer.kv1.endpointPath && up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.id == producer.kv1.endpointPath && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    producer.kv1.handle.update(ValueString("v01"))
    producer.buffer.flush()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer.kv1.endpointPath) { v: DataKeyUpdate =>
            v.value == KeyValueUpdate(ValueString("v01"))
          }
        }), 5000)
  }

  test("Kv updates") {
    import EdgeSubHelpers._

    startRelay()

    val producerA = new TestProducer
    val producer = new TypesProducer(producerA.producerMgr, "type01")
    producerA.connect()

    producer.kv1.handle.update(ValueString("v01"))
    producer.buffer.flush()

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        keyValues = Seq(producer.kv1.endpointPath)))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.id == producer.kv1.endpointPath && up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.id == producer.kv1.endpointPath && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer.kv1.endpointPath) { v: DataKeyUpdate =>
            v.value == KeyValueUpdate(ValueString("v01"))
          }
        }), 5000)

    producer.kv1.handle.update(ValueString("v02"))
    producer.buffer.flush()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer.kv1.endpointPath) { v: DataKeyUpdate =>
            v.value == KeyValueUpdate(ValueString("v02"))
          }
        }), 5000)
  }

  test("event topics updates") {
    import EdgeSubHelpers._

    startRelay()

    val producerA = new TestProducer
    val producer = new TypesProducer(producerA.producerMgr, "type01")
    producerA.connect()

    producer.event1.handle.update(Path("topic"), ValueString("v01"), 8)
    producer.buffer.flush()

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        topicEvent = Seq(producer.event1.endpointPath)))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.id == producer.event1.endpointPath && up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.id == producer.event1.endpointPath && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer.event1.endpointPath) { v: DataKeyUpdate =>
            v.value == TopicEventUpdate(Path("topic"), ValueString("v01"), 8)
          }
        }), 5000)

    producer.event1.handle.update(Path("topic"), ValueString("v02"), 10)
    producer.buffer.flush()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer.event1.endpointPath) { v: DataKeyUpdate =>
            v.value == TopicEventUpdate(Path("topic"), ValueString("v02"), 10)
          }
        }), 5000)
  }

  // need to fix added showing up on first resolve
  test("active sets updates") {
    import EdgeSubHelpers._

    startRelay()

    val producerA = new TestProducer
    val producer = new TypesProducer(producerA.producerMgr, "type01")
    producerA.connect()

    producer.activeSet1.handle.update(Map(ValueString("Key01") -> ValueUInt32(2), ValueString("Key02") -> ValueUInt32(3)))
    producer.buffer.flush()

    val params = SubscriptionParams(
      dataKeys = DataKeySubscriptionParams(
        activeSet = Seq(producer.activeSet1.endpointPath)))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDataKeyUpdate => up.id == producer.activeSet1.endpointPath && up.data == Pending
        },
        fixed {
          case up: IdDataKeyUpdate => up.id == producer.activeSet1.endpointPath && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer.activeSet1.endpointPath) { v: DataKeyUpdate =>
            v.value == ActiveSetUpdate(Map(ValueString("Key01") -> ValueUInt32(2), ValueString("Key02") -> ValueUInt32(3)), Set(), Set((ValueString("Key01"), ValueUInt32(2)), (ValueString("Key02"), ValueUInt32(3))), Set())
          }
        }), 5000)

    producer.activeSet1.handle.update(Map(ValueString("Key03") -> ValueUInt32(5), ValueString("Key02") -> ValueUInt32(4)))
    producer.buffer.flush()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDataKeyResolved(producer.activeSet1.endpointPath) { v: DataKeyUpdate =>
            v.value == ActiveSetUpdate(Map(ValueString("Key03") -> ValueUInt32(5), ValueString("Key02") -> ValueUInt32(4)), Set(ValueString("Key01")), Set((ValueString("Key03"), ValueUInt32(5))), Set((ValueString("Key02"), ValueUInt32(4))))
          }
        }), 5000)
  }
}

