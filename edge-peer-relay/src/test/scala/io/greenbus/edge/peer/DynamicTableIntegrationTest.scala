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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.data.ValueDouble
import io.greenbus.edge.peer.TestModel.DynamicKeyProducer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ BeforeAndAfterEach, FunSuite, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Promise }

@RunWith(classOf[JUnitRunner])
class DynamicTableIntegrationTest extends FunSuite with Matchers with BeforeAndAfterEach with BaseEdgeIntegration with LazyLogging {
  import EdgeMatchers._

  test("dynamic table basic") {
    import EdgeSubHelpers._

    startRelay()

    val bufferOpt = new AtomicReference[Option[ProducerHandle]](None)
    val dynHandleOpt = new AtomicReference[Option[DynamicSeriesHandle]](None)

    val handle = new AtomicReference[Option[SeriesValueHandle]](None)

    val unsubProm = Promise[Boolean]

    val dyn = new DynamicDataKey {
      def subscribed(path: Path): Unit = {
        logger.debug(s"TEST GOT ADD: $path")
        val seriesHandle = dynHandleOpt.get.get.add(path)
        handle.set(Some(seriesHandle))
        seriesHandle.update(ValueDouble(0.44), 1)
        bufferOpt.get.get.flush()
      }

      def unsubscribed(path: Path): Unit = {
        logger.debug(s"TEST GOT REMOVE: $path")
        unsubProm.success(true)
      }
    }

    val producerA = new TestProducer
    val producer = new DynamicKeyProducer(producerA.producerMgr, "dyn01", dyn)

    dynHandleOpt.set(Some(producer.dynHandle))
    bufferOpt.set(Some(producer.buffer))

    producerA.connect()

    val dynKey = EndpointDynamicPath(producer.endpointId, DynamicPath("dset", Path(Seq("path", "01"))))

    val params = SubscriptionParams(dynamicDataKeys = Set(dynKey))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKey && up.data == Pending
        },
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKey && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDynamicDataKeyResolved(dynKey) {
            case up => up.value == SeriesUpdate(ValueDouble(0.44), 1)
          }
        }), 5000)

    handle.get().get.update(ValueDouble(0.55), 2)
    producer.buffer.flush()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDynamicDataKeyResolved(dynKey) {
            case up => up.value == SeriesUpdate(ValueDouble(0.55), 2)
          }
        }), 5000)

    consA.disconnect()

    Await.result(unsubProm.future, 5000.milliseconds) shouldEqual true
  }

  test("dynamic table unsub") {
    import EdgeSubHelpers._

    startRelay()

    val bufferOpt = new AtomicReference[Option[ProducerHandle]](None)
    val dynHandleOpt = new AtomicReference[Option[DynamicSeriesHandle]](None)

    val handle = new AtomicReference[Option[SeriesValueHandle]](None)

    val unsubProm = Promise[Boolean]

    val dyn = new DynamicDataKey {
      def subscribed(path: Path): Unit = {
        logger.debug(s"TEST GOT ADD: $path")
        val seriesHandle = dynHandleOpt.get.get.add(path)
        handle.set(Some(seriesHandle))
        seriesHandle.update(ValueDouble(0.44), 1)
        bufferOpt.get.get.flush()
      }

      def unsubscribed(path: Path): Unit = {
        logger.debug(s"TEST GOT REMOVE: $path")
        unsubProm.success(true)
      }
    }

    val producerA = new TestProducer
    val producer = new DynamicKeyProducer(producerA.producerMgr, "dyn01", dyn)

    dynHandleOpt.set(Some(producer.dynHandle))
    bufferOpt.set(Some(producer.buffer))

    producerA.connect()

    val dynKey = EndpointDynamicPath(producer.endpointId, DynamicPath("dset", Path(Seq("path", "01"))))

    val params = SubscriptionParams(dynamicDataKeys = Set(dynKey))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKey && up.data == Pending
        },
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKey && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDynamicDataKeyResolved(dynKey) {
            case up => up.value == SeriesUpdate(ValueDouble(0.44), 1)
          }
        }), 5000)

    consA.subscription.close()

    Await.result(unsubProm.future, 5000.milliseconds) shouldEqual true
  }

  test("dynamic table premature remove") {
    import EdgeSubHelpers._

    startRelay()

    val bufferOpt = new AtomicReference[Option[ProducerHandle]](None)
    val dynHandleOpt = new AtomicReference[Option[DynamicSeriesHandle]](None)

    val handle = new AtomicReference[Option[SeriesValueHandle]](None)

    val unsubProm = Promise[Boolean]

    val dyn = new DynamicDataKey {
      def subscribed(path: Path): Unit = {
        logger.debug(s"TEST GOT ADD: $path")
        val seriesHandle = dynHandleOpt.get.get.add(path)
        handle.set(Some(seriesHandle))
        seriesHandle.update(ValueDouble(0.44), 1)
        bufferOpt.get.get.flush()
      }

      def unsubscribed(path: Path): Unit = {
        logger.debug(s"TEST GOT REMOVE: $path")
        unsubProm.success(true)
      }
    }

    val producerA = new TestProducer
    val producer = new DynamicKeyProducer(producerA.producerMgr, "dyn01", dyn)

    dynHandleOpt.set(Some(producer.dynHandle))
    bufferOpt.set(Some(producer.buffer))

    producerA.connect()

    val dynKey = EndpointDynamicPath(producer.endpointId, DynamicPath("dset", Path(Seq("path", "01"))))

    val params = SubscriptionParams(dynamicDataKeys = Set(dynKey))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKey && up.data == Pending
        },
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKey && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDynamicDataKeyResolved(dynKey) {
            case up => up.value == SeriesUpdate(ValueDouble(0.44), 1)
          }
        }), 5000)

    dynHandleOpt.get().get.remove(dynKey.key.path)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKey && up.data == ResolvedAbsent
        }), 5000)

  }

  test("dynamic table multiple") {
    import EdgeSubHelpers._

    startRelay()

    val bufferOpt = new AtomicReference[Option[ProducerHandle]](None)
    val dynHandleOpt = new AtomicReference[Option[DynamicSeriesHandle]](None)

    val map = new ConcurrentHashMap[Path, SeriesValueHandle]()

    val unsubProm = Promise[Boolean]

    val dyn = new DynamicDataKey {
      def subscribed(path: Path): Unit = {
        logger.debug(s"TEST GOT ADD: $path")
        val seriesHandle = dynHandleOpt.get.get.add(path)
        map.put(path, seriesHandle)
        path match {
          case Path(Seq("path", "01")) =>
            seriesHandle.update(ValueDouble(0.44), 1)
          case Path(Seq("path", "02")) =>
            seriesHandle.update(ValueDouble(0.88), 2)
        }
        bufferOpt.get.get.flush()
      }

      def unsubscribed(path: Path): Unit = {
        logger.debug(s"TEST GOT REMOVE: $path")
        unsubProm.success(true)
      }
    }

    val producerA = new TestProducer
    val producer = new DynamicKeyProducer(producerA.producerMgr, "dyn01", dyn)

    dynHandleOpt.set(Some(producer.dynHandle))
    bufferOpt.set(Some(producer.buffer))

    producerA.connect()

    val dynKeyA = EndpointDynamicPath(producer.endpointId, DynamicPath("dset", Path(Seq("path", "01"))))
    val paramsA = SubscriptionParams(dynamicDataKeys = Set(dynKeyA))
    val consA = new TestConsumer(paramsA)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKeyA && up.data == Pending
        },
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKeyA && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDynamicDataKeyResolved(dynKeyA) {
            case up => up.value == SeriesUpdate(ValueDouble(0.44), 1)
          }
        }), 5000)

    val dynKeyB = EndpointDynamicPath(producer.endpointId, DynamicPath("dset", Path(Seq("path", "02"))))
    val paramsB = SubscriptionParams(dynamicDataKeys = Set(dynKeyB))
    val consB = new TestConsumer(paramsB)

    consB.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKeyB && up.data == Pending
        },
        fixed {
          case up: IdDynamicDataKeyUpdate => up.id == dynKeyB && up.data == DataUnresolved
        }), 5000)

    consB.connect()

    consB.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDynamicDataKeyResolved(dynKeyB) {
            case up => up.value == SeriesUpdate(ValueDouble(0.88), 2)
          }
        }), 5000)

    map.get(Path(Seq("path", "01"))).update(ValueDouble(0.55), 3)
    map.get(Path(Seq("path", "02"))).update(ValueDouble(0.99), 4)
    producer.buffer.flush()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDynamicDataKeyResolved(dynKeyA) {
            case up => up.value == SeriesUpdate(ValueDouble(0.55), 3)
          }
        }), 5000)

    consB.queue.awaitListen(
      prefixMatcher(
        fixed {
          idDynamicDataKeyResolved(dynKeyB) {
            case up => up.value == SeriesUpdate(ValueDouble(0.99), 4)
          }
        }), 5000)

    consA.disconnect()

    Await.result(unsubProm.future, 5000.milliseconds) shouldEqual true
  }
}

