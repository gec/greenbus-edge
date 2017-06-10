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
import io.greenbus.edge.api.stream.DynamicDataKey
import io.greenbus.edge.data.ValueString
import io.greenbus.edge.flow
import io.greenbus.edge.peer.TestModel.{ DynamicKeyProducer, OutputProducer }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ BeforeAndAfterEach, FunSuite, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Promise }

@RunWith(classOf[JUnitRunner])
class DynamicTableIntegrationTest extends FunSuite with Matchers with BeforeAndAfterEach with BaseEdgeIntegration with LazyLogging {
  import EdgeMatchers._

  test("dynamic table") {
    import EdgeSubHelpers._

    startRelay()

    val dyn = new DynamicDataKey {
      def subscribed(path: Path): Unit = {
        println(s"TEST GOT ADD: $path")
      }

      def unsubscribed(path: Path): Unit = {
        println(s"TEST GOT REMOVE: $path")
      }
    }

    val producerA = new TestProducer
    val producer = new DynamicKeyProducer(producerA.producerMgr, "dyn01", dyn)
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

    Thread.sleep(2000)

  }
}

