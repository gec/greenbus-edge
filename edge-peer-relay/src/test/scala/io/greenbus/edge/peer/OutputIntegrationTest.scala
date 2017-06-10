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
import io.greenbus.edge.flow
import io.greenbus.edge.flow.Closeable
import io.greenbus.edge.peer.TestModel.{ OutputProducer, Producer1, Producer2, TypesProducer }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ BeforeAndAfterEach, FunSuite, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Promise }

@RunWith(classOf[JUnitRunner])
class OutputIntegrationTest extends FunSuite with Matchers with BeforeAndAfterEach with BaseEdgeIntegration with LazyLogging {
  import EdgeMatchers._
  import EdgeSubHelpers._

  test("output key status") {
    import EdgeSubHelpers._

    startRelay()

    val producerA = new TestProducer
    val producer = new OutputProducer(producerA.producerMgr, "out01")
    producerA.connect()

    producer.outStatus.handle.update(OutputKeyStatus(producer.uuid, 0, None))
    producer.buffer.flush()

    val params = SubscriptionParams(outputKeys = Seq(producer.outStatus.endpointPath))

    val consA = new TestConsumer(params)

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          case up: IdOutputKeyUpdate => up.id == producer.outStatus.endpointPath && up.data == Pending
        },
        fixed {
          case up: IdOutputKeyUpdate => up.id == producer.outStatus.endpointPath && up.data == DataUnresolved
        }), 5000)

    consA.connect()

    consA.queue.awaitListen(
      prefixMatcher(
        fixed {
          idOutputKeyResolved(producer.outStatus.endpointPath) { v: OutputKeyUpdate =>
            v.value == OutputKeyStatus(producer.uuid, 0, None)
          }
        }), 5000)

    producer.outRcv.bind(new flow.Responder[OutputParams, OutputResult] {
      def handle(obj: OutputParams, respond: (OutputResult) => Unit): Unit = {
        respond(OutputSuccess(Some(ValueString("response 1"))))
      }
    })

    val prom = Promise[OutputResult]
    consA.consumer.queuingServiceClient.send(OutputRequest(producer.outStatus.endpointPath, OutputParams()), r => prom.complete(r))

    Await.result(prom.future, 5000.milliseconds) shouldEqual OutputSuccess(Some(ValueString("response 1")))

  }
}

