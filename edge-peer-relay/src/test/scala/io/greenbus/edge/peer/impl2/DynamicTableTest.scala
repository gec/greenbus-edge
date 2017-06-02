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
import io.greenbus.edge.api.stream.{ DynamicDataKey, KeyMetadata }
import io.greenbus.edge.api.{ EndpointId, EndpointPath, Path, SubscriptionParams }
import io.greenbus.edge.data.{ ValueDouble, ValueString }
import io.greenbus.edge.flow.Closeable
import io.greenbus.edge.peer.ProducerServices
import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterEach, FunSuite, Matchers }
import org.scalatest.junit.JUnitRunner

class ProducerDynamic(producer: ProducerServices, table: DynamicDataKey) {

  val endpointId = EndpointId(Path("my-endpoint"))
  val builder = producer.endpointBuilder(endpointId)

  /*val seriesDataKey = Path("series-double-1")
  val seriesEndPath = EndpointPath(endpointId, seriesDataKey)

  val series1 = builder.seriesValue(seriesDataKey, KeyMetadata(indexes = Map(Path("index1") -> ValueString("value 1"))))*/

  val dynamic = builder.dynamic("dynkeys", table)

  val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

  def close(): Unit = {
    buffer.close()
  }
}

@RunWith(classOf[JUnitRunner])
class DynamicTableTest extends FunSuite with Matchers with BeforeAndAfterEach with BaseEdgeIntegration with LazyLogging {
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

  test("hello") {
    println("ran")

  }
}
