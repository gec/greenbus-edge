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

import io.greenbus.edge.api.stream.{ DynamicDataKey, KeyMetadata }
import io.greenbus.edge.api.{ EndpointId, EndpointPath, Path }
import io.greenbus.edge.data.{ ValueDouble, ValueString }

object TestModel {

  class Producer1(producer: ProducerServices) {

    val endpointId = EndpointId(Path("my-endpoint"))
    val builder = producer.endpointBuilder(endpointId)

    val seriesDataKey = Path("series-double-1")
    val seriesEndPath = EndpointPath(endpointId, seriesDataKey)

    val series1 = builder.seriesValue(seriesDataKey, KeyMetadata(indexes = Map(Path("index1") -> ValueString("value 1"))))
    val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

    def updateAndFlush(v: Double, time: Long): Unit = {
      series1.update(ValueDouble(v), time)
      buffer.flush()
    }
    def updateAndFlush(seq: Seq[(Double, Long)]): Unit = {
      seq.foreach { case (v, time) => series1.update(ValueDouble(v), time) }
      buffer.flush()
    }

    def close(): Unit = {
      buffer.close()
    }
  }

  class Producer2(producer: ProducerServices) {

    val endpointId = EndpointId(Path("my-endpoint-2"))
    val builder = producer.endpointBuilder(endpointId)

    val seriesDataKey = Path("series-double-2A")
    val seriesEndPath = EndpointPath(endpointId, seriesDataKey)

    val series1 = builder.seriesValue(seriesDataKey, KeyMetadata(indexes = Map(Path("index2") -> ValueString("value 2"))))
    val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

    def updateAndFlush(v: Double, time: Long): Unit = {
      series1.update(ValueDouble(v), time)
      buffer.flush()
    }
    def updateAndFlush(seq: Seq[(Double, Long)]): Unit = {
      seq.foreach { case (v, time) => series1.update(ValueDouble(v), time) }
      buffer.flush()
    }

    def close(): Unit = {
      buffer.close()
    }
  }

  object KeyEntry {
    def build[A](endpointId: EndpointId, key: Path)(bld: Path => A): KeyEntry[A] = {
      new KeyEntry[A](endpointId, key, bld(key))
    }
  }
  class KeyEntry[A](endpointId: EndpointId, val key: Path, val handle: A) {
    val endpointPath = EndpointPath(endpointId, key)
  }

  class TypesProducer(producer: ProducerServices, suffix: String) {

    val endpointId = EndpointId(Path(s"my-types-endpoint-$suffix"))
    val builder = producer.endpointBuilder(endpointId)

    val series1 = KeyEntry.build(endpointId, Path("series-double-1")) { key => builder.seriesValue(key, KeyMetadata(indexes = Map(Path("index2") -> ValueString("value 2")))) }
    val activeSet1 = KeyEntry.build(endpointId, Path("active-set-1")) { key => builder.activeSet(key, KeyMetadata(indexes = Map(Path("index2") -> ValueString("value 2")))) }
    val kv1 = KeyEntry.build(endpointId, Path("latest-kv-1")) { key => builder.latestKeyValue(key, KeyMetadata(indexes = Map(Path("index2") -> ValueString("value 2")))) }
    val event1 = KeyEntry.build(endpointId, Path("events-1")) { key => builder.topicEventValue(key, KeyMetadata(indexes = Map(Path("index2") -> ValueString("value 2")))) }

    val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

    def updateAndFlush(v: Double, time: Long): Unit = {
      series1.handle.update(ValueDouble(v), time)
      buffer.flush()
    }
    def updateAndFlush(seq: Seq[(Double, Long)]): Unit = {
      seq.foreach { case (v, time) => series1.handle.update(ValueDouble(v), time) }
      buffer.flush()
    }

    def close(): Unit = {
      buffer.close()
    }
  }

  class OutputProducer(producer: ProducerServices, suffix: String) {

    val uuid = UUID.randomUUID()

    val endpointId = EndpointId(Path(s"my-endpoint-$suffix"))
    val builder = producer.endpointBuilder(endpointId)

    val outStatus = KeyEntry.build(endpointId, Path("output-key-1")) { key => builder.outputStatus(key) }
    val outRcv = builder.registerOutput(Path("output-key-1"))

    val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

    def close(): Unit = {
      buffer.close()
    }
  }

  class DynamicKeyProducer(producer: ProducerServices, suffix: String, dynamicDataKey: DynamicDataKey) {
    val endpointId = EndpointId(Path(s"my-endpoint-$suffix"))
    val builder = producer.endpointBuilder(endpointId)

    builder.dynamic("dset", dynamicDataKey)

    val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

    def close(): Unit = {
      buffer.close()
    }
  }
}
