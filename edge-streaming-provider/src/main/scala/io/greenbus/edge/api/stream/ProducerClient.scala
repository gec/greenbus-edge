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
package io.greenbus.edge.api.stream

import io.greenbus.edge.api._
import io.greenbus.edge.data.{ IndexableValue, SampleValue, Value }
import io.greenbus.edge.flow.{ QueuedDistributor, Receiver, Source }
import io.greenbus.edge.stream._

case class KeyMetadata(indexes: Map[Path, IndexableValue] = Map(), metadata: Map[Path, Value] = Map())

trait DynamicDataKey {
  def subscribed(path: Path): Unit
  def unsubscribed(path: Path): Unit
}

trait EndpointBuilder {

  def setIndexes(paramIndexes: Map[Path, IndexableValue]): Unit
  def setMetadata(paramMetadata: Map[Path, Value]): Unit

  def seriesValue(key: Path, metadata: KeyMetadata = KeyMetadata()): SeriesValueHandle

  def latestKeyValue(key: Path, metadata: KeyMetadata = KeyMetadata()): LatestKeyValueHandle

  def topicEventValue(key: Path, metadata: KeyMetadata = KeyMetadata()): TopicEventHandle

  def activeSet(key: Path, metadata: KeyMetadata = KeyMetadata()): ActiveSetHandle

  def outputStatus(key: Path, metadata: KeyMetadata = KeyMetadata()): OutputStatusHandle

  //def outputRequests(key: Path, handler: Responder[OutputParams, OutputResult]): Unit

  def registerOutput(key: Path): Receiver[OutputParams, OutputResult]

  def dynamic(set: String, callbacks: DynamicDataKey): Unit

  def build(seriesBuffersSize: Int, eventBuffersSize: Int): ProducerHandle
}

trait OutputStatusHandle {
  def update(status: OutputKeyStatus): Unit
}

trait SeriesValueHandle {
  def update(value: SampleValue, timeMs: Long): Unit
}
trait TopicEventHandle {
  def update(topic: Path, value: Value, timeMs: Long): Unit
}
trait LatestKeyValueHandle {
  def update(value: Value): Unit
}
trait ActiveSetHandle {
  def update(value: Map[IndexableValue, Value]): Unit
}

trait DataValueDistributor[A] {
  protected val queue = new QueuedDistributor[A]
  def source: Source[A] = queue
}

sealed trait DataValueQueue

trait ProducerUserBuffer {
  def enqueue(path: Path, data: TypeValue)
}
trait ProducerHandle {
  def flush(): Unit
  def close(): Unit
}
