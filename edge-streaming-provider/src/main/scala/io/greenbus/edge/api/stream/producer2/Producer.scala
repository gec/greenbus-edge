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
package io.greenbus.edge.api.stream.producer2

import io.greenbus.edge.api._
import io.greenbus.edge.api.stream._
import io.greenbus.edge.data.{ IndexableValue, SampleValue, Value }
import io.greenbus.edge.flow.Sink
import io.greenbus.edge.stream.{ SequenceCtx, TableRow, TypeValue }
import io.greenbus.edge.stream.gateway3._

import scala.collection.mutable

/*

sealed trait ProducerKeyEvent
case class AddRow(key: TableRow, ctx: SequenceCtx) extends ProducerKeyEvent
case class RowUpdate(key: TableRow, update: ProducerDataUpdate) extends ProducerKeyEvent
case class DrowRow(key: TableRow) extends ProducerKeyEvent

sealed trait ProducerEvent

case class RouteBatchEvent(route: TypeValue, events: Seq[ProducerKeyEvent]) extends ProducerEvent

case class RouteBindEvent(route: TypeValue,
  initialEvents: Seq[ProducerKeyEvent],
  dynamic: Map[String, DynamicTable],
  handler: Sink[RouteServiceRequest]) extends ProducerEvent

case class RouteUnbindEvent(route: TypeValue) extends ProducerEvent
 */

class Producer {

}

class RowUpdateBuffer extends Sink[RowUpdate] {
  private var buffer = Vector.newBuilder[RowUpdate]

  def push(obj: RowUpdate): Unit = {
    buffer += obj
  }

  def dequeue(): Seq[RowUpdate] = {
    val result = buffer.result()
    buffer = Vector.newBuilder[RowUpdate]
    result
  }
}

class SeriesPublisher(key: TableRow, updates: Sink[RowUpdate]) extends SeriesValueHandle {
  def update(value: SampleValue, timeMs: Long): Unit = {
    val v = EdgeCodecCommon.writeSampleValueSeries((value, timeMs))
    updates.push(RowUpdate(key, AppendProducerUpdate(Seq(v))))
  }
}
class LatestKeyValuePublisher(key: TableRow, updates: Sink[RowUpdate]) extends LatestKeyValueHandle {
  def update(value: Value): Unit = {
    val v = EdgeCodecCommon.writeValue(value)
    updates.push(RowUpdate(key, AppendProducerUpdate(Seq(v))))
  }
}
class TopicEventPublisher(key: TableRow, updates: Sink[RowUpdate]) extends TopicEventHandle {
  def update(topic: Path, value: Value, timeMs: Long): Unit = {
    val v = EdgeCodecCommon.writeTopicEvent((topic, value, timeMs))
    updates.push(RowUpdate(key, AppendProducerUpdate(Seq(v))))
  }
}
class ActiveSetPublisher(key: TableRow, updates: Sink[RowUpdate]) extends ActiveSetHandle {
  def update(value: Map[IndexableValue, Value]): Unit = {
    val v = EdgeCodecCommon.writeMap(value)
    updates.push(RowUpdate(key, MapProducerUpdate(v)))
  }
}

class OutputStatusPublisher(key: TableRow, updates: Sink[RowUpdate]) extends OutputStatusHandle {
  def update(status: OutputKeyStatus): Unit = {
    val v = EdgeCodecCommon.writeOutputKeyStatus(status)
    updates.push(RowUpdate(key, AppendProducerUpdate(Seq(v))))
  }
}

class EndpointBuilderImpl(endpointId: EndpointId, gateway: GatewayEventHandler) {
  private val updateBuffer = new RowUpdateBuffer
  private val adds = Vector.newBuilder[AddRow]

  private var indexes = Map.empty[Path, IndexableValue]
  private var metadata = Map.empty[Path, Value]
  private val data = mutable.Map.empty[Path, DataKeyDescriptor]
  private val outputStatuses = mutable.Map.empty[Path, OutputKeyDescriptor]

  def setIndexes(paramIndexes: Map[Path, IndexableValue]): Unit = {
    indexes = paramIndexes
  }
  def setMetadata(paramMetadata: Map[Path, Value]): Unit = {
    metadata = paramMetadata
  }

  def seriesValue(key: Path, metadata: KeyMetadata = KeyMetadata()): SeriesValueHandle = {
    val rowId = EdgeCodecCommon.dataKeyRowId(EndpointPath(endpointId, key))
    val handle = new SeriesPublisher(rowId.tableRow, updateBuffer)
    val desc = TimeSeriesValueDescriptor(metadata.indexes, metadata.metadata)
    data += (key -> desc)
    adds += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc))))
    handle
  }

  def latestKeyValue(key: Path, metadata: KeyMetadata = KeyMetadata()): LatestKeyValueHandle = {
    val rowId = EdgeCodecCommon.dataKeyRowId(EndpointPath(endpointId, key))
    val handle = new LatestKeyValuePublisher(rowId.tableRow, updateBuffer)
    val desc = LatestKeyValueDescriptor(metadata.indexes, metadata.metadata)
    data += (key -> desc)
    adds += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc))))
    handle
  }

  def topicEventValue(key: Path, metadata: KeyMetadata = KeyMetadata()): TopicEventHandle = {
    val rowId = EdgeCodecCommon.dataKeyRowId(EndpointPath(endpointId, key))
    val handle = new TopicEventPublisher(rowId.tableRow, updateBuffer)
    val desc = EventTopicValueDescriptor(metadata.indexes, metadata.metadata)
    data += (key -> desc)
    adds += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc))))
    handle
  }

  def activeSet(key: Path, metadata: KeyMetadata = KeyMetadata()): ActiveSetHandle = {
    val rowId = EdgeCodecCommon.dataKeyRowId(EndpointPath(endpointId, key))
    val handle = new ActiveSetPublisher(rowId.tableRow, updateBuffer)
    val desc = ActiveSetValueDescriptor(metadata.indexes, metadata.metadata)
    data += (key -> desc)
    adds += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc))))
    handle
  }

  def outputStatus(key: Path, metadata: KeyMetadata = KeyMetadata()): OutputStatusHandle = {
    val rowId = EdgeCodecCommon.dataKeyRowId(EndpointPath(endpointId, key))
    val handle = new OutputStatusPublisher(rowId.tableRow, updateBuffer)
    val desc = OutputKeyDescriptor(metadata.indexes, metadata.metadata)
    outputStatuses += (key -> desc)
    adds += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeOutputKeyDescriptor(desc))))
    handle
  }

  def build(): ProducerHandle = {
    val desc = EndpointDescriptor(indexes, metadata, data.toMap, outputStatuses.toMap)
    val route = EdgeCodecCommon.endpointIdToRoute(endpointId)
    new EndpointProducer(route, gateway, updateBuffer)
  }
}

class EndpointProducer(route: TypeValue, gateway: GatewayEventHandler, updateBuffer: RowUpdateBuffer) extends ProducerHandle {
  def flush(): Unit = {
    val events = updateBuffer.dequeue()
    gateway.handleEvent(RouteBatchEvent(route, events))
  }

  def close(): Unit = {
    gateway.handleEvent(RouteUnbindEvent(route))
  }
}
