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
package io.greenbus.edge.api.stream.producer

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.api.stream._
import io.greenbus.edge.data.{ IndexableValue, SampleValue, Value }
import io.greenbus.edge.flow._
import io.greenbus.edge.stream.gateway._
import io.greenbus.edge.stream.{ SequenceCtx, TableRow, TextVal, TypeValue }
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable

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

class DynamicSeriesPublisher(endpointId: EndpointId, set: String, gateway: GatewayEventHandler, updates: Sink[RowUpdate]) extends DynamicSeriesHandle {
  def add(path: Path, metadata: KeyMetadata = KeyMetadata()): SeriesValueHandle = {
    val rowId = EdgeCodecCommon.dynamicDataKeyRow(EndpointDynamicPath(endpointId, DynamicPath(set, path)))
    val handle = new SeriesPublisher(rowId.tableRow, updates)
    val desc = TimeSeriesValueDescriptor(metadata.indexes, metadata.metadata)
    gateway.handleEvent(RouteBatchEvent(EdgeCodecCommon.writeEndpointId(endpointId), Seq(AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc)))))))
    handle
  }
  def remove(path: Path): Unit = {
    val rowId = EdgeCodecCommon.dynamicDataKeyRow(EndpointDynamicPath(endpointId, DynamicPath(set, path)))
    gateway.handleEvent(RouteBatchEvent(EdgeCodecCommon.writeEndpointId(endpointId), Seq(DropRow(rowId.tableRow))))
  }
}

class DynamicKeyValuePublisher(endpointId: EndpointId, set: String, gateway: GatewayEventHandler, updates: Sink[RowUpdate]) extends DynamicKeyValueHandle {
  def add(path: Path, metadata: KeyMetadata = KeyMetadata()): LatestKeyValueHandle = {
    val rowId = EdgeCodecCommon.dynamicDataKeyRow(EndpointDynamicPath(endpointId, DynamicPath(set, path)))
    val handle = new LatestKeyValuePublisher(rowId.tableRow, updates)
    val desc = LatestKeyValueDescriptor(metadata.indexes, metadata.metadata)
    gateway.handleEvent(RouteBatchEvent(EdgeCodecCommon.writeEndpointId(endpointId), Seq(AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc)))))))
    handle
  }
  def remove(path: Path): Unit = {
    val rowId = EdgeCodecCommon.dynamicDataKeyRow(EndpointDynamicPath(endpointId, DynamicPath(set, path)))
    gateway.handleEvent(RouteBatchEvent(EdgeCodecCommon.writeEndpointId(endpointId), Seq(DropRow(rowId.tableRow))))
  }
}

class DynamicActiveSetPublisher(endpointId: EndpointId, set: String, gateway: GatewayEventHandler, updates: Sink[RowUpdate]) extends DynamicActiveSetHandle {
  def add(path: Path, metadata: KeyMetadata = KeyMetadata()): ActiveSetHandle = {
    val rowId = EdgeCodecCommon.dynamicDataKeyRow(EndpointDynamicPath(endpointId, DynamicPath(set, path)))
    val handle = new ActiveSetPublisher(rowId.tableRow, updates)
    val desc = LatestKeyValueDescriptor(metadata.indexes, metadata.metadata)
    gateway.handleEvent(RouteBatchEvent(EdgeCodecCommon.writeEndpointId(endpointId), Seq(AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc)))))))
    handle
  }
  def remove(path: Path): Unit = {
    val rowId = EdgeCodecCommon.dynamicDataKeyRow(EndpointDynamicPath(endpointId, DynamicPath(set, path)))
    gateway.handleEvent(RouteBatchEvent(EdgeCodecCommon.writeEndpointId(endpointId), Seq(DropRow(rowId.tableRow))))
  }
}

case class ProducerOutputEntry(path: Path, responder: Responder[OutputParams, OutputResult])

class EndpointBuilderImpl(endpointId: EndpointId, gatewayThread: CallMarshaller, gateway: GatewayEventHandler) extends EndpointBuilder with LazyLogging {
  private val updateBuffer = new RowUpdateBuffer
  private val initEvents = Vector.newBuilder[ProducerKeyEvent]

  private var indexes = Map.empty[Path, IndexableValue]
  private var metadata = Map.empty[Path, Value]
  private val data = mutable.Map.empty[Path, DataKeyDescriptor]
  private val outputStatuses = mutable.Map.empty[Path, OutputKeyDescriptor]
  private val outputEntries = mutable.ArrayBuffer.empty[ProducerOutputEntry]
  private val dynamicTables = mutable.Map.empty[String, DynamicTable]

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
    initEvents += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc))))
    handle
  }

  def latestKeyValue(key: Path, metadata: KeyMetadata = KeyMetadata()): LatestKeyValueHandle = {
    val rowId = EdgeCodecCommon.dataKeyRowId(EndpointPath(endpointId, key))
    val handle = new LatestKeyValuePublisher(rowId.tableRow, updateBuffer)
    val desc = LatestKeyValueDescriptor(metadata.indexes, metadata.metadata)
    data += (key -> desc)
    initEvents += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc))))
    handle
  }

  def topicEventValue(key: Path, metadata: KeyMetadata = KeyMetadata()): TopicEventHandle = {
    val rowId = EdgeCodecCommon.dataKeyRowId(EndpointPath(endpointId, key))
    val handle = new TopicEventPublisher(rowId.tableRow, updateBuffer)
    val desc = TopicEventValueDescriptor(metadata.indexes, metadata.metadata)
    data += (key -> desc)
    initEvents += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc))))
    handle
  }

  def activeSet(key: Path, metadata: KeyMetadata = KeyMetadata()): ActiveSetHandle = {
    val rowId = EdgeCodecCommon.dataKeyRowId(EndpointPath(endpointId, key))
    val handle = new ActiveSetPublisher(rowId.tableRow, updateBuffer)
    val desc = ActiveSetValueDescriptor(metadata.indexes, metadata.metadata)
    data += (key -> desc)
    initEvents += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeDataKeyDescriptor(desc))))
    handle
  }

  def outputStatus(key: Path, metadata: KeyMetadata = KeyMetadata()): OutputStatusHandle = {
    val rowId = EdgeCodecCommon.outputKeyRowId(EndpointPath(endpointId, key))
    val handle = new OutputStatusPublisher(rowId.tableRow, updateBuffer)
    val desc = OutputKeyDescriptor(metadata.indexes, metadata.metadata)
    outputStatuses += (key -> desc)
    initEvents += AddRow(rowId.tableRow, SequenceCtx(None, Some(EdgeCodecCommon.writeOutputKeyDescriptor(desc))))
    handle
  }

  def registerOutput(key: Path): Receiver[OutputParams, OutputResult] = {
    val rcvImpl = new RemoteBoundQueueingReceiverImpl[OutputParams, OutputResult](gatewayThread)
    outputEntries += ProducerOutputEntry(key, rcvImpl)
    rcvImpl
  }

  def seriesDynamicSet(set: String, callbacks: DynamicDataKey): DynamicSeriesHandle = {
    val table = new DynamicTableShim(callbacks)
    dynamicTables.put(set, table)

    new DynamicSeriesPublisher(endpointId, set, gateway, updateBuffer)
  }

  def keyValueDynamicSet(set: String, callbacks: DynamicDataKey): DynamicKeyValueHandle = {
    val table = new DynamicTableShim(callbacks)
    dynamicTables.put(set, table)

    new DynamicKeyValuePublisher(endpointId, set, gateway, updateBuffer)
  }

  def activeSetDynamicSet(set: String, callbacks: DynamicDataKey): DynamicActiveSetHandle = {
    val table = new DynamicTableShim(callbacks)
    dynamicTables.put(set, table)

    new DynamicActiveSetPublisher(endpointId, set, gateway, updateBuffer)
  }

  def build(): ProducerHandle = {
    val desc = EndpointDescriptor(indexes, metadata, data.toMap, outputStatuses.toMap)
    addDescriptor(desc)
    val route = EdgeCodecCommon.endpointIdToRoute(endpointId)

    val requests = registerRequests()

    val events = initEvents.result()

    gateway.handleEvent(RouteBindEvent(route, events, dynamicTables.toMap, requests))

    new EndpointProducer(route, gateway, updateBuffer)
  }

  private def addDescriptor(desc: EndpointDescriptor): Unit = {
    val row = EdgeCodecCommon.endpointIdToEndpointDescriptorTableRow(endpointId)
    initEvents += AddRow(row, SequenceCtx(None, None))
    initEvents += RowUpdate(row, AppendProducerUpdate(Seq(EdgeCodecCommon.writeEndpointDescriptor(desc))))
  }

  private def registerRequests(): Sink[RouteServiceRequest] = {
    val dist = new RemoteBoundQueuedDistributor[RouteServiceRequest](gatewayThread)

    val requestHandlers: Map[TableRow, Responder[OutputParams, OutputResult]] = {
      outputEntries.map { entry =>
        val rowKey = EdgeCodecCommon.writePath(entry.path)
        val tableRow = TableRow(EdgeTables.outputTable, rowKey)
        (tableRow, entry.responder)
      }.toMap
    }

    dist.bind { req =>
      EdgeCodecCommon.readOutputRequest(req.value) match {
        case Left(err) => req.respond(TextVal(s"Expecting edge output request protobuf: " + err))
        case Right(converted) =>
          requestHandlers.get(req.row) match {
            case None => req.respond(EdgeCodecCommon.writeOutputResult(OutputFailure(s"key not handled")))
            case Some(rcv) =>
              rcv.handle(converted, result => req.respond(EdgeCodecCommon.writeOutputResult(result)))
          }
      }
    }

    dist
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
