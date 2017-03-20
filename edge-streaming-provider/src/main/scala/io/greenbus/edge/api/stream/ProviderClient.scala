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
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.gateway._
import io.greenbus.edge.flow.{ QueuedDistributor, Sink }

import scala.collection.mutable

case class CommonMetadata(indexes: Map[Path, IndexableValue] = Map(), metadata: Map[Path, Value] = Map())

trait EndpointProviderBuilder {

  def seriesBool(): BoolSeriesHandle

  def build()
}

class EndpointProviderBuilderImpl(endpointId: EndpointId) {

  private var indexes = Map.empty[Path, IndexableValue]
  private var metadata = Map.empty[Path, Value]
  private val data = mutable.Map.empty[Path, DataKeyDescriptor]
  private val outputs = mutable.Map.empty[Path, OutputKeyDescriptor]

  private val dataDescs = mutable.ArrayBuffer.empty[ProviderDataEntry]

  def setIndexes(paramIndexes: Map[Path, IndexableValue]): Unit = {
    indexes = paramIndexes
  }
  def setMetadata(paramMetadata: Map[Path, Value]): Unit = {
    metadata = paramMetadata
  }

  def seriesBool(key: Path, metadata: CommonMetadata = CommonMetadata()): BoolSeriesHandle = {
    val desc = TimeSeriesValueDescriptor(metadata.indexes, metadata.metadata)
    val handle = new BoolSeriesQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  def seriesLong(key: Path, metadata: CommonMetadata = CommonMetadata()): LongSeriesHandle = {
    val desc = TimeSeriesValueDescriptor(metadata.indexes, metadata.metadata)
    val handle = new LongSeriesQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  def seriesDouble(key: Path, metadata: CommonMetadata = CommonMetadata()): DoubleSeriesHandle = {
    val desc = TimeSeriesValueDescriptor(metadata.indexes, metadata.metadata)
    val handle = new DoubleSeriesQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  def latestKeyValue(key: Path, metadata: CommonMetadata = CommonMetadata()): LatestKeyValueHandle = {
    val desc = LatestKeyValueDescriptor(metadata.indexes, metadata.metadata)
    val handle = new LatestKeyValueQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  def topicEventValue(key: Path, metadata: CommonMetadata = CommonMetadata()): LatestKeyValueHandle = {
    val desc = LatestKeyValueDescriptor(metadata.indexes, metadata.metadata)
    val handle = new LatestKeyValueQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  def activeSet(key: Path, metadata: CommonMetadata = CommonMetadata()): ActiveSetHandle = {
    val desc = ActiveSetValueDescriptor(metadata.indexes, metadata.metadata)
    val handle = new ActiveSetQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  def ouput() = ???

  def build(): EndpointProviderDesc = {
    val desc = EndpointDescriptor(indexes, metadata, data.toMap, outputs.toMap)
    EndpointProviderDesc(desc, dataDescs.toVector)
  }
}

trait BoolSeriesHandle extends Sink[(Boolean, Long)] {
  def update(value: Boolean, timeMs: Long): Unit
}
trait LongSeriesHandle extends Sink[(Long, Long)] {
  def update(value: Long, timeMs: Long): Unit
}
trait DoubleSeriesHandle extends Sink[(Double, Long)] {
  def update(value: Double, timeMs: Long): Unit
}
trait TopicEventHandle extends Sink[(Path, Value, Long)] {
  def update(topic: Path, value: Value, timeMs: Long): Unit
}
trait LatestKeyValueHandle extends Sink[Value] {
  def update(value: Value): Unit
}
trait ActiveSetHandle extends Sink[Map[IndexableValue, Value]] {
  def update(value: Map[IndexableValue, Value]): Unit
}

sealed trait DataValueQueue
class BoolSeriesQueue extends QueuedDistributor[(Boolean, Long)] with DataValueQueue with BoolSeriesHandle {
  def update(value: Boolean, timeMs: Long): Unit = push((value, timeMs))
}
class LongSeriesQueue extends QueuedDistributor[(Long, Long)] with DataValueQueue with LongSeriesHandle {
  def update(value: Long, timeMs: Long): Unit = push((value, timeMs))
}
class DoubleSeriesQueue extends QueuedDistributor[(Double, Long)] with DataValueQueue with DoubleSeriesHandle {
  def update(value: Double, timeMs: Long): Unit = push((value, timeMs))
}
class TopicEventQueue extends QueuedDistributor[(Path, Value, Long)] with DataValueQueue with TopicEventHandle {
  def update(topic: Path, value: Value, timeMs: Long): Unit = push((topic, value, timeMs))
}
class LatestKeyValueQueue extends QueuedDistributor[Value] with DataValueQueue with LatestKeyValueHandle {
  def update(value: Value): Unit = push(value)
}
class ActiveSetQueue extends QueuedDistributor[Map[IndexableValue, Value]] with DataValueQueue with ActiveSetHandle {
  def update(value: Map[IndexableValue, Value]): Unit = push(value)
}

case class ProviderDataEntry(path: Path, dataType: DataKeyDescriptor, distributor: DataValueQueue)

object EndpointProviderDesc {
}
case class EndpointProviderDesc(
  descriptor: EndpointDescriptor,
  data: Seq[ProviderDataEntry])

trait ProviderUserBuffer {
  def enqueue(path: Path, data: TypeValue)
}
trait ProviderHandle {
  def flush(): Unit
}

class ProviderHandleImpl(handle: RouteSourceHandle) extends ProviderHandle {

  def flush(): Unit = {
    handle.flushEvents()
  }
}

trait ProviderFactory {

  def bindEndpoint(provider: EndpointProviderDesc, seriesBuffersSize: Int, eventBuffersSize: Int): ProviderHandle
}

object ColsetProviderFactory {

  val latestKeyValueTable = "edm.lkv"
  val timeSeriesValueTable = "edm.tsv"
  val eventTopicValueTable = "edm.events"
  val activeSetValueTable = "edm.set"

  private def pathToRowKey(path: Path): TypeValue = ???

  private def bindAppend(sink: AppendEventSink, queue: DataValueQueue): Unit = {
    queue match {
      case q: BoolSeriesQueue => q.bind(obj => sink.append(ColsetCodec.encodeBoolSeries(obj)))
      case q: LongSeriesQueue => q.bind(obj => sink.append(ColsetCodec.encodeLongSeries(obj)))
      case q: DoubleSeriesQueue => q.bind(obj => sink.append(ColsetCodec.encodeDoubleSeries(obj)))
      case q: LatestKeyValueQueue => q.bind(obj => sink.append(ColsetCodec.encodeValue(obj)))
      case _ => throw new IllegalArgumentException(s"Data value type did not queue type")
    }
  }

  private def bindKeyed(sink: KeyedSetEventSink, queue: DataValueQueue): Unit = {
    queue match {
      case q: ActiveSetQueue => q.bind(obj => sink.update(ColsetCodec.encodeMap(obj)))
      case _ => throw new IllegalArgumentException(s"Data value type did not queue type")
    }
  }

}

class ColsetProviderFactory(routeSource: GatewayRouteSource) extends ProviderFactory {
  import ColsetProviderFactory._

  def bindEndpoint(provider: EndpointProviderDesc, seriesBuffersSize: Int, eventBuffersSize: Int): ProviderHandle = {
    val routeHandle = routeSource.route(SymbolVal("endpoint-id-encoded"))

    val endpointDescriptorRow = TableRow("edm.endpoint", SymbolVal("the id encoded maybe?"))
    val descSink = routeHandle.appendSetRow(endpointDescriptorRow, 1)
    descSink.append(SymbolVal("this would be the binary protobuf of the endpoint desc"))

    provider.data.foreach { entry =>
      val rowKey = pathToRowKey(entry.path)

      val (tableRow, sink) = entry.dataType match {
        case d: LatestKeyValueDescriptor =>
          val tableRow = TableRow(latestKeyValueTable, rowKey)
          val sink = routeHandle.appendSetRow(tableRow, 1)
          bindAppend(sink, entry.distributor)
          (tableRow, routeHandle.appendSetRow(tableRow, 1))
        case d: TimeSeriesValueDescriptor =>
          val tableRow = TableRow(timeSeriesValueTable, rowKey)
          val sink = routeHandle.appendSetRow(tableRow, seriesBuffersSize)
          bindAppend(sink, entry.distributor)
          (tableRow, sink)
        case d: EventTopicValueDescriptor =>
          val tableRow = TableRow(eventTopicValueTable, rowKey)
          val sink = routeHandle.appendSetRow(tableRow, eventBuffersSize)
          bindAppend(sink, entry.distributor)
          (tableRow, sink)
        case d: ActiveSetValueDescriptor =>
          val tableRow = TableRow(activeSetValueTable, rowKey)
          val sink = routeHandle.keyedSetRow(tableRow)
          bindKeyed(sink, entry.distributor)
          (tableRow, sink)
      }

      (tableRow, sink)
    }

    new ProviderHandleImpl(routeHandle)
  }
}
