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
import io.greenbus.edge.flow.{ QueuedDistributor, Sink, Source }

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

  def topicEventValue(key: Path, metadata: CommonMetadata = CommonMetadata()): TopicEventHandle = {
    val desc = EventTopicValueDescriptor(metadata.indexes, metadata.metadata)
    val handle = new TopicEventQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  def activeSet(key: Path, metadata: CommonMetadata = CommonMetadata()): ActiveSetHandle = {
    val desc = ActiveSetValueDescriptor(metadata.indexes, metadata.metadata)
    val handle = new ActiveSetQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  def outputStatus(key: Path, metadata: CommonMetadata = CommonMetadata()): OutputStatusHandle = {
    val desc = OutputKeyDescriptor(metadata.indexes, metadata.metadata)
    val handle = new OutputKeyStatusQueue
    dataDescs += ProviderDataEntry(key, desc, handle)
    handle
  }

  //def ouput() = ???

  def build(): EndpointProviderDesc = {
    val desc = EndpointDescriptor(indexes, metadata, data.toMap, outputs.toMap)
    EndpointProviderDesc(endpointId, desc, dataDescs.toVector)
  }
}

trait BoolSeriesHandle {
  def update(value: Boolean, timeMs: Long): Unit
}
trait LongSeriesHandle {
  def update(value: Long, timeMs: Long): Unit
}
trait DoubleSeriesHandle {
  def update(value: Double, timeMs: Long): Unit
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
trait OutputStatusHandle {
  def update(value: OutputKeyStatus): Unit
}

trait DataValueDistributor[A] {
  protected val queue = new QueuedDistributor[A]
  def source: Source[A] = queue
}

sealed trait DataValueQueue
sealed trait SampleValueQueue extends DataValueQueue with DataValueDistributor[(SampleValue, Long)]
class BoolSeriesQueue extends SampleValueQueue with BoolSeriesHandle {
  def update(value: Boolean, timeMs: Long): Unit = queue.push((ValueBool(value), timeMs))
}
class LongSeriesQueue extends SampleValueQueue with LongSeriesHandle {
  def update(value: Long, timeMs: Long): Unit = queue.push((ValueInt64(value), timeMs))
}
class DoubleSeriesQueue extends SampleValueQueue with DoubleSeriesHandle {
  def update(value: Double, timeMs: Long): Unit = queue.push((ValueDouble(value), timeMs))
}
class TopicEventQueue extends DataValueDistributor[(Path, Value, Long)] with DataValueQueue with TopicEventHandle {
  def update(topic: Path, value: Value, timeMs: Long): Unit = queue.push((topic, value, timeMs))
}
class LatestKeyValueQueue extends DataValueDistributor[Value] with DataValueQueue with LatestKeyValueHandle {
  def update(value: Value): Unit = queue.push(value)
}
class ActiveSetQueue extends DataValueDistributor[Map[IndexableValue, Value]] with DataValueQueue with ActiveSetHandle {
  def update(value: Map[IndexableValue, Value]): Unit = queue.push(value)
}
class OutputKeyStatusQueue extends DataValueDistributor[OutputKeyStatus] with DataValueQueue with OutputStatusHandle {
  def update(value: OutputKeyStatus): Unit = queue.push(value)
}

case class ProviderDataEntry(path: Path, dataType: KeyDescriptor, distributor: DataValueQueue)

case class EndpointProviderDesc(
  endpointId: EndpointId,
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

object StreamProviderFactory {

  private def bindAppend(sink: AppendEventSink, queue: DataValueQueue): Unit = {
    queue match {
      case q: SampleValueQueue => q.source.bind(obj => sink.append(EdgeCodecCommon.writeSampleValueSeries(obj)))
      case q: LatestKeyValueQueue => q.source.bind(obj => sink.append(EdgeCodecCommon.writeValue(obj)))
      case q: TopicEventQueue => q.source.bind(obj => sink.append(EdgeCodecCommon.writeTopicEvent(obj)))
      case _ => throw new IllegalArgumentException(s"Queue type did not match append stream type: $queue")
    }
  }

  private def bindKeyed(sink: KeyedSetEventSink, queue: DataValueQueue): Unit = {
    queue match {
      case q: ActiveSetQueue => q.source.bind(obj => sink.update(EdgeCodecCommon.writeMap(obj)))
      case _ => throw new IllegalArgumentException(s"Queue type did not match keyed set stream type: $queue")
    }
  }

}

class StreamProviderFactory(routeSource: GatewayRouteSource) extends ProviderFactory {
  import StreamProviderFactory._

  def bindEndpoint(provider: EndpointProviderDesc, seriesBuffersSize: Int, eventBuffersSize: Int): ProviderHandle = {
    val routeHandle = routeSource.route(EdgeCodecCommon.endpointIdToRoute(provider.endpointId))

    val endpointDescriptorRow = EdgeCodecCommon.endpointIdToEndpointDescriptorTableRow(provider.endpointId)
    val descSink = routeHandle.appendSetRow(endpointDescriptorRow, 1)
    descSink.append(EdgeCodecCommon.writeEndpointDescriptor(provider.descriptor))

    provider.data.foreach { entry =>
      val rowKey = EdgeCodecCommon.writePath(entry.path)

      val (tableRow, sink) = entry.dataType match {
        case d: LatestKeyValueDescriptor =>
          val tableRow = TableRow(EdgeTables.latestKeyValueTable, rowKey)
          val sink = routeHandle.appendSetRow(tableRow, 1)
          bindAppend(sink, entry.distributor)
          (tableRow, sink)
        case d: TimeSeriesValueDescriptor =>
          val tableRow = TableRow(EdgeTables.timeSeriesValueTable, rowKey)
          val sink = routeHandle.appendSetRow(tableRow, seriesBuffersSize)
          bindAppend(sink, entry.distributor)
          (tableRow, sink)
        case d: EventTopicValueDescriptor =>
          val tableRow = TableRow(EdgeTables.eventTopicValueTable, rowKey)
          val sink = routeHandle.appendSetRow(tableRow, eventBuffersSize)
          bindAppend(sink, entry.distributor)
          (tableRow, sink)
        case d: ActiveSetValueDescriptor =>
          val tableRow = TableRow(EdgeTables.activeSetValueTable, rowKey)
          val sink = routeHandle.keyedSetRow(tableRow)
          bindKeyed(sink, entry.distributor)
          (tableRow, sink)
        case d: OutputKeyDescriptor =>
          val tableRow = TableRow(EdgeTables.outputTable, rowKey)
          val sink = routeHandle.appendSetRow(tableRow, 1)
          bindAppend(sink, entry.distributor)
          (tableRow, sink)
      }

      (tableRow, sink)
    }

    new ProviderHandleImpl(routeHandle)
  }
}
