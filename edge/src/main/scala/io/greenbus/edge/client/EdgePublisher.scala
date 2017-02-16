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
package io.greenbus.edge.client

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge._
import io.greenbus.edge.channel._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.Try

trait EndpointPublisher {

  def keyValueStreams: Map[Path, Sink[Value]]
  def timeSeriesStreams: Map[Path, Sink[TimeSeriesSample]]
  def outputStreams: Map[Path, OutputInteraction]

  def flush(): Unit

  def outputReceiver: Source[UserOutputRequestBatch]

  def subscribeToUpdates(): EventChannelReceiver[EndpointPublishMessage]

  def handleOutput(outputMessage: PublisherOutputRequestMessage, asyncResult: (OutputResult, Long) => Unit): Unit

}

case class UserOutputRequest(key: Path, outputRequest: PublisherOutputParams, resultAsync: OutputResult => Unit)
case class UserOutputRequestBatch(requests: Seq[UserOutputRequest])

trait EndpointBuilder {

  def addIndexes(indexes: Map[Path, IndexableValue])
  def addMetadata(metadata: Map[Path, Value])

  //def addIndex(key: Path, value: IndexableValue): Unit
  //def addMetadata(key: Path, value: Value): Unit

  def addKeyValue(key: Path, initialValue: Value, indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]): Sink[Value]
  def addTimeSeries(key: Path, initialValue: TimeSeriesSample, indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]): Sink[TimeSeriesSample]

  def addOutput(key: Path, initialValue: PublisherOutputValueStatus, indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]): (Sink[PublisherOutputValueStatus], Source[PublisherOutputParams])

  def build(): EndpointPublisher
}

case class MetadataDesc(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value])

case class LatestKeyValueEntry(initialValue: Value, meta: MetadataDesc)
case class TimeSeriesValueEntry(initialValue: TimeSeriesSample, meta: MetadataDesc)
case class OutputEntry(initialValue: PublisherOutputValueStatus, meta: MetadataDesc)

// TODO: this was probably a waste, can go to normal data infos, don't need initial values in client publisher?
case class ClientEndpointPublisherDesc(
  indexes: Map[Path, IndexableValue],
  metadata: Map[Path, Value],
  latestKeyValueEntries: Map[Path, LatestKeyValueEntry],
  timeSeriesValueEntries: Map[Path, TimeSeriesValueEntry],
  outputEntries: Map[Path, OutputEntry])

class BatteryPublisher(b: EndpointBuilder) {
  b.addIndexes(Map(Path("index1") -> ValueString("indexed")))
  b.addMetadata(Map(Path("metadata") -> ValueString("informative")))
}

trait EdgeSession {
  def endpointBuilder(): EndpointBuilder
}

trait ClientDataStreamDb {
  def snapshot(): DataValueState
  def dequeue(): DataValueUpdate
}
trait UpdateableClientDb[A] extends ClientDataStreamDb {
  def process(obj: A): Boolean
}

class ClientTimeSeriesDb(initial: TimeSeriesSample) extends UpdateableClientDb[TimeSeriesSample] {
  private var sequence: Long = 1
  private val queue = ArrayBuffer.empty[TimeSeriesSequenced]
  private var state: TimeSeriesState = TimeSeriesState(Seq(TimeSeriesSequenced(0, initial)))

  def process(obj: TimeSeriesSample): Boolean = {
    val seq = sequence
    sequence += 1
    val delta = TimeSeriesSequenced(seq, obj)
    queue += delta
    state = TimeSeriesState(Seq(delta))
    true
  }

  def snapshot(): DataValueState = {
    state
  }

  def dequeue(): DataValueUpdate = {
    val updates = queue.toVector
    val update = TimeSeriesUpdate(updates)
    queue.clear()
    update
  }
}

class LatestSequenceDb(initial: Value) extends UpdateableClientDb[Value] {
  private var sequence: Long = 1
  private var latest: SequencedValue = SequencedValue(0, initial)

  def process(obj: Value): Boolean = {
    val seq = sequence
    sequence += 1
    latest = SequencedValue(seq, obj)
    true
  }

  def snapshot(): DataValueState = {
    latest
  }

  def dequeue(): DataValueUpdate = {
    latest
  }
}

class OutputValueStatusDb(initial: Value) extends UpdateableClientDb[Value] {
  private var sequence: Long = 1
  private var latest: PublisherOutputValueStatus = PublisherOutputValueStatus(0, Some(initial))

  def process(obj: Value): Boolean = {
    val seq = sequence
    sequence += 1
    latest = PublisherOutputValueStatus(seq, Some(obj))
    true
  }

  def snapshot(): DataValueState = {
    latest
  }

  def dequeue(): DataValueUpdate = {
    latest
  }
}

case class ClientOutputResult(statusUpdateOpt: Option[PublisherOutputValueStatus], response: ClientOutputResponse)
case class OutputInteraction(statusSink: Sink[PublisherOutputValueStatus], outputReceiver: Receiver[PublisherOutputParams, ClientOutputResult])

class EndpointPublisherImpl(
  eventThread: CallMarshaller,
  endpointId: EndpointId,
  endpointDesc: ClientEndpointPublisherDesc)
    extends EndpointPublisher with LazyLogging {

  private var dirtyDataSet = Set.empty[(Path, ClientDataStreamDb)]

  private val keyValueDbs: Map[Path, LatestSequenceDb] = {
    endpointDesc.latestKeyValueEntries.mapValues(entry => new LatestSequenceDb(entry.initialValue))
  }
  private val keyValueSinks: Map[Path, Sink[Value]] = keyValueDbs.map { case (path, db) => (path, sinkForDb(path, db)) }

  private val timeSeriesDbs: Map[Path, ClientTimeSeriesDb] = {
    endpointDesc.timeSeriesValueEntries.mapValues(entry => new ClientTimeSeriesDb(entry.initialValue))
  }
  private val timeSeriesSinks: Map[Path, Sink[TimeSeriesSample]] = timeSeriesDbs.map { case (path, db) => (path, sinkForDb(path, db)) }

  private val outs = Map.empty[Path, OutputInteraction]

  private val endpointInfo = {
    EndpointDescriptor(
      endpointDesc.indexes,
      endpointDesc.metadata,
      endpointDesc.latestKeyValueEntries.mapValues(v => LatestKeyValueDescriptor(v.meta.indexes, v.meta.metadata)) ++
        endpointDesc.timeSeriesValueEntries.mapValues(v => TimeSeriesValueDescriptor(v.meta.indexes, v.meta.metadata)),
      endpointDesc.outputEntries.mapValues(v => OutputKeyDescriptor(v.meta.indexes, v.meta.metadata)))
  }

  private val ouputReqSink = new SinkOwnedSourceJoin[UserOutputRequestBatch](eventThread)

  def outputReceiver: Source[UserOutputRequestBatch] = ouputReqSink

  private def buildSnapshot(): EndpointPublishMessage = {
    val record = EndpointDescriptorRecord(0, endpointId, endpointInfo)

    val dataSnap = keyValueDbs.mapValues(_.snapshot()) ++ timeSeriesDbs.mapValues(_.snapshot())

    val snapshot = EndpointPublishSnapshot(record, dataSnap, Map())
    EndpointPublishMessage(Some(snapshot), None, Seq(), Seq())
  }

  private val dataDistributor = new SnapshotAndUpdateDistributor[EndpointPublishMessage](eventThread, buildSnapshot)

  private def sinkForDb[A](path: Path, db: UpdateableClientDb[A]): Sink[A] = {
    new Sink[A] {
      def push(obj: A): Unit = {
        eventThread.marshal { onDataUpdate(obj, path, db) }
      }
    }
  }

  private def onDataUpdate[A](obj: A, path: Path, db: UpdateableClientDb[A]): Unit = {
    val dirty = db.process(obj)
    if (dirty) {
      dirtyDataSet += (path -> db)
    }
  }

  def flush(): Unit = {
    eventThread.marshal {
      val dataUpdates = dirtyDataSet.map {
        case (path, db) => (path, db.dequeue())
      }.toVector
      val message = EndpointPublishMessage(None, None, dataUpdates, Seq())
      dataDistributor.update(message)
    }
  }

  def handleOutput(outputMessage: PublisherOutputRequestMessage, asyncResult: (OutputResult, Long) => Unit): Unit = {
    val requests = outputMessage.requests.map { pubReq =>
      UserOutputRequest(pubReq.key, pubReq.value, asyncResult(_, pubReq.correlation))
    }
    val batch = UserOutputRequestBatch(requests)
    ouputReqSink.push(batch)
  }

  def keyValueStreams: Map[Path, Sink[Value]] = keyValueSinks

  def timeSeriesStreams: Map[Path, Sink[TimeSeriesSample]] = timeSeriesSinks

  def outputStreams: Map[Path, OutputInteraction] = outs

  def subscribeToUpdates(): EventChannelReceiver[EndpointPublishMessage] = dataDistributor.subscribe()

}
