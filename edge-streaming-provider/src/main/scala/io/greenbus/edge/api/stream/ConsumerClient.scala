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

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.subscribe._
import io.greenbus.edge.flow._
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

/*

info sub,
data sub,
output sub

key sub with or without desc

 */

trait EndpointDescSink extends Sink[EndpointDescriptor]

case class EndpointDescSub(endpointId: EndpointId)

class EndpointSubscription(endpointId: EndpointId, descOpt: Option[EndpointDescSub], dataKeys: Set[Path], outputKeys: Path)

sealed trait EdgeDataKeyValue
sealed trait EdgeSequenceDataKeyValue extends EdgeDataKeyValue
case class KeyValueUpdate(value: Value) extends EdgeSequenceDataKeyValue
case class SeriesUpdate(value: SampleValue, time: Long) extends EdgeSequenceDataKeyValue
case class TopicEventUpdate(topic: Path, value: Value, time: Long) extends EdgeSequenceDataKeyValue
case class ActiveSetUpdate(value: Map[IndexableValue, Value], removes: Set[IndexableValue], added: Set[(IndexableValue, Value)], modified: Set[(IndexableValue, Value)]) extends EdgeDataKeyValue

case class EndpointSetUpdate(set: Set[EndpointId], removes: Set[EndpointId], adds: Set[EndpointId])
case class KeySetUpdate(set: Set[EndpointPath], removes: Set[EndpointPath], adds: Set[EndpointPath])

sealed trait IdentifiedEdgeUpdate
case class IdEndpointUpdate(id: EndpointId, data: EdgeDataStatus[EndpointDescriptor]) extends IdentifiedEdgeUpdate
case class IdDataKeyUpdate(id: EndpointPath, data: EdgeDataStatus[EdgeDataKeyValue]) extends IdentifiedEdgeUpdate
case class IdOutputKeyUpdate(id: EndpointPath, data: EdgeDataStatus[OutputKeyStatus]) extends IdentifiedEdgeUpdate

case class IdEndpointPrefixUpdate(prefix: Path, data: EdgeDataStatus[EndpointSetUpdate]) extends IdentifiedEdgeUpdate
case class IdEndpointIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[EndpointSetUpdate]) extends IdentifiedEdgeUpdate
case class IdDataKeyIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[KeySetUpdate]) extends IdentifiedEdgeUpdate
case class IdOutputKeyIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[KeySetUpdate]) extends IdentifiedEdgeUpdate

/*
Pending
Unresolved
ResolvedAbsent
ResolvedValue
 */
sealed trait EdgeDataStatus[+A]
case object Pending extends EdgeDataStatus[Nothing]
case object DataUnresolved extends EdgeDataStatus[Nothing]
case object ResolvedAbsent extends EdgeDataStatus[Nothing]
case class ResolvedValue[A](value: A) extends EdgeDataStatus[A]
case object Disconnected extends EdgeDataStatus[Nothing]

trait EdgeUpdateQueue {
  def enqueue(update: IdentifiedEdgeUpdate): Unit
  def flush(): Unit
}

trait EdgeUpdateSubjectImpl extends EdgeTypeSubMgr {
  protected type Data

  private var observerSet = Set.empty[EdgeUpdateQueue]

  protected def current(): EdgeDataStatus[Data]

  def observers: Set[EdgeUpdateQueue] = observerSet
  def addObserver(buffer: EdgeUpdateQueue): Unit = {
    observerSet += buffer
  }
  def removeObserver(buffer: EdgeUpdateQueue): Unit = {
    observerSet -= buffer
  }
}

trait EdgeTypeSubMgr {
  def handle(update: ValueUpdate): Set[EdgeUpdateQueue]
  def observers: Set[EdgeUpdateQueue]
  def addObserver(buffer: EdgeUpdateQueue): Unit
  def removeObserver(buffer: EdgeUpdateQueue): Unit
}

trait GenEdgeTypeSubMgr extends EdgeUpdateSubjectImpl with LazyLogging {

  protected def logId: String

  protected type Data

  protected var state: EdgeDataStatus[Data] = Pending

  protected def current(): EdgeDataStatus[Data] = state

  protected def toUpdate(v: EdgeDataStatus[Data]): IdentifiedEdgeUpdate

  protected def handleData(dataUpdate: DataValueUpdate): Set[EdgeUpdateQueue]

  def handle(update: ValueUpdate): Set[EdgeUpdateQueue] = {
    update match {
      case dvu: DataValueUpdate => handleData(dvu)
      case ValueAbsent =>
        observers.foreach(_.enqueue(toUpdate(ResolvedAbsent)))
        observers
      case ValueUnresolved =>
        observers.foreach(_.enqueue(toUpdate(DataUnresolved)))
        observers
      case ValueDisconnected =>
        observers.foreach(_.enqueue(toUpdate(Disconnected)))
        observers
      case _ => Set()
    }
  }
}

trait GenAppendSubMgr extends GenEdgeTypeSubMgr with LazyLogging {

  def latest: Boolean

  def fromTypeValue(v: TypeValue): Either[String, Data]

  private def handleValue(v: Data): Set[EdgeUpdateQueue] = {
    val nextOpt = state match {
      case ResolvedValue(prev) => if (v != prev) Some(ResolvedValue(v)) else None
      case _ => Some(ResolvedValue(v))
    }

    nextOpt match {
      case None => Set()
      case Some(next) =>
        state = next
        val up = toUpdate(next)
        observers.foreach(_.enqueue(up))
        observers
    }
  }

  protected def handleData(dataUpdate: DataValueUpdate): Set[EdgeUpdateQueue] = {
    dataUpdate match {
      case ap: Appended => {
        // TODO: don't pass dirtiness awareness onto handleValue() when it can be called multiple times
        if (latest) {
          ap.values.lastOption.map { last =>
            fromTypeValue(last.value) match {
              case Left(str) =>
                logger.warn(s"Could not extract data value for $logId: $str")
                Set.empty[EdgeUpdateQueue]
              case Right(desc) =>
                handleValue(desc)
            }
          }.getOrElse(Set())
        } else {

          var dirty = false
          ap.values.foreach { append =>
            fromTypeValue(append.value) match {
              case Left(str) =>
                logger.warn(s"Could not extract data value for $logId: $str")
              case Right(desc) =>
                handleValue(desc)
                dirty = true
            }
          }

          if (dirty) {
            observers
          } else {
            Set()
          }
        }
      }
      case _ =>
        logger.warn(s"Wrong value update type for $logId: $dataUpdate")
        Set()
    }
  }
}

class EndpointDescSubMgr(id: EndpointId) extends GenAppendSubMgr {

  protected type Data = EndpointDescriptor

  protected def logId: String = id.toString

  def latest: Boolean = true

  def toUpdate(v: EdgeDataStatus[EndpointDescriptor]): IdentifiedEdgeUpdate = IdEndpointUpdate(id, v)

  def fromTypeValue(v: TypeValue): Either[String, EndpointDescriptor] = {
    EdgeCodecCommon.readEndpointDescriptor(v)
  }

}

// TODO: need different gen for actual series???
class AppendDataKeySubMgr(id: EndpointPath, codec: AppendDataKeyCodec) extends GenAppendSubMgr {

  protected type Data = EdgeSequenceDataKeyValue

  protected def logId: String = id.toString

  def latest: Boolean = codec.latest

  def toUpdate(v: EdgeDataStatus[EdgeSequenceDataKeyValue]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  def fromTypeValue(v: TypeValue): Either[String, EdgeSequenceDataKeyValue] = {
    codec.fromTypeValue(v)
  }
}

class KeyedSetSubMgr(id: EndpointPath, codec: KeyedSetDataKeyCodec) extends GenEdgeTypeSubMgr with LazyLogging {

  protected def logId: String = id.toString

  protected type Data = ActiveSetUpdate

  protected def toUpdate(v: EdgeDataStatus[ActiveSetUpdate]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  protected def handleData(dataUpdate: DataValueUpdate): Set[EdgeUpdateQueue] = {
    dataUpdate match {
      case up: KeyedSetUpdated => {
        codec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data update for $logId: $str")
            Set.empty[EdgeUpdateQueue]
          case Right(data) =>
            val updateOpt = if (data.removes.nonEmpty || data.added.nonEmpty || data.modified.nonEmpty) {
              state = ResolvedValue(ActiveSetUpdate(data.value, Set(), Set(), Set()))
              Some(ResolvedValue(data))
            } else {
              None
            }

            updateOpt match {
              case None => Set()
              case Some(update) =>
                val identified = toUpdate(update)
                observers.foreach(_.enqueue(identified))
                observers
            }
        }
      }
      case _ =>
        logger.warn(s"Wrong value update type for $logId: $dataUpdate")
        Set()
    }
  }
}

class OutputStatusSubMgr(id: EndpointPath) extends GenAppendSubMgr {

  protected type Data = OutputKeyStatus

  protected def logId: String = id.toString

  def latest: Boolean = true

  def toUpdate(v: EdgeDataStatus[OutputKeyStatus]): IdentifiedEdgeUpdate = {
    IdOutputKeyUpdate(id, v)
  }

  def fromTypeValue(v: TypeValue): Either[String, OutputKeyStatus] = {
    AppendOutputKeyCodec.fromTypeValue(v)
  }
}

trait EndpointSetUpdateSubMgr extends GenEdgeTypeSubMgr {

  protected type Data = EndpointSetUpdate

  private val codec = SetCodec.EndpointIdSetCodec

  protected def handleData(dataUpdate: DataValueUpdate): Set[EdgeUpdateQueue] = {
    dataUpdate match {
      case up: SetUpdated => {
        codec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data update for $logId: $str")
            Set.empty[EdgeUpdateQueue]
          case Right(data) =>
            val updateOpt = if (data.removes.nonEmpty || data.adds.nonEmpty) {
              state = ResolvedValue(data)
              Some(ResolvedValue(data))
            } else {
              None
            }

            updateOpt match {
              case None => Set()
              case Some(update) =>
                val identified = toUpdate(update)
                observers.foreach(_.enqueue(identified))
                observers
            }
        }
      }
      case _ =>
        logger.warn(s"Wrong value update type for $logId: $dataUpdate")
        Set()
    }
  }
}

class EndpointPrefixSubMgr(prefix: Path) extends EndpointSetUpdateSubMgr {
  protected def logId: String = prefix.toString

  protected def toUpdate(v: EdgeDataStatus[EndpointSetUpdate]): IdentifiedEdgeUpdate = {
    IdEndpointPrefixUpdate(prefix, v)
  }
}

class EndpointIndexSubMgr(specifier: IndexSpecifier) extends EndpointSetUpdateSubMgr {
  protected def logId: String = specifier.toString

  protected def toUpdate(v: EdgeDataStatus[EndpointSetUpdate]): IdentifiedEdgeUpdate = {
    IdEndpointIndexUpdate(specifier, v)
  }
}

trait KeySetUpdateSubMgr extends GenEdgeTypeSubMgr {

  protected type Data = KeySetUpdate

  private val codec = SetCodec.EndpointPathSetCodec

  protected def handleData(dataUpdate: DataValueUpdate): Set[EdgeUpdateQueue] = {
    dataUpdate match {
      case up: SetUpdated => {
        codec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data update for $logId: $str")
            Set.empty[EdgeUpdateQueue]
          case Right(data) =>
            val updateOpt = if (data.removes.nonEmpty || data.adds.nonEmpty) {
              state = ResolvedValue(data)
              Some(ResolvedValue(data))
            } else {
              None
            }

            updateOpt match {
              case None => Set()
              case Some(update) =>
                val identified = toUpdate(update)
                observers.foreach(_.enqueue(identified))
                observers
            }
        }
      }
      case _ =>
        logger.warn(s"Wrong value update type for $logId: $dataUpdate")
        Set()
    }
  }
}

class DataKeyIndexSubMgr(specifier: IndexSpecifier) extends KeySetUpdateSubMgr {
  protected def logId: String = specifier.toString

  protected def toUpdate(v: EdgeDataStatus[KeySetUpdate]): IdentifiedEdgeUpdate = {
    IdDataKeyIndexUpdate(specifier, v)
  }
}
class OutputKeyIndexSubMgr(specifier: IndexSpecifier) extends KeySetUpdateSubMgr {
  protected def logId: String = specifier.toString

  protected def toUpdate(v: EdgeDataStatus[KeySetUpdate]): IdentifiedEdgeUpdate = {
    IdOutputKeyIndexUpdate(specifier, v)
  }
}

class BatchedSink[A](batchSink: Sink[Seq[A]]) {
  private val buffer = mutable.ArrayBuffer.empty[A]

  def enqueue(update: A): Unit = {
    buffer += update
  }

  def flush(): Unit = {
    val seq = buffer.toVector
    buffer.clear()
    batchSink.push(seq)
  }
}

class EdgeUpdateQueueImpl(batchSink: Sink[Seq[IdentifiedEdgeUpdate]]) extends BatchedSink[IdentifiedEdgeUpdate](batchSink) with EdgeUpdateQueue

object EdgeSubscription {

  private class EdgeSubscriptionImpl(source: Source[Seq[IdentifiedEdgeUpdate]], onClose: () => Unit) extends EdgeSubscription {
    def updates: Source[Seq[IdentifiedEdgeUpdate]] = source

    def close(): Unit = onClose()
  }

  def apply(source: Source[Seq[IdentifiedEdgeUpdate]], onClose: () => Unit): EdgeSubscription = {
    new EdgeSubscriptionImpl(source, onClose)
  }
}
trait EdgeSubscription {
  def updates: Source[Seq[IdentifiedEdgeUpdate]]
  def close(): Unit
}

trait EdgeSubscriptionClient {
  def subscribe(params: SubscriptionParams): EdgeSubscription
  def subscribe(endpoints: Seq[EndpointId],
    keys: Seq[(EndpointPath, EdgeDataKeyCodec)],
    outputKeys: Seq[EndpointPath],
    indexing: IndexSubscriptionParams): EdgeSubscription
}

class EdgeSubscriptionManager(eventThread: CallMarshaller, subImpl: StreamDynamicSubscriptionManager) extends EdgeSubscriptionClient with LazyLogging {

  private val keyMap = mutable.Map.empty[SubscriptionKey, EdgeTypeSubMgr]
  subImpl.source.bind(batch => handleBatch(batch))

  def subscribe(params: SubscriptionParams): EdgeSubscription = {
    subscribe(params.descriptors, params.keys, params.outputKeys, params.indexing)
  }

  def subscribe(
    endpoints: Seq[EndpointId],
    dataKeys: Seq[(EndpointPath, EdgeDataKeyCodec)],
    outputKeys: Seq[EndpointPath],
    indexing: IndexSubscriptionParams): EdgeSubscription = {

    val batchQueue = new RemoteBoundQueuedDistributor[Seq[IdentifiedEdgeUpdate]](eventThread)
    val updateQueue = new EdgeUpdateQueueImpl(batchQueue)
    val closeLatch = new RemotelyAppliedLatchSource(eventThread)

    eventThread.marshal {
      val prevRowSet = keyMap.keySet.toSet

      val endpointDescKeys = endpoints.map { id =>
        val row = EdgeCodecCommon.endpointIdToEndpointDescriptorRow(id)
        val key = RowSubKey(row)
        val mgr = keyMap.getOrElseUpdate(key, new EndpointDescSubMgr(id))
        mgr.addObserver(updateQueue)
        key
      }

      val dataKeyKeys = dataKeys.map {
        case (id, codec) =>
          val row = codec.dataKeyToRow(id)
          val key = RowSubKey(row)
          val mgr = keyMap.getOrElseUpdate(key, {
            codec match {
              case c: AppendDataKeyCodec => new AppendDataKeySubMgr(id, c)
              case c: KeyedSetDataKeyCodec => new KeyedSetSubMgr(id, c)
            }
          })
          mgr.addObserver(updateQueue)
          key
      }

      val outputKeyKeys = outputKeys.map { id =>
        val row = EdgeCodecCommon.outputKeyRowId(id)
        val key = RowSubKey(row)
        val mgr = keyMap.getOrElseUpdate(key, new OutputStatusSubMgr(id))
        mgr.addObserver(updateQueue)
        key
      }

      val endpointIndexKeys = indexing.endpointIndexes.map { spec =>
        val key = EdgeCodecCommon.endpointIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, new EndpointIndexSubMgr(spec))
        mgr.addObserver(updateQueue)
        key
      }

      val dataIndexKeys = indexing.dataKeyIndexes.map { spec =>
        val key = EdgeCodecCommon.dataKeyIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, new DataKeyIndexSubMgr(spec))
        mgr.addObserver(updateQueue)
        key
      }

      val outputIndexKeys = indexing.outputKeyIndexes.map { spec =>
        val key = EdgeCodecCommon.outputKeyIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, new OutputKeyIndexSubMgr(spec))
        mgr.addObserver(updateQueue)
        key
      }

      if (keyMap.keySet != prevRowSet) {
        subImpl.update(keyMap.keySet.toSet)
      }

      val allKeys = endpointDescKeys ++ dataKeyKeys ++ outputKeyKeys ++ endpointIndexKeys ++ dataIndexKeys ++ outputIndexKeys

      closeLatch.bind(() => eventThread.marshal { unsubscribed(updateQueue, allKeys.toSet) })
    }

    // TODO: pending for all rows?
    EdgeSubscription(batchQueue, () => closeLatch())
  }

  private def unsubscribed(queue: EdgeUpdateQueue, rows: Set[SubscriptionKey]): Unit = {

    val prevRowSet = keyMap.keySet.toSet

    rows.foreach { row =>
      keyMap.get(row).foreach { mgr =>
        mgr.removeObserver(queue)
        if (mgr.observers.isEmpty) {
          keyMap -= row
        }
      }
    }

    if (keyMap.keySet != prevRowSet) {
      subImpl.update(keyMap.keySet.toSet)
    }
  }

  private def handleBatch(updates: Seq[KeyedUpdate]): Unit = {
    logger.trace(s"Handle batch: $updates")

    val dirty = Set.newBuilder[EdgeUpdateQueue]

    updates.foreach { update =>
      keyMap.get(update.key).foreach { row =>
        dirty ++= row.handle(update.value)
      }
    }

    val dirtySet = dirty.result()
    dirtySet.foreach(_.flush())
  }
}

case class IndexSubscriptionParams(
  endpointPrefixes: Seq[Path] = Seq(),
  endpointIndexes: Seq[IndexSpecifier] = Seq(),
  dataKeyIndexes: Seq[IndexSpecifier] = Seq(),
  outputKeyIndexes: Seq[IndexSpecifier] = Seq())
trait SubscriptionParams {
  def descriptors: Seq[EndpointId]
  def keys: Seq[(EndpointPath, EdgeDataKeyCodec)]
  def outputKeys: Seq[EndpointPath]
  def indexing: IndexSubscriptionParams
}

object SubscriptionBuilder {

  case class SubParams(descriptors: Seq[EndpointId], keys: Seq[(EndpointPath, EdgeDataKeyCodec)], outputKeys: Seq[EndpointPath], indexing: IndexSubscriptionParams) extends SubscriptionParams

  class SubscriptionBuilderImpl extends SubscriptionBuilder {
    private val endpointBuffer = mutable.ArrayBuffer.empty[EndpointId]
    private val keys = mutable.ArrayBuffer.empty[(EndpointPath, EdgeDataKeyCodec)]
    private val outputKeys = mutable.ArrayBuffer.empty[EndpointPath]

    private val endpointPrefixes = mutable.ArrayBuffer.empty[Path]
    private val endpointIndexes = mutable.ArrayBuffer.empty[IndexSpecifier]
    private val dataKeyIndexes = mutable.ArrayBuffer.empty[IndexSpecifier]
    private val outputKeyIndexes = mutable.ArrayBuffer.empty[IndexSpecifier]

    def endpointDescriptor(endpointId: EndpointId): SubscriptionBuilder = {
      endpointBuffer += endpointId
      this
    }
    def series(path: EndpointPath): SubscriptionBuilder = {
      keys += ((path, AppendDataKeyCodec.SeriesCodec))
      this
    }
    def keyValue(path: EndpointPath): SubscriptionBuilder = {
      keys += ((path, AppendDataKeyCodec.LatestKeyValueCodec))
      this
    }
    def topicEvent(path: EndpointPath): SubscriptionBuilder = {
      keys += ((path, AppendDataKeyCodec.TopicEventCodec))
      this
    }
    def activeSet(path: EndpointPath): SubscriptionBuilder = {
      keys += ((path, KeyedSetDataKeyCodec.ActiveSetCodec))
      this
    }
    def outputStatus(path: EndpointPath): SubscriptionBuilder = {
      outputKeys += path
      this
    }

    def endpointSet(prefix: Path): SubscriptionBuilder = {
      endpointPrefixes += prefix
      this
    }
    def endpointIndex(specifier: IndexSpecifier): SubscriptionBuilder = {
      endpointIndexes += specifier
      this
    }
    def dataKeyIndex(specifier: IndexSpecifier): SubscriptionBuilder = {
      dataKeyIndexes += specifier
      this
    }
    def outputKeyIndex(specifier: IndexSpecifier): SubscriptionBuilder = {
      outputKeyIndexes += specifier
      this
    }

    def build(): SubscriptionParams = {
      val indexing = IndexSubscriptionParams(
        endpointPrefixes.toVector,
        endpointIndexes.toVector,
        dataKeyIndexes.toVector,
        outputKeyIndexes.toVector)
      SubParams(endpointBuffer.toVector, keys.toVector, outputKeys.toVector, indexing)
    }
  }

  def newBuilder: SubscriptionBuilder = {
    new SubscriptionBuilderImpl
  }
}
trait SubscriptionBuilder {

  def endpointDescriptor(endpointId: EndpointId): SubscriptionBuilder
  def series(endpointPath: EndpointPath): SubscriptionBuilder
  def keyValue(path: EndpointPath): SubscriptionBuilder
  def topicEvent(path: EndpointPath): SubscriptionBuilder
  def activeSet(path: EndpointPath): SubscriptionBuilder
  def outputStatus(path: EndpointPath): SubscriptionBuilder

  def endpointSet(prefix: Path): SubscriptionBuilder
  def endpointIndex(specifier: IndexSpecifier): SubscriptionBuilder
  def dataKeyIndex(specifier: IndexSpecifier): SubscriptionBuilder
  def outputKeyIndex(specifier: IndexSpecifier): SubscriptionBuilder

  def build(): SubscriptionParams
}

trait ServiceClient extends Sender[OutputRequest, OutputResult]
class ServiceClientImpl(client: StreamServiceClient) extends ServiceClient with CloseObservable {

  def send(obj: OutputRequest, handleResponse: (Try[OutputResult]) => Unit): Unit = {
    val row = EdgeCodecCommon.keyRowId(obj.key, EdgeTables.outputTable)
    val value = EdgeCodecCommon.writeIndexSpecifier(obj.value)

    def handle(resp: Try[UserServiceResponse]): Unit = {
      resp.map { userResp =>
        val converted = resp.flatMap { resp =>
          EdgeCodecCommon.readOutputResult(resp.value) match {
            case Right(result) => Success(result)
            case Left(err) => Failure(new Exception(err))
          }
        }

        handleResponse(converted)
      }
    }

    client.send(UserServiceRequest(row, value), handle)
  }

  def onClose: LatchSubscribable = client.onClose
}

