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

/*sealed trait SeriesValue
case class BoolValue(value: Boolean) extends SeriesValue
case class LongValue(value: Long) extends SeriesValue
case class DoubleValue(value: Double) extends SeriesValue*/

sealed trait EdgeDataKeyValue
sealed trait EdgeSequenceDataKeyValue extends EdgeDataKeyValue
case class KeyValueUpdate(value: Value) extends EdgeSequenceDataKeyValue
case class SeriesUpdate(value: SampleValue, time: Long) extends EdgeSequenceDataKeyValue
case class TopicEventUpdate(topic: Path, value: Value, time: Long) extends EdgeSequenceDataKeyValue
case class ActiveSetUpdate(value: Map[IndexableValue, Value], removes: Set[IndexableValue], added: Set[(IndexableValue, Value)], modified: Set[(IndexableValue, Value)]) extends EdgeDataKeyValue

sealed trait IdentifiedEdgeUpdate
case class IdEndpointUpdate(id: EndpointId, data: EdgeDataStatus[EndpointDescriptor]) extends IdentifiedEdgeUpdate
case class IdDataKeyUpdate(id: EndpointPath, data: EdgeDataStatus[EdgeDataKeyValue]) extends IdentifiedEdgeUpdate
case class IdOutputKeyUpdate(id: EndpointPath, data: EdgeDataStatus[OutputKeyStatus]) extends IdentifiedEdgeUpdate

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
  def subscribe(endpoints: Seq[EndpointId], keys: Seq[(EndpointPath, EdgeDataKeyCodec)], outputKeys: Seq[EndpointPath]): EdgeSubscription
}

class EdgeSubscriptionManager(eventThread: CallMarshaller, subImpl: StreamSubscriptionManager) extends EdgeSubscriptionClient with LazyLogging {

  private val rowMap = mutable.Map.empty[RowId, EdgeTypeSubMgr]
  subImpl.source.bind(batch => handleBatch(batch))

  def subscribe(params: SubscriptionParams): EdgeSubscription = {
    subscribe(params.descriptors, params.keys, params.outputKeys)
  }

  def subscribe(endpoints: Seq[EndpointId], dataKeys: Seq[(EndpointPath, EdgeDataKeyCodec)], outputKeys: Seq[EndpointPath]): EdgeSubscription = {

    val batchQueue = new RemoteBoundQueuedDistributor[Seq[IdentifiedEdgeUpdate]](eventThread)
    val updateQueue = new EdgeUpdateQueueImpl(batchQueue)
    val closeLatch = new RemotelyAppliedLatchSource(eventThread)

    eventThread.marshal {
      val prevRowSet = rowMap.keySet.toSet

      val endpointIdToRows = endpoints.map(id => (id, EdgeCodecCommon.endpointIdToEndpointDescriptorRow(id)))

      endpointIdToRows.foreach {
        case (id, row) =>
          val mgr = rowMap.getOrElseUpdate(row, new EndpointDescSubMgr(id))
          mgr.addObserver(updateQueue)
      }

      val dataKeyAndRows = dataKeys.map {
        case (endPath, codec) => (endPath, codec, codec.dataKeyToRow(endPath))
      }

      dataKeyAndRows.foreach {
        case (id, codec, row) =>
          val mgr = rowMap.getOrElseUpdate(row, {
            codec match {
              case c: AppendDataKeyCodec => new AppendDataKeySubMgr(id, c)
              case c: KeyedSetDataKeyCodec => new KeyedSetSubMgr(id, c)
            }
          })
          mgr.addObserver(updateQueue)
      }

      val outputKeysAndRows = outputKeys.map {
        case endPath => (endPath, EdgeCodecCommon.outputKeyRowId(endPath))
      }

      outputKeysAndRows.foreach {
        case (id, row) =>
          val mgr = rowMap.getOrElseUpdate(row, new OutputStatusSubMgr(id))
          mgr.addObserver(updateQueue)
      }

      if (rowMap.keySet != prevRowSet) {
        subImpl.update(rowMap.keySet.toSet)
      }

      val allRows = (endpointIdToRows.map(_._2) ++ dataKeyAndRows.map(_._3)).toSet
      closeLatch.bind(() => eventThread.marshal { unsubscribed(updateQueue, allRows) })
    }

    // TODO: pending for all rows?
    EdgeSubscription(batchQueue, () => closeLatch())
  }

  private def unsubscribed(queue: EdgeUpdateQueue, rows: Set[RowId]): Unit = {

    val prevRowSet = rowMap.keySet.toSet

    rows.foreach { row =>
      rowMap.get(row).foreach { mgr =>
        mgr.removeObserver(queue)
        if (mgr.observers.isEmpty) {
          rowMap -= row
        }
      }
    }

    if (rowMap.keySet != prevRowSet) {
      subImpl.update(rowMap.keySet.toSet)
    }
  }

  private def handleBatch(updates: Seq[RowUpdate]): Unit = {
    logger.trace(s"Handle batch: $updates")

    val dirty = Set.newBuilder[EdgeUpdateQueue]

    updates.foreach { update =>
      rowMap.get(update.row).foreach { row =>
        dirty ++= row.handle(update.update)
      }
    }

    val dirtySet = dirty.result()
    logger.trace(s"Dirty update queues: $dirtySet")
    dirtySet.foreach(_.flush())
  }
}

trait SubscriptionParams {
  def descriptors: Seq[EndpointId]
  def keys: Seq[(EndpointPath, EdgeDataKeyCodec)]
  def outputKeys: Seq[EndpointPath]
}

object SubscriptionBuilder {

  case class SubParams(descriptors: Seq[EndpointId], keys: Seq[(EndpointPath, EdgeDataKeyCodec)], outputKeys: Seq[EndpointPath]) extends SubscriptionParams

  class SubscriptionBuilderImpl extends SubscriptionBuilder {
    private val endpointBuffer = mutable.ArrayBuffer.empty[EndpointId]
    private val keys = mutable.ArrayBuffer.empty[(EndpointPath, EdgeDataKeyCodec)]
    private val outputKeys = mutable.ArrayBuffer.empty[EndpointPath]

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

    def build(): SubscriptionParams = {
      SubParams(endpointBuffer.toVector, keys.toVector, outputKeys.toVector)
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

  def build(): SubscriptionParams
}

trait ServiceClient extends Sender[OutputRequest, OutputResult]
class ServiceClientImpl(client: StreamServiceClient) extends ServiceClient with CloseObservable {

  def send(obj: OutputRequest, handleResponse: (Try[OutputResult]) => Unit): Unit = {
    val row = EdgeCodecCommon.keyRowId(obj.key, EdgeTables.outputTable)
    val value = EdgeCodecCommon.writeOutputRequest(obj.value)

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

