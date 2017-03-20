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
import io.greenbus.edge.colset.gateway.MapDiff
import io.greenbus.edge.colset.subscribe._
import io.greenbus.edge.flow._
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable

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
case class IdEndpointUpdate(id: EndpointId, data: EdgeDataState[EndpointDescriptor]) extends IdentifiedEdgeUpdate
case class IdDataKeyUpdate(id: EndpointPath, data: EdgeDataState[EdgeDataKeyValue]) extends IdentifiedEdgeUpdate

/*
Pending
Unresolved
ResolvedAbsent
ResolvedValue
 */
sealed trait EdgeDataState[+A]
case object Pending extends EdgeDataState[Nothing]
case object DataUnresolved extends EdgeDataState[Nothing]
case object ResolvedAbsent extends EdgeDataState[Nothing]
case class ResolvedValue[A](value: A) extends EdgeDataState[A]
case object Disconnected extends EdgeDataState[Nothing]

trait EdgeUpdateQueue {
  def enqueue(update: IdentifiedEdgeUpdate): Unit
  def flush(): Unit
}

trait EdgeUpdateSubjectImpl extends EdgeTypeSubMgr {
  protected type Data

  private var observerSet = Set.empty[EdgeUpdateQueue]

  protected def current(): EdgeDataState[Data]

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

  protected var state: EdgeDataState[Data] = Pending

  protected def current(): EdgeDataState[Data] = state

  protected def toUpdate(v: EdgeDataState[Data]): IdentifiedEdgeUpdate

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
      case Appended(values) => {
        values.lastOption.map { last =>
          fromTypeValue(last) match {
            case Left(str) =>
              logger.warn(s"Could not extract data value for $logId: $str")
              Set.empty[EdgeUpdateQueue]
            case Right(desc) =>
              handleValue(desc)
          }
        }.getOrElse(Set())
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

  def toUpdate(v: EdgeDataState[EndpointDescriptor]): IdentifiedEdgeUpdate = IdEndpointUpdate(id, v)

  def fromTypeValue(v: TypeValue): Either[String, EndpointDescriptor] = {
    EdgeCodecCommon.endpointDescriptorFromTypeValue(v)
  }

}

// TODO: need different gen for actual series???
class AppendDataKeySubMgr(id: EndpointPath, codec: AppendDataKeyCodec) extends GenAppendSubMgr {

  protected type Data = EdgeSequenceDataKeyValue

  protected def logId: String = id.toString

  def toUpdate(v: EdgeDataState[EdgeSequenceDataKeyValue]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  def fromTypeValue(v: TypeValue): Either[String, EdgeSequenceDataKeyValue] = {
    codec.fromTypeValue(v)
  }
}

class KeyedSetSubMgr(id: EndpointPath, codec: KeyedSetDataKeyCodec) extends GenEdgeTypeSubMgr with LazyLogging {

  protected def logId: String = id.toString

  protected type Data = ActiveSetUpdate

  protected def toUpdate(v: EdgeDataState[ActiveSetUpdate]): IdentifiedEdgeUpdate = ???

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
  def subscribe(endpoints: Seq[EndpointId], keys: Seq[(EndpointPath, EdgeDataKeyCodec)]): EdgeSubscription
}

class EdgeSubscriptionManager(eventThread: CallMarshaller, subImpl: ColsetSubscriptionManager, mainCodec: EdgeCodec) extends EdgeSubscriptionClient {

  private val rowMap = mutable.Map.empty[RowId, EdgeTypeSubMgr]
  subImpl.source.bind(batch => handleBatch(batch))

  def subscribe(endpoints: Seq[EndpointId], keys: Seq[(EndpointPath, EdgeDataKeyCodec)]): EdgeSubscription = {

    val batchQueue = new RemoteBoundQueuedDistributor[Seq[IdentifiedEdgeUpdate]](eventThread)
    val updateQueue = new EdgeUpdateQueueImpl(batchQueue)
    val closeLatch = new RemotelyAppliedLatchSource(eventThread)

    eventThread.marshal {
      val prevRowSet = rowMap.keySet.toSet

      val endpointIdToRows = endpoints.map(id => (id, mainCodec.endpointIdToRow(id)))

      endpointIdToRows.foreach {
        case (id, row) =>
          val mgr = rowMap.getOrElseUpdate(row, new EndpointDescSubMgr(id))
          mgr.addObserver(updateQueue)
      }

      val dataKeyAndRows = keys.map {
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
    val dirty = Set.newBuilder[EdgeUpdateQueue]

    updates.foreach { update =>
      rowMap.get(update.row).foreach { row =>
        dirty ++= row.handle(update.update)
      }
    }

    dirty.result().foreach(_.flush())
  }
}

