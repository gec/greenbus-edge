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
import io.greenbus.edge.api.stream.AppendDataKeyCodec.{ LatestKeyValueCodec, SeriesCodec, TopicEventCodec }
import io.greenbus.edge.api.stream.KeyedSetDataKeyCodec.ActiveSetCodec
import io.greenbus.edge.api.stream.SetCodec.EndpointIdSetCodec
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.subscribe._
import io.greenbus.edge.flow._
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

trait EdgeUpdateQueue {
  def enqueue(update: IdentifiedEdgeUpdate): Unit
  def flush(): Unit
}

/*trait EdgeUpdateSubjectImpl extends EdgeTypeSubMgr {
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
}*/

trait ObservedEdgeTypeSubMgr extends EdgeTypeSubMgr {

  //protected def getCurrent(): Option[IdentifiedEdgeUpdate]

  private var observerSet = Set.empty[EdgeUpdateQueue]
  def observers: Set[EdgeUpdateQueue] = observerSet
  def addObserver(buffer: EdgeUpdateQueue): Unit = {
    observerSet += buffer
  }
  def removeObserver(buffer: EdgeUpdateQueue): Unit = {
    observerSet -= buffer
  }
}

trait EdgeTypeSubMgr {
  def codec: EdgeSubCodec
  def handle(update: ValueUpdate): Set[EdgeUpdateQueue]
  def observers: Set[EdgeUpdateQueue]
  def addObserver(buffer: EdgeUpdateQueue): Unit
  def removeObserver(buffer: EdgeUpdateQueue): Unit
}

trait EdgeSubCodec {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate
  //def updateFor(v: DataValueUpdate, metaOpt: Option[TypeValue]): (Seq[IdentifiedEdgeUpdate], Option[IdentifiedEdgeUpdate])

  def updateFor(v: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate]
}

class GenEdgeTypeSubMgrComp(logId: String, val codec: EdgeSubCodec) extends ObservedEdgeTypeSubMgr with LazyLogging {

  //private var last = Option.empty[IdentifiedEdgeUpdate]

  /*protected def getCurrent(): Option[IdentifiedEdgeUpdate] = {
    last
  }*/

  def handle(update: ValueUpdate): Set[EdgeUpdateQueue] = {
    update match {
      case vs: ValueSync =>
        val updates = codec.updateFor(vs.initial, vs.metadata)
        /*nextLastOpt.foreach { next =>
          last = Some(next)
        }*/
        if (updates.nonEmpty) {
          observers.foreach(obs => updates.foreach(up => obs.enqueue(up)))
          observers
        } else {
          Set()
        }
      case vd: ValueDelta =>
        val updates = codec.updateFor(vd.update, None)
        /*nextLastOpt.foreach { next =>
          last = Some(next)
        }*/
        if (updates.nonEmpty) {
          observers.foreach(obs => updates.foreach(up => obs.enqueue(up)))
          observers
        } else {
          Set()
        }
      case ValueAbsent =>
        val up = codec.simpleToUpdate(ResolvedAbsent)
        //last = Some(up)
        observers.foreach(_.enqueue(up))
        observers
      case ValueUnresolved =>
        val up = codec.simpleToUpdate(DataUnresolved)
        //last = Some(up)
        observers.foreach(_.enqueue(up))
        observers
      case ValueDisconnected =>
        val up = codec.simpleToUpdate(Disconnected)
        //last = Some(up)
        observers.foreach(_.enqueue(up))
        observers
      case _ => Set()
    }
  }
}

class AppendDataKeySubCodec(logId: String, id: EndpointPath, codec: AppendDataKeyCodec) extends EdgeSubCodec with LazyLogging {

  //private var descOpt = Option.empty[DataKeyDescriptor]

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  /*def current(): Option[IdentifiedEdgeUpdate] = {

  }*/

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {

    dataValueUpdate match {
      case up: Appended => {
        val readValues: Seq[SequenceDataKeyValueUpdate] = up.values.flatMap { ap =>
          codec.fromTypeValue(ap.value) match {
            case Left(str) =>
              logger.warn(s"Could not extract data value for $logId: $str")
              None
            case Right(value) =>
              Some(value)
          }
        }

        val descUpdateOpt = metaOpt.flatMap { tv =>
          EdgeCodecCommon.readDataKeyDescriptor(tv) match {
            case Left(str) =>
              logger.warn(s"Could not extract descriptor for $logId: $str")
              None
            case Right(value) => Some(value)
          }
        }

        /*descUpdateOpt.foreach { desc =>
          descOpt = Some(desc)
        }*/

        if (readValues.nonEmpty) {
          val head = readValues.head
          val headUp = IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(descUpdateOpt, head)))
          Seq(headUp) ++ readValues.tail.map(v => IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(None, v))))
        } else {
          Seq()
        }
      }
      case _ =>
        Seq()
    }
  }
}

class EndpointDescSubCodec(logId: String, id: EndpointId) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdEndpointUpdate(id, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {

    dataValueUpdate match {
      case up: Appended => {
        val descOpt = up.values.lastOption.flatMap { av =>
          EdgeCodecCommon.readEndpointDescriptor(av.value) match {
            case Left(str) =>
              logger.warn(s"Could not extract data value for $logId: $str")
              None
            case Right(value) =>
              Some(value)
          }

        }

        descOpt.map(desc => IdEndpointUpdate(id, ResolvedValue(desc)))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class MapDataKeySubCodec(logId: String, id: EndpointPath, codec: KeyedSetDataKeyCodec) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: MapUpdated => {

        val vOpt = codec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data value for $logId: $str")
            None
          case Right(value) =>
            Some(value)
        }

        val descOpt = metaOpt.flatMap { tv =>
          EdgeCodecCommon.readDataKeyDescriptor(tv) match {
            case Left(str) =>
              logger.warn(s"Could not extract descriptor for $logId: $str")
              None
            case Right(value) => Some(value)
          }
        }

        vOpt.map(v => IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(descOpt, v))))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class AppendOutputKeySubCodec(logId: String, id: EndpointPath, codec: AppendOutputKeyCodec) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdOutputKeyUpdate(id, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {

    dataValueUpdate match {
      case up: Appended => {
        val valueOpt = up.values.lastOption.flatMap { av =>
          codec.fromTypeValue(av.value) match {
            case Left(str) =>
              logger.warn(s"Could not extract data value for $logId: $str")
              None
            case Right(value) =>
              Some(value)
          }
        }

        val descOpt = metaOpt.flatMap { tv =>
          EdgeCodecCommon.readOutputKeyDescriptor(tv) match {
            case Left(str) =>
              logger.warn(s"Could not extract descriptor for $logId: $str")
              None
            case Right(value) => Some(value)
          }
        }

        valueOpt.map(v => IdOutputKeyUpdate(id, ResolvedValue(OutputKeyUpdate(descOpt, v))))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class EndpointSetSubCodec(logId: String, identify: EdgeDataStatus[EndpointSetUpdate] => IdentifiedEdgeUpdate) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    identify(v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: SetUpdated => {
        val vOpt = EndpointIdSetCodec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data value for $logId: $str")
            None
          case Right(data) =>
            Some(data)
        }

        vOpt.map(v => identify(ResolvedValue(v)))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class KeySetSubCodec(logId: String, identify: EdgeDataStatus[KeySetUpdate] => IdentifiedEdgeUpdate) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    identify(v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: SetUpdated => {
        val vOpt = SetCodec.EndpointPathSetCodec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data value for $logId: $str")
            None
          case Right(data) =>
            Some(data)
        }

        vOpt.map(v => identify(ResolvedValue(v)))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

object SubscriptionManagers {

  def subEndpointDesc(id: EndpointId): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(id.toString, endpointDesc(id.toString, id))
  }
  def endpointDesc(logId: String, id: EndpointId): EdgeSubCodec = {
    new EndpointDescSubCodec(logId, id)
  }

  def subAppendDataKey(id: EndpointPath, codec: AppendDataKeyCodec): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(id.toString, appendDataKey(id, codec))
  }
  def appendDataKey(id: EndpointPath, codec: AppendDataKeyCodec): EdgeSubCodec = {
    new AppendDataKeySubCodec(id.toString, id, codec)
  }

  def subMapDataKey(id: EndpointPath, codec: KeyedSetDataKeyCodec): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(id.toString, mapDataKey(id, codec))
  }
  def mapDataKey(id: EndpointPath, codec: KeyedSetDataKeyCodec): EdgeSubCodec = {
    new MapDataKeySubCodec(id.toString, id, codec)
  }

  def subOutputStatus(id: EndpointPath, codec: AppendOutputKeyCodec): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(id.toString, outputStatus(id, codec))
  }
  def outputStatus(id: EndpointPath, codec: AppendOutputKeyCodec): EdgeSubCodec = {
    new AppendOutputKeySubCodec(id.toString, id, codec)
  }

  def subPrefixSet(prefix: Path): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(prefix.toString, prefixSet(prefix))
  }
  def prefixSet(prefix: Path): EdgeSubCodec = {

    def identify(status: EdgeDataStatus[EndpointSetUpdate]): IdentifiedEdgeUpdate = IdEndpointPrefixUpdate(prefix, status)

    new EndpointSetSubCodec(prefix.toString, identify)
  }

  def subEndpointIndexSet(spec: IndexSpecifier): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(spec.toString, endpointIndexSet(spec))
  }
  def endpointIndexSet(spec: IndexSpecifier): EdgeSubCodec = {

    def identify(status: EdgeDataStatus[EndpointSetUpdate]): IdentifiedEdgeUpdate = IdEndpointIndexUpdate(spec, status)

    new EndpointSetSubCodec(spec.toString, identify)
  }

  def subDataKeyIndexSet(spec: IndexSpecifier): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(spec.toString, dataKeyIndexSet(spec))
  }
  def dataKeyIndexSet(spec: IndexSpecifier): EdgeSubCodec = {

    def identify(status: EdgeDataStatus[KeySetUpdate]): IdentifiedEdgeUpdate = IdDataKeyIndexUpdate(spec, status)

    new KeySetSubCodec(spec.toString, identify)
  }

  def subOutputKeyIndexSet(spec: IndexSpecifier): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(spec.toString, outputKeyIndexSet(spec))
  }
  def outputKeyIndexSet(spec: IndexSpecifier): EdgeSubCodec = {

    def identify(status: EdgeDataStatus[KeySetUpdate]): IdentifiedEdgeUpdate = IdOutputKeyIndexUpdate(spec, status)

    new KeySetSubCodec(spec.toString, identify)
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

object EdgeSubscriptionManager {

  private class EdgeSubscriptionImpl(source: Source[Seq[IdentifiedEdgeUpdate]], onClose: () => Unit) extends EdgeSubscription {
    def updates: Source[Seq[IdentifiedEdgeUpdate]] = source

    def close(): Unit = onClose()
  }

}
class EdgeSubscriptionManager(eventThread: CallMarshaller, subImpl: StreamDynamicSubscriptionManager) extends EdgeSubscriptionClient with LazyLogging {

  private val keyMap = mutable.Map.empty[SubscriptionKey, EdgeTypeSubMgr]
  subImpl.source.bind(batch => handleBatch(batch))

  private def handleDataKey(updateQueue: EdgeUpdateQueue, id: EndpointPath, codec: EdgeDataKeyCodec): SubscriptionKey = {
    val row = codec.dataKeyToRow(id)
    val key = RowSubKey(row)
    val mgr = keyMap.getOrElseUpdate(key, {
      codec match {
        case c: AppendDataKeyCodec => SubscriptionManagers.subAppendDataKey(id, c)
        case c: KeyedSetDataKeyCodec => SubscriptionManagers.subMapDataKey(id, c)
      }
    })
    syncAndObserve(mgr, updateQueue, key)
    key
  }

  private def handleDataKeys(updateQueue: EdgeUpdateQueue, params: DataKeySubscriptionParams): Seq[SubscriptionKey] = {
    params.series.map(handleDataKey(updateQueue, _, SeriesCodec)) ++
      params.keyValues.map(handleDataKey(updateQueue, _, LatestKeyValueCodec)) ++
      params.topicEvent.map(handleDataKey(updateQueue, _, TopicEventCodec)) ++
      params.activeSet.map(handleDataKey(updateQueue, _, ActiveSetCodec))
  }

  private def syncAndObserve(mgr: EdgeTypeSubMgr, updateQueue: EdgeUpdateQueue, key: SubscriptionKey): Unit = {
    mgr.addObserver(updateQueue)
    logger.debug("RESYNC ATTEMPT: " + key)
    subImpl.sync(key).map { up =>
      logger.debug("RESYNC: " + up)
      val result = mgr.codec.updateFor(up.initial, up.metadata)
      logger.debug("UPDATE: " + result)
      result
    }.foreach(_.foreach(updateQueue.enqueue))
  }

  def subscribe(params: SubscriptionParams): EdgeSubscription = {

    logger.debug(s"Subscribing to $params")

    val batchQueue = new RemoteBoundQueuedDistributor[Seq[IdentifiedEdgeUpdate]](eventThread)
    val updateQueue = new EdgeUpdateQueueImpl(batchQueue)
    val closeLatch = new RemotelyAppliedLatchSource(eventThread)

    eventThread.marshal {
      val prevRowSet = keyMap.keySet.toSet

      val endpointDescKeys = params.descriptors.map { id =>
        val row = EdgeCodecCommon.endpointIdToEndpointDescriptorRow(id)
        val key = RowSubKey(row)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subEndpointDesc(id))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val dataKeyKeys = handleDataKeys(updateQueue, params.dataKeys)

      val outputKeyKeys = params.outputKeys.map { id =>
        val row = EdgeCodecCommon.outputKeyRowId(id)
        val key = RowSubKey(row)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subOutputStatus(id, AppendOutputKeyCodec))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val endpointPrefixKeys = params.indexing.endpointPrefixes.map { path =>
        val key = EdgeCodecCommon.endpointPrefixToSubKey(path)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subPrefixSet(path))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val endpointIndexKeys = params.indexing.endpointIndexes.map { spec =>
        val key = EdgeCodecCommon.endpointIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subEndpointIndexSet(spec))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val dataIndexKeys = params.indexing.dataKeyIndexes.map { spec =>
        val key = EdgeCodecCommon.dataKeyIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subDataKeyIndexSet(spec))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val outputIndexKeys = params.indexing.outputKeyIndexes.map { spec =>
        val key = EdgeCodecCommon.outputKeyIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subOutputKeyIndexSet(spec))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      if (keyMap.keySet != prevRowSet) {
        subImpl.update(keyMap.keySet.toSet)
      }

      val allKeys = endpointDescKeys ++
        dataKeyKeys ++
        outputKeyKeys ++
        endpointPrefixKeys ++
        endpointIndexKeys ++
        dataIndexKeys ++
        outputIndexKeys

      closeLatch.bind(() => eventThread.marshal { unsubscribed(updateQueue, allKeys.toSet) })

      updateQueue.flush()
    }

    // TODO: pending for all rows?
    new EdgeSubscriptionManager.EdgeSubscriptionImpl(batchQueue, () => closeLatch())
  }

  private def unsubscribed(queue: EdgeUpdateQueue, rows: Set[SubscriptionKey]): Unit = {

    logger.debug(s"Queue $queue unsubscribed $rows")

    val prevRowSet = keyMap.keySet.toSet

    rows.foreach { row =>
      keyMap.get(row).foreach { mgr =>
        mgr.removeObserver(queue)
        if (mgr.observers.isEmpty) {
          keyMap -= row
        }
      }
    }

    logger.trace(s"Prev set: " + prevRowSet)
    logger.trace(s"new key map: " + keyMap)

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

class ServiceClientImpl(client: StreamServiceClient) extends ServiceClientChannel {

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

