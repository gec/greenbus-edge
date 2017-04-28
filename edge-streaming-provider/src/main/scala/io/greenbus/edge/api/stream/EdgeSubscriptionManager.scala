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
import io.greenbus.edge.flow._
import io.greenbus.edge.stream.subscribe._
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

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
    updateQueue.enqueue(IdDataKeyUpdate(id, Pending))
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
    subImpl.initial(key).foreach { up =>
      println("update: " + up)
      mgr.handle(up)
    }
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
        updateQueue.enqueue(IdEndpointUpdate(id, Pending))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val dataKeyKeys = handleDataKeys(updateQueue, params.dataKeys)

      val outputKeyKeys = params.outputKeys.map { id =>
        val row = EdgeCodecCommon.outputKeyRowId(id)
        val key = RowSubKey(row)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subOutputStatus(id, AppendOutputKeyCodec))
        updateQueue.enqueue(IdOutputKeyUpdate(id, Pending))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val endpointPrefixKeys = params.indexing.endpointPrefixes.map { path =>
        val key = EdgeCodecCommon.endpointPrefixToSubKey(path)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subPrefixSet(path))
        updateQueue.enqueue(IdEndpointPrefixUpdate(path, Pending))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val endpointIndexKeys = params.indexing.endpointIndexes.map { spec =>
        val key = EdgeCodecCommon.endpointIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subEndpointIndexSet(spec))
        updateQueue.enqueue(IdEndpointIndexUpdate(spec, Pending))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val dataIndexKeys = params.indexing.dataKeyIndexes.map { spec =>
        val key = EdgeCodecCommon.dataKeyIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subDataKeyIndexSet(spec))
        updateQueue.enqueue(IdDataKeyIndexUpdate(spec, Pending))
        syncAndObserve(mgr, updateQueue, key)
        key
      }

      val outputIndexKeys = params.indexing.outputKeyIndexes.map { spec =>
        val key = EdgeCodecCommon.outputKeyIndexSpecToSubKey(spec)
        val mgr = keyMap.getOrElseUpdate(key, SubscriptionManagers.subOutputKeyIndexSet(spec))
        updateQueue.enqueue(IdOutputKeyIndexUpdate(spec, Pending))
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

