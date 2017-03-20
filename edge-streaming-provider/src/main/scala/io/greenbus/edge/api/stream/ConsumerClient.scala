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

sealed trait ValueUpdate
case object ValueAbsent extends ValueUpdate
case object ValueUnresolved extends ValueUpdate
case object ValueDisconnected extends ValueUpdate
sealed trait DataValueUpdate extends ValueUpdate
case class Appended(values: Seq[TypeValue]) extends DataValueUpdate
case class SetUpdated(value: Set[TypeValue], removed: Set[TypeValue], added: Set[TypeValue]) extends DataValueUpdate
case class KeyedSetUpdated(value: Map[TypeValue, TypeValue], removed: Set[TypeValue], added: Set[(TypeValue, TypeValue)], modified: Set[(TypeValue, TypeValue)]) extends DataValueUpdate

case class RowUpdate(row: RowId, update: ValueUpdate)

object ConsumerSetFilter extends LazyLogging {
  def build(snap: SetSnapshot, prev: Option[ConsumerSetFilter]): Option[ConsumerSetFilter] = {
    snap match {
      case s: ModifiedSetSnapshot =>
        val prevValueOpt = prev.flatMap {
          case f: ModifiedSetConsumerFilter => Some(f.latest)
          case _ => None
        }
        Some(new ModifiedSetConsumerFilter(s, prevValueOpt))
      case s: ModifiedKeyedSetSnapshot =>
        val prevValueOpt = prev.flatMap {
          case f: ModifiedKeyedSetConsumerFilter => Some(f.latest)
          case _ => None
        }
        Some(new ModifiedKeyedSetConsumerFilter(s, prevValueOpt))
      case s: AppendSetSequence =>
        if (s.appends.nonEmpty) {
          Some(new AppendSetConsumerFilter(s))
        } else {
          logger.warn(s"Append set filter tried to initialize with and empty sequence")
          None
        }
    }
  }
}
trait ConsumerSetFilter {
  def delta(delta: SetDelta): Option[ValueUpdate]
  def snapshot(snapshot: SetSnapshot): Option[ValueUpdate]
}

class ModifiedSetConsumerFilter(start: ModifiedSetSnapshot, prev: Option[Set[TypeValue]]) extends ConsumerSetFilter with LazyLogging {

  private var seq: SequencedTypeValue = start.sequence
  private var current = prev.getOrElse(Set.empty[TypeValue])

  def latest: Set[TypeValue] = current

  def delta(delta: SetDelta): Option[ValueUpdate] = {
    delta match {
      case d: ModifiedSetDelta => {
        if (seq.precedes(d.sequence)) {
          val updated = (current -- d.removes) ++ d.adds
          current = updated
          seq = d.sequence
          Some(SetUpdated(updated, d.removes, d.adds))
        } else {
          None
        }
      }
      case _ =>
        logger.warn(s"Incorrect delta type in consumer filter: " + delta)
        None
    }
  }

  def snapshot(snapshot: SetSnapshot): Option[ValueUpdate] = {
    snapshot match {
      case d: ModifiedSetSnapshot => {
        if (seq.isLessThan(d.sequence).contains(true)) {

          val added = d.snapshot -- current
          val removed = current -- d.snapshot

          current = d.snapshot
          seq = d.sequence

          if (added.nonEmpty || removed.nonEmpty) {
            Some(SetUpdated(d.snapshot, removed, added))
          } else {
            None
          }
        } else {
          None
        }
      }
      case _ =>
        logger.warn(s"Incorrect snapshot type in consumer filter: " + snapshot)
        None
    }
  }
}

class ModifiedKeyedSetConsumerFilter(start: ModifiedKeyedSetSnapshot, prevOpt: Option[Map[TypeValue, TypeValue]]) extends ConsumerSetFilter with LazyLogging {

  private var seq: SequencedTypeValue = start.sequence
  private var current = prevOpt.getOrElse(Map.empty[TypeValue, TypeValue])

  def latest: Map[TypeValue, TypeValue] = current

  def delta(delta: SetDelta): Option[ValueUpdate] = {
    delta match {
      case d: ModifiedKeyedSetDelta => {
        if (seq.precedes(d.sequence)) {
          val updated = (current -- d.removes) ++ d.adds ++ d.modifies
          current = updated
          seq = d.sequence
          Some(KeyedSetUpdated(updated, d.removes, d.adds, d.modifies))
        } else {
          None
        }
      }
      case _ =>
        logger.warn(s"Incorrect delta type in consumer filter: " + delta)
        None
    }
  }

  def snapshot(snapshot: SetSnapshot): Option[ValueUpdate] = {
    snapshot match {
      case d: ModifiedKeyedSetSnapshot => {
        if (seq.isLessThan(d.sequence).contains(true)) {

          val (removed, added, modified) = MapDiff.calculate(d.snapshot, current)

          current = d.snapshot
          seq = d.sequence

          if (added.nonEmpty || removed.nonEmpty || modified.nonEmpty) {
            Some(KeyedSetUpdated(d.snapshot, removed, added, modified))
          } else {
            None
          }
        } else {
          None
        }
      }
      case _ =>
        logger.warn(s"Incorrect snapshot type in consumer filter: " + snapshot)
        None
    }
  }
}

class AppendSetConsumerFilter(start: AppendSetSequence) extends ConsumerSetFilter with LazyLogging {

  private var seq: SequencedTypeValue = start.appends.last.sequence

  def sequence: SequencedTypeValue = seq

  private def handleSequence(d: AppendSetSequence): Option[ValueUpdate] = {
    if (d.appends.nonEmpty) {

      val b = Vector.newBuilder[TypeValue]
      d.appends.foreach { app =>
        if (seq.precedes(app.sequence)) {
          b += app.value
          seq = app.sequence
        }
      }
      val filtered = b.result()
      if (filtered.nonEmpty) {
        Some(Appended(filtered))
      } else {
        None
      }
    } else {
      None
    }
  }

  def delta(delta: SetDelta): Option[ValueUpdate] = {
    delta match {
      case d: AppendSetSequence => handleSequence(d)
      case _ =>
        logger.warn(s"Incorrect delta type in consumer filter: " + delta)
        None
    }
  }

  def snapshot(snapshot: SetSnapshot): Option[ValueUpdate] = {
    snapshot match {
      case d: AppendSetSequence => handleSequence(d)
      case _ =>
        logger.warn(s"Incorrect snapshot type in consumer filter: " + snapshot)
        None
    }
  }
}

class RowFilterImpl extends RowFilter with LazyLogging {

  private var activeFilterOpt = Option.empty[ConsumerSetFilter]

  def handle(event: AppendEvent): Option[ValueUpdate] = {
    event match {
      case ev: StreamDelta => activeFilterOpt.flatMap(_.delta(ev.update))
      case ev: ResyncSnapshot => activeFilterOpt.flatMap(_.snapshot(ev.snapshot))
      case ev: ResyncSession => {
        val nextOpt = ConsumerSetFilter.build(ev.snapshot, activeFilterOpt)
        nextOpt match {
          case None =>
            logger.warn(s"Could not create consumer filter for: $ev")
            None
          case Some(next) =>
            activeFilterOpt = Some(next)
            next.snapshot(ev.snapshot)
        }
      }
    }
  }
}

/*
- Maintain a set of row subs
- Maintain a map of rows -> edm sub types
- Filter event batches to updates
- Route row updates to edm sub mgrs to cache and forward

 */

trait RowFilter {
  def handle(event: AppendEvent): Option[ValueUpdate]
}

class SubscriptionFilterMap(sink: Sink[Seq[RowUpdate]]) {

  private val map = mutable.Map.empty[TypeValue, mutable.Map[TableRow, RowFilter]]

  private def lookup(rowId: RowId): Option[RowFilter] = {
    map.get(rowId.routingKey).flatMap(_.get(rowId.tableRow))
  }

  def handle(sevs: Seq[StreamEvent]): Unit = {

    val updates: Seq[RowUpdate] = sevs.flatMap {
      case ev: RowAppendEvent => {
        lookup(ev.rowId) match {
          case None => {
            val filter = new RowFilterImpl
            val routeMap = map.getOrElseUpdate(ev.rowId.routingKey, mutable.Map.empty[TableRow, RowFilter])
            routeMap.put(ev.rowId.tableRow, filter)
            filter.handle(ev.appendEvent).map(up => RowUpdate(ev.rowId, up))
          }
          case Some(filter) =>
            filter.handle(ev.appendEvent).map(up => RowUpdate(ev.rowId, up))
        }
      }
      case ev: RouteUnresolved => {
        map.get(ev.routingKey).map { rowMap =>
          rowMap.keys.map { tr =>
            val row = tr.toRowId(ev.routingKey)
            RowUpdate(row, ValueUnresolved)
          }.toVector
        }.getOrElse(Seq())
      }
    }

    sink.push(updates)
  }

  def removeRows(removes: Set[RowId]): Unit = {
    removes.groupBy(_.routingKey).foreach {
      case (route, rows) =>
        map.get(route).foreach { trMap =>
          rows.foreach(row => trMap.remove(row.tableRow))
          if (trMap.isEmpty) {
            map.remove(route)
          }
        }
    }
  }

  def handleDisconnected(): Unit = {
    val updates = map.flatMap {
      case (route, rowMap) =>
        rowMap.map {
          case (tableRow, _) =>
            RowUpdate(tableRow.toRowId(route), ValueDisconnected)
        }
    }
    sink.push(updates.toVector)
  }
}

class SubscriptionManager(eventThread: CallMarshaller) extends ColsetSubscriptionManager {

  private val dist = new QueuedDistributor[Seq[RowUpdate]]
  private val filters = new SubscriptionFilterMap(dist)
  private var subscriptionSet = Set.empty[RowId]

  private var connectionOpt = Option.empty[PeerLinkProxy]

  // TODO: PeerLinkProxyChannel needs to have cross thread marshalling
  def connected(proxy: PeerLinkProxyChannel): Unit = {
    eventThread.marshal {
      connectionOpt = Some(proxy)
      proxy.subscriptions.push(subscriptionSet)
    }
    proxy.onClose.subscribe(() => eventThread.marshal { disconnected() })
    proxy.events.bind(events => eventThread.marshal { filters.handle(events) })
  }

  private def disconnected(): Unit = {
    connectionOpt = None
    filters.handleDisconnected()
  }

  def update(set: Set[RowId]): Unit = {
    val removes = subscriptionSet -- set
    connectionOpt.foreach(_.subscriptions.push(set))
    subscriptionSet = set
    filters.removeRows(removes)
  }

  def source: Source[Seq[RowUpdate]] = dist
}

trait ColsetSubscriptionManager {
  def update(set: Set[RowId])
  def source: Source[Seq[RowUpdate]]
}

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

