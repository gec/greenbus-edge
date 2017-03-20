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
package io.greenbus.edge.api

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.CallMarshaller
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.gateway.MapDiff
import io.greenbus.edge.flow._

import scala.collection.mutable

class ConsumerClientImpl {

}

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
case class Appended(values: Seq[TypeValue]) extends ValueUpdate
case class SetUpdated(value: Set[TypeValue], removed: Set[TypeValue], added: Set[TypeValue]) extends ValueUpdate
case class KeyedSetUpdated(value: Map[TypeValue, TypeValue], removed: Set[TypeValue], added: Set[(TypeValue, TypeValue)], modified: Set[(TypeValue, TypeValue)]) extends ValueUpdate
case object Absent extends ValueUpdate
case object Unresolved extends ValueUpdate

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

trait RowSubMgr {

  def addObserver()
  def removeObserver()

  def update(update: ValueUpdate): Unit
  def routeUnresolved()

}

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
            RowUpdate(row, Unresolved)
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
            RowUpdate(tableRow.toRowId(route), Unresolved)
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

sealed trait SeriesValue
case class BoolValue(value: Boolean) extends SeriesValue
case class LongValue(value: Long) extends SeriesValue
case class DoubleValue(value: Double) extends SeriesValue

sealed trait EdgeDataKeyValue
case class KeyValueUpdate(value: Value) extends EdgeDataKeyValue
case class SeriesUpdate(value: SeriesValue, time: Long) extends EdgeDataKeyValue
case class TopicEventUpdate(topic: Path, value: Value, time: Long) extends EdgeDataKeyValue
case class ActiveSetUpdate(value: Map[IndexableValue, Value]) extends EdgeDataKeyValue

trait EdgeDataKeyValueCodec {
  def fromTypeValue(v: TypeValue): Either[String, EdgeDataKeyValue]
  def dataKeyToRow(endpointPath: EndpointPath): RowId
}
trait EdgeCodec {
  def endpointIdToRow(id: EndpointId): RowId

  def fromTypeValue(v: TypeValue): Either[String, EndpointDescriptor]
}

object EdgeCodecCommon {

  def endpointIdToRow(id: EndpointId): RowId = ???

  def fromTypeValue(v: TypeValue): Either[String, EndpointDescriptor] = {
    ???
  }
}

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

  private var state: EdgeDataState[Data] = Pending

  protected def current(): EdgeDataState[Data] = state

  def toUpdate(v: EdgeDataState[Data]): IdentifiedEdgeUpdate
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

  def handle(update: ValueUpdate): Set[EdgeUpdateQueue] = {
    update match {
      case Appended(values) => {
        values.lastOption.map { last =>
          fromTypeValue(last) match {
            case Left(str) =>
              logger.warn(s"Could not extract endpoint descriptor for $logId: $str")
              Set.empty[EdgeUpdateQueue]
            case Right(desc) =>
              handleValue(desc)
          }
        }.getOrElse(Set())
      }
      case _ => Set()
    }
  }
}

class EndpointDescSubMgr(id: EndpointId) extends GenEdgeTypeSubMgr {

  protected type Data = EndpointDescriptor

  protected def logId: String = id.toString

  def toUpdate(v: EdgeDataState[EndpointDescriptor]): IdentifiedEdgeUpdate = IdEndpointUpdate(id, v)

  def fromTypeValue(v: TypeValue): Either[String, EndpointDescriptor] = {
    EdgeCodecCommon.fromTypeValue(v)
  }
}

class DataKeySubMgr(id: EndpointPath, codec: EdgeDataKeyValueCodec) extends GenEdgeTypeSubMgr {

  protected type Data = EdgeDataKeyValue

  protected def logId: String = id.toString

  def toUpdate(v: EdgeDataState[EdgeDataKeyValue]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  def fromTypeValue(v: TypeValue): Either[String, EdgeDataKeyValue] = {
    codec.fromTypeValue(v)
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

class EdgeSubscriptionManager(eventThread: CallMarshaller, subImpl: ColsetSubscriptionManager, mainCodec: EdgeCodec) {

  private val rowMap = mutable.Map.empty[RowId, EdgeTypeSubMgr]

  def subscribe(endpoints: Seq[EndpointId], keys: Seq[(EndpointPath, EdgeDataKeyValueCodec)]): EdgeSubscription = {

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
          val mgr = rowMap.getOrElseUpdate(row, new DataKeySubMgr(id, codec))
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

  def handleBatch(updates: Seq[RowUpdate]): Unit = {
    val dirty = Set.newBuilder[EdgeUpdateQueue]

    updates.foreach { update =>
      rowMap.get(update.row).foreach { row =>
        dirty ++= row.handle(update.update)
      }
    }

    dirty.result().foreach(_.flush())
  }
}

