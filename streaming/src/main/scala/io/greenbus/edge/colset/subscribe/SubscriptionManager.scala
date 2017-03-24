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
package io.greenbus.edge.colset.subscribe

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.gateway.MapDiff
import io.greenbus.edge.flow._
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable

sealed trait ValueUpdate
case object ValueAbsent extends ValueUpdate
case object ValueUnresolved extends ValueUpdate
case object ValueDisconnected extends ValueUpdate
sealed trait DataValueUpdate extends ValueUpdate
case class Appended(session: PeerSessionId, values: Seq[AppendSetValue]) extends DataValueUpdate
case class SetUpdated(session: PeerSessionId, sequence: SequencedTypeValue, value: Set[TypeValue], removed: Set[TypeValue], added: Set[TypeValue]) extends DataValueUpdate
case class KeyedSetUpdated(session: PeerSessionId, sequence: SequencedTypeValue, value: Map[TypeValue, TypeValue], removed: Set[TypeValue], added: Set[(TypeValue, TypeValue)], modified: Set[(TypeValue, TypeValue)]) extends DataValueUpdate

case class RowUpdate(row: RowId, update: ValueUpdate)

object ConsumerSetFilter extends LazyLogging {
  def build(session: PeerSessionId, snap: SetSnapshot, prev: Option[ConsumerSetFilter]): Option[(ConsumerSetFilter, DataValueUpdate)] = {
    snap match {
      case s: ModifiedSetSnapshot =>
        val prevValueOpt = prev.flatMap {
          case f: ModifiedSetConsumerFilter => Some(f.latest)
          case _ => None
        }
        val filter = new ModifiedSetConsumerFilter(session, s, prevValueOpt)
        Some((filter, filter.first))
      case s: ModifiedKeyedSetSnapshot =>
        val prevValueOpt = prev.flatMap {
          case f: ModifiedKeyedSetConsumerFilter => Some(f.latest)
          case _ => None
        }
        val filter = new ModifiedKeyedSetConsumerFilter(session, s, prevValueOpt)
        Some((filter, filter.first))
      case s: AppendSetSequence =>
        if (s.appends.nonEmpty) {
          val filter = new AppendSetConsumerFilter(session, s)
          Some((filter, filter.first))
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

class ModifiedSetConsumerFilter(session: PeerSessionId, start: ModifiedSetSnapshot, prevOpt: Option[Set[TypeValue]]) extends ConsumerSetFilter with LazyLogging {

  private var seq: SequencedTypeValue = start.sequence
  private var current = start.snapshot

  def latest: Set[TypeValue] = current

  def first: DataValueUpdate = {
    prevOpt match {
      case None => SetUpdated(session, start.sequence, start.snapshot, Set(), start.snapshot)
      case Some(prev) =>
        val removed = prev -- start.snapshot
        val added = start.snapshot -- prev
        SetUpdated(session, start.sequence, start.snapshot, removed, added)
    }
  }

  def delta(delta: SetDelta): Option[ValueUpdate] = {
    delta match {
      case d: ModifiedSetDelta => {
        if (seq.precedes(d.sequence)) {
          val updated = (current -- d.removes) ++ d.adds
          current = updated
          seq = d.sequence
          Some(SetUpdated(session, d.sequence, updated, d.removes, d.adds))
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
            Some(SetUpdated(session, d.sequence, d.snapshot, removed, added))
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

class ModifiedKeyedSetConsumerFilter(session: PeerSessionId, start: ModifiedKeyedSetSnapshot, prevOpt: Option[Map[TypeValue, TypeValue]]) extends ConsumerSetFilter with LazyLogging {

  private var seq: SequencedTypeValue = start.sequence
  private var current = start.snapshot

  def latest: Map[TypeValue, TypeValue] = current

  def first: DataValueUpdate = {
    prevOpt match {
      case None => KeyedSetUpdated(session, start.sequence, start.snapshot, Set(), start.snapshot.toVector.toSet, Set())
      case Some(prev) =>
        val (removed, added, modified) = MapDiff.calculate(start.snapshot, prev)
        KeyedSetUpdated(session, start.sequence, start.snapshot, removed, added, modified)
    }
  }

  def delta(delta: SetDelta): Option[ValueUpdate] = {
    delta match {
      case d: ModifiedKeyedSetDelta => {
        if (seq.precedes(d.sequence)) {
          val updated = (current -- d.removes) ++ d.adds ++ d.modifies
          current = updated
          seq = d.sequence
          Some(KeyedSetUpdated(session, d.sequence, updated, d.removes, d.adds, d.modifies))
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
            Some(KeyedSetUpdated(session, d.sequence, d.snapshot, removed, added, modified))
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

class AppendSetConsumerFilter(session: PeerSessionId, start: AppendSetSequence) extends ConsumerSetFilter with LazyLogging {

  private var seq: SequencedTypeValue = start.appends.last.sequence

  def sequence: SequencedTypeValue = seq

  def first: DataValueUpdate = {
    Appended(session, start.appends)
  }

  private def handleSequence(d: AppendSetSequence): Option[ValueUpdate] = {
    if (d.appends.nonEmpty) {

      val b = Vector.newBuilder[AppendSetValue]
      d.appends.foreach { app =>
        if (seq.precedes(app.sequence)) {
          b += app
          seq = app.sequence
        }
      }
      val filtered = b.result()
      if (filtered.nonEmpty) {
        Some(Appended(session, filtered))
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
        val nextOpt = ConsumerSetFilter.build(ev.sessionId, ev.snapshot, activeFilterOpt)
        nextOpt match {
          case None =>
            logger.warn(s"Could not create consumer filter for: $ev")
            None
          case Some((next, update)) =>
            activeFilterOpt = Some(next)
            Some(update)
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

class SubscriptionFilterMap(sink: Sink[Seq[RowUpdate]]) extends LazyLogging {

  private val map = mutable.Map.empty[TypeValue, mutable.Map[TableRow, RowFilter]]

  private def lookup(rowId: RowId): Option[RowFilter] = {
    map.get(rowId.routingKey).flatMap(_.get(rowId.tableRow))
  }

  def handle(sevs: Seq[StreamEvent]): Unit = {
    logger.debug(s"Filter map handling: " + sevs)

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

    logger.debug(s"Passed filter: " + updates)
    if (updates.nonEmpty) {
      sink.push(updates)
    }
  }

  def notifyActiveRowSet(set: Set[RowId]): Unit = {
    val perRoute = set.groupBy(_.routingKey)

    val removedRoutes = map.keySet -- perRoute.keySet
    removedRoutes.foreach(map.remove)

    perRoute.foreach {
      case (route, rowIds) =>
        map.get(route).foreach { tableRowMap =>
          val trSet = rowIds.map(_.tableRow)
          val trRemoves = tableRowMap.keySet -- trSet
          trRemoves.foreach(tableRowMap.remove)
        }
    }
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

class SubscriptionManager(eventThread: CallMarshaller) extends StreamSubscriptionManager {

  private val dist = new QueuedDistributor[Seq[RowUpdate]]
  private val filters = new SubscriptionFilterMap(dist)
  private var subscriptionSet = Set.empty[RowId]

  private var connectionOpt = Option.empty[PeerLinkProxy]

  // TODO: PeerLinkProxyChannel needs to have cross thread marshalling
  def connected(proxy: PeerLinkProxyChannel): Unit = {
    eventThread.marshal {
      connectionOpt = Some(proxy)
      if (subscriptionSet.nonEmpty) {
        proxy.subscriptions.push(subscriptionSet)
      }
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

trait StreamSubscriptionManager {
  def update(set: Set[RowId])
  def source: Source[Seq[RowUpdate]]
}

trait StreamDynamicSubscriptionManager {
  def update(set: Set[SubscriptionKey]): Unit
  def source: Source[Seq[KeyedUpdate]]
}

class DynamicSubscriptionManager(eventThread: CallMarshaller) extends StreamDynamicSubscriptionManager {

  private val dist = new RemoteBoundQueuedDistributor[Seq[RowUpdate]](eventThread)
  private val filters = new SubscriptionFilterMap(dist)
  private var registeredKeys = Set.empty[SubscriptionKey]
  private var subscribedRows = Set.empty[RowId]

  private var keyRowMap = KeyRowMapping.empty

  private var connectionOpt = Option.empty[(PeerSessionId, PeerLinkProxy)]

  def connected(peerSessionId: PeerSessionId, proxy: PeerLinkProxyChannel): Unit = {
    eventThread.marshal {
      connectionOpt = Some((peerSessionId, proxy))
      computeAndUpdateSub(peerSessionId, proxy)
    }
    proxy.onClose.subscribe(() => eventThread.marshal { disconnected() })
    proxy.events.bind(events => eventThread.marshal { filters.handle(events) })
  }

  private def disconnected(): Unit = {
    connectionOpt = None
    filters.handleDisconnected()
    subscribedRows = Set()
  }

  private def computeAndUpdateSub(session: PeerSessionId, proxy: PeerLinkProxy): Unit = {
    if (registeredKeys != keyRowMap.keys) {
      keyRowMap = KeyRowMapping.build(rowMappings(session, registeredKeys).toVector)
    }

    val activeRows = keyRowMap.rows
    filters.notifyActiveRowSet(keyRowMap.rows)

    if (subscribedRows != activeRows) {
      proxy.subscriptions.push(activeRows)
      subscribedRows = activeRows
    }
  }

  def update(set: Set[SubscriptionKey]): Unit = {
    eventThread.marshal {
      registeredKeys = set
      connectionOpt.foreach {
        case (sess, proxy) => computeAndUpdateSub(sess, proxy)
      }
    }
  }

  private def computeRowSet(session: PeerSessionId, set: Set[SubscriptionKey]): Set[RowId] = {
    set.map {
      case RowSubKey(row) => row
      case PeerBasedSubKey(f) => f(session)
    }
  }

  private def rowMappings(session: PeerSessionId, set: Set[SubscriptionKey]): Map[SubscriptionKey, RowId] = {
    set.map {
      case k @ RowSubKey(row) => k -> row
      case k @ PeerBasedSubKey(f) => k -> f(session)
    }.toMap
  }

  def source: Source[Seq[KeyedUpdate]] = new Source[Seq[KeyedUpdate]] {
    def bind(handler: Handler[Seq[KeyedUpdate]]) = {
      dist.bind(new Handler[Seq[RowUpdate]] {
        def handle(obj: Seq[RowUpdate]): Unit = {
          val updates = obj.flatMap { rowUpdate =>
            keyRowMap.rowToKeys(rowUpdate.row).map { key =>
              KeyedUpdate(key, rowUpdate.update)
            }
          }
          handler.handle(updates)
        }
      })
    }
  }
}

object KeyRowMapping {
  def build(mappings: Seq[(SubscriptionKey, RowId)]) = {
    val keyToRow = mutable.Map.empty[SubscriptionKey, RowId]
    val rowToKeys = mutable.Map.empty[RowId, mutable.Set[SubscriptionKey]]

    mappings.foreach {
      case (key, row) =>
        keyToRow += ((key, row))
        val set = rowToKeys.getOrElseUpdate(row, mutable.Set.empty[SubscriptionKey])
        set += key
    }

    KeyRowMapping(keyToRow.toMap, rowToKeys.mapValues(_.toVector).toMap)
  }

  def empty: KeyRowMapping = {
    KeyRowMapping(Map(), Map())
  }
}
case class KeyRowMapping(keyToRow: Map[SubscriptionKey, RowId], rowToKeys: Map[RowId, Seq[SubscriptionKey]]) {
  def rows: Set[RowId] = rowToKeys.keySet.toSet
  def keys: Set[SubscriptionKey] = keyToRow.keySet.toSet
}

sealed trait SubscriptionKey
case class RowSubKey(row: RowId) extends SubscriptionKey
case class PeerBasedSubKey(rowFun: PeerSessionId => RowId) extends SubscriptionKey

case class KeyedUpdate(key: SubscriptionKey, value: ValueUpdate)