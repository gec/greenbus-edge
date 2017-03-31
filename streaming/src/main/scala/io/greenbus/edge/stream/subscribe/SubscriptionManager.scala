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
package io.greenbus.edge.stream.subscribe

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.gateway.MapDiffCalc
import io.greenbus.edge.flow._
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable

sealed trait ValueUpdate
case object ValueAbsent extends ValueUpdate
case object ValueUnresolved extends ValueUpdate
case object ValueDisconnected extends ValueUpdate
case class ValueSync(metadata: Option[TypeValue], initial: DataValueUpdate) extends ValueUpdate
case class ValueDelta(update: DataValueUpdate) extends ValueUpdate

sealed trait DataValueUpdate
case class Appended(values: Seq[AppendValue]) extends DataValueUpdate
case class SetUpdated(value: Set[TypeValue], removed: Set[TypeValue], added: Set[TypeValue]) extends DataValueUpdate
case class MapUpdated(value: Map[TypeValue, TypeValue], removed: Set[TypeValue], added: Set[(TypeValue, TypeValue)], modified: Set[(TypeValue, TypeValue)]) extends DataValueUpdate

case class RowUpdate(row: RowId, update: ValueUpdate)

trait UpdateSynthesizer {
  def delta(delta: Delta): Option[DataValueUpdate]
  def resync(resync: Resync): Option[DataValueUpdate]
  def get(): DataValueUpdate
}

class SetUpdateSynthesizer(orig: Set[TypeValue]) extends UpdateSynthesizer with LazyLogging {

  private var current: Set[TypeValue] = orig

  def get(): DataValueUpdate = {
    SetUpdated(current, Set(), Set())
  }

  def delta(delta: Delta): Option[DataValueUpdate] = {

    val start = current

    delta.diffs.foreach { seqDiff =>
      seqDiff.diff match {
        case d: SetDiff =>
          val updated = (current -- d.removes) ++ d.adds
          current = updated
        case _ =>
          logger.warn(s"Incorrect diff type in consumer filter: " + seqDiff.diff)
          None
      }
    }

    val adds = current -- start
    val removes = start -- current
    if (removes.nonEmpty || adds.nonEmpty) {
      Some(SetUpdated(current, removes, adds))
    } else {
      None
    }
  }

  def resync(resync: Resync): Option[DataValueUpdate] = {
    resync.snapshot match {
      case d: SetSnapshot => {
        val added = d.snapshot -- current
        val removed = current -- d.snapshot
        current = d.snapshot

        if (added.nonEmpty || removed.nonEmpty) {
          Some(SetUpdated(d.snapshot, removed, added))
        } else {
          None
        }
      }
      case _ =>
        logger.warn(s"Incorrect snapshot type in consumer filter: " + resync)
        None
    }
  }
}
class MapUpdateSynthesizer(orig: Map[TypeValue, TypeValue]) extends UpdateSynthesizer with LazyLogging {

  private var current: Map[TypeValue, TypeValue] = orig

  def get(): DataValueUpdate = {
    MapUpdated(current, Set(), Set(), Set())
  }

  def delta(delta: Delta): Option[DataValueUpdate] = {

    val start = current

    delta.diffs.foreach { seqDiff =>
      seqDiff.diff match {
        case d: MapDiff =>
          val updated = (current -- d.removes) ++ d.adds ++ d.modifies
          current = updated
        case _ =>
          logger.warn(s"Incorrect diff type in consumer filter: " + seqDiff.diff)
          None
      }
    }

    val (removed, added, modified) = MapDiffCalc.calculate(start, current)

    if (removed.nonEmpty || added.nonEmpty || modified.nonEmpty) {
      Some(MapUpdated(current, removed, added, modified))
    } else {
      None
    }
  }

  def resync(resync: Resync): Option[DataValueUpdate] = {
    resync.snapshot match {
      case d: MapSnapshot => {
        val (removed, added, modified) = MapDiffCalc.calculate(current, d.snapshot)
        current = d.snapshot

        if (removed.nonEmpty || added.nonEmpty || modified.nonEmpty) {
          Some(MapUpdated(current, removed, added, modified))
        } else {
          None
        }
      }
      case _ =>
        logger.warn(s"Incorrect snapshot type in consumer filter: " + resync)
        None
    }
  }
}

object AppendUpdateSynthesizer {

  def diffToAppend(v: SequenceTypeDiff): Option[AppendValue] = {
    v match {
      case v: AppendValue => Some(v)
      case _ => None
    }
  }

  def snapToAppends(v: AppendSnapshot): Seq[AppendValue] = {
    val prevs = v.previous.flatMap { seqDiff =>
      diffToAppend(seqDiff.diff)
    }

    prevs ++ diffToAppend(v.current.diff).map(Seq(_)).getOrElse(Seq())
  }
}
class AppendUpdateSynthesizer extends UpdateSynthesizer with LazyLogging {
  import AppendUpdateSynthesizer._

  private var lastOpt = Option.empty[AppendValue]

  def get(): DataValueUpdate = {
    Appended(lastOpt.map(Seq(_)).getOrElse(Seq()))
  }

  def delta(delta: Delta): Option[DataValueUpdate] = {
    val values = delta.diffs.flatMap { seqDiff =>
      diffToAppend(seqDiff.diff)
    }

    if (values.nonEmpty) {
      lastOpt = Some(values.last)
      Some(Appended(values))
    } else {
      None
    }
  }

  def resync(resync: Resync): Option[DataValueUpdate] = {
    resync.snapshot match {
      case v: AppendSnapshot =>

        val all = snapToAppends(v)

        if (all.nonEmpty) {
          lastOpt = Some(all.last)
          Some(Appended(all))
        } else {
          None
        }

      case _ =>
        logger.warn(s"Incorrect snapshot type in consumer filter: " + resync)
        None
    }
  }
}

class ConsumerUpdateFilter(cid: String, resync: ResyncSession, updates: UpdateSynthesizer) {

  private val filter = new GenInitializedStreamFilter(cid, resync)
  updates.resync(resync.resync) // TODO: manage the cache states better to not have to "warm this up"

  def handle(event: AppendEvent): Option[DataValueUpdate] = {
    filter.handle(event).flatMap {
      case sd: StreamDelta => updates.delta(sd.update)
      case rs: ResyncSnapshot => updates.resync(rs.resync)
      case rss: ResyncSession => updates.resync(rss.resync)
    }
  }

  def sync(): ValueSync = {
    ValueSync(resync.context.userMetadata, updates.get())
  }
}

class RowFilterImpl extends RowFilter with LazyLogging {

  private var activeFilterOpt = Option.empty[ConsumerUpdateFilter]

  def handle(event: AppendEvent): Option[ValueUpdate] = {
    event match {
      case ev: StreamDelta => activeFilterOpt.flatMap(_.handle(ev)).map(dv => ValueDelta(dv))
      case ev: ResyncSnapshot => activeFilterOpt.flatMap(_.handle(ev)).map(dv => ValueDelta(dv))
      case ev: ResyncSession => {

        val (update, updateFilter) = ev.resync.snapshot match {
          case s: SetSnapshot => (SetUpdated(s.snapshot, Set(), Set()), new SetUpdateSynthesizer(s.snapshot))
          case s: MapSnapshot => (MapUpdated(s.snapshot, Set(), Set(), Set()), new MapUpdateSynthesizer(s.snapshot))
          case s: AppendSnapshot =>
            val all = AppendUpdateSynthesizer.snapToAppends(s)
            (Appended(all), new AppendUpdateSynthesizer)
        }

        val filter = new ConsumerUpdateFilter("", ev, updateFilter)
        activeFilterOpt = Some(filter)

        Some(ValueSync(ev.context.userMetadata, update))
      }
    }
  }

  def sync(): Option[ValueSync] = {
    activeFilterOpt match {
      case None =>
        logger.warn(s"Filter sync had no active filter"); None
      case Some(filt) =>
        Some(filt.sync())
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
  def sync(): Option[ValueSync]
}

class SubscriptionFilterMap(sink: Sink[Seq[RowUpdate]]) extends LazyLogging {

  private val map = mutable.Map.empty[TypeValue, mutable.Map[TableRow, RowFilter]]

  private def lookup(rowId: RowId): Option[RowFilter] = {
    map.get(rowId.routingKey).flatMap(_.get(rowId.tableRow))
  }

  def sync(rowId: RowId): Option[ValueSync] = lookup(rowId).flatMap(_.sync())

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
  def sync(key: SubscriptionKey): Option[ValueSync]
}

class DynamicSubscriptionManager(eventThread: CallMarshaller) extends StreamDynamicSubscriptionManager with LazyLogging {

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

  def sync(key: SubscriptionKey): Option[ValueSync] = {
    keyRowMap.keyToRow.get(key).flatMap(filters.sync)
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
      case key: PeerBasedSubKey => key.row(session)
    }
  }

  private def rowMappings(session: PeerSessionId, set: Set[SubscriptionKey]): Map[SubscriptionKey, RowId] = {
    set.map {
      case k @ RowSubKey(row) => k -> row
      case k: PeerBasedSubKey => k -> k.row(session)
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
  def keys: Set[SubscriptionKey] = keyToRow.keySet
}

sealed trait SubscriptionKey
case class RowSubKey(row: RowId) extends SubscriptionKey
trait PeerBasedSubKey extends SubscriptionKey {
  def row(session: PeerSessionId): RowId
}

case class KeyedUpdate(key: SubscriptionKey, value: ValueUpdate)
