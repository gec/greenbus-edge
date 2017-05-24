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
package io.greenbus.edge.stream.gateway2

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow.{QueuedDistributor, Sink, Source}
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.engine2.{GenericSource, SourceEvents}
import io.greenbus.edge.stream.gateway.{MapDiffCalc, RouteServiceRequest}

import scala.collection.mutable

class RoutePublish {

}

trait DynamicTable {
  def subscribed(key: TypeValue): Unit
  def unsubscribed(key: TypeValue): Unit
}

case class RoutePublishConfig(
  appendKeys: Seq[(TableRow, SequenceCtx)],
  setKeys: Seq[(TableRow, SequenceCtx)],
  mapKeys: Seq[(TableRow, SequenceCtx)],
  dynamicTables: Seq[(TableRow, DynamicTable)],
  handler: Sink[RouteServiceRequest])

object PublisherRouteMgr {

  def build(route: TypeValue, cfg: RoutePublishConfig): PublisherRouteMgr = {
    ???
  }
}
class PublisherRouteMgr(route: TypeValue, sequencer: RoutePublishSequencer) {

  def handleBatch(batch: PublishBatch): Seq[RowAppendEvent] = {
    sequencer.handleBatch(batch)
  }
}

class GatewayPublisherSource extends GenericSource with LazyLogging {

  //private val subQueue = new QueuedDistributor[Set[RowId]]
  private val eventQueue = new QueuedDistributor[SourceEvents]
  private val responseQueue = new QueuedDistributor[Seq[ServiceResponse]]

  def subscriptions: Sink[Set[RowId]] = ???
  def events: Source[SourceEvents] = eventQueue
  def requests: Sink[Seq[ServiceRequest]] = ???
  def responses: Source[Seq[ServiceResponse]] = responseQueue

  private def onRequests(requests: Seq[ServiceRequest]): Unit = {

  }
  private def onSubscription(requests: Seq[ServiceRequest]): Unit = {

  }
}

/*
  route pub lifetime map
    - * route mgrs
    - set(routes)

  single gateway proxy channel lifetime OR

  generic source implementation, used by stream engine,
  route set needs to find its way to gateway proxy channel implementation of generic target

 */
class PublisherRouteSetMgr(handle: GatewayPublishHandle) extends LazyLogging {

  private val publishedRoutes = mutable.Map.empty[TypeValue, PublisherRouteMgr]

  def routePublished(route: TypeValue, config: RoutePublishConfig): Unit = {
    val contained = publishedRoutes.contains(route)
    publishedRoutes.update(route, PublisherRouteMgr.build(route, config))
    if (!contained) {
      handle.events(Some(publishedRoutes.keySet.toSet), Seq())
    }
  }
  def routeUnpublished(route: TypeValue): Unit = {
    val contained = publishedRoutes.contains(route)
    publishedRoutes -= route
    if (contained) {
      handle.events(Some(publishedRoutes.keySet.toSet), Seq())
    }
  }
  def routeBatch(route: TypeValue, batch: PublishBatch): Unit = {
    publishedRoutes.get(route) match {
      case None => logger.warn(s"Batch for unpublished route: $route")
      case Some(mgr) =>
        val events = mgr.handleBatch(batch)
        handle.events(None, events)
    }
  }
}

//case class SourceEvents(routeUpdatesOpt: Option[Map[TypeValue, RouteManifestEntry]], events: Seq[StreamEvent])

/*trait GenericSource {
  def subscriptions: Sink[Set[RowId]]
  def events: Source[SourceEvents]
  def requests: Sink[Seq[ServiceRequest]]
  def responses: Source[Seq[ServiceResponse]]
}

trait GenericTarget {
  def events(events: Seq[StreamEvent]): Unit
}*/

/*trait RouteSourcing {
  def observeForDelivery(events: Seq[StreamEvent]): Unit
  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit
}

trait StreamSourcingManager[Source, Target] {

  def sourcing: Map[TypeValue, RouteSourcing]

  def sourceUpdate(source: Source, routes: Map[TypeValue, RouteManifestEntry]): Unit
  def sourceRemoved(source: Source): Unit

  def targetUpdate(target: Target, rows: Set[RowId]): Unit
  def targetRemoved(target: Target): Unit

}*/
/*abstract class StreamEngine {
  // cache
  // sourcing map

  def sourceAdded(source: GenericSource): Unit
  def sourceRemoved(source: GenericSource): Unit
  //def sourceEvents(events: SourceEvents): Unit

  //def targetAdded(): Unit
  def targetSubscriptionUpdate(rows: Set[RowId]): Unit
  def targetRemoved(): Unit

}*/

/*

  alternative:

  KeyStream
    -> intake filter?
    -> synthesizer
      - session set,
    -> cache
    -> target set
    -> queues

  maps:
    updates/removes to source links (potentially) remove sessions in synth
    updates/removes to source links can remove from cache
    lack of subscription means whole stream goes away
    added subscription means new key stream created or anchored

    peer has separate map for manifest keys?
   */

/*

PeerLinkSource <: GenericSource --> LinkSources <: GenericSource
  | GatewaySource <: GenericSource

Source manager:
- route -> route sourcing
- source list

-----------------------------

abstract: sourcing :: cache :: sub channels
gateway: [pubroutes] :: cache :: single channel?
peer: [link sourcing -> synth] :: cache :: multichannel

joined: [gw OR peerlinks] -> src mgr :: cache :: sub channels

data:
sources -> cache & sub channels

subs:
sub channels -> route sourcing, unresolved or does sub


route source mgr:

- observeForDelivery(seq streamevents)





-----

SubTable:
- target -> submgr
- rowid -> Set(submgr)

SubMgr:
- interface for syncs...?
- rowId -> queue
- enqueue(row, update)
- flush()

ALTERNATELY:

RouteSourcing
- target -> Set[tablerows]

 */

trait StreamCacheTable {
  def handleEvents(events: Seq[RowAppendEvent]): Unit
  def sync(rows: Set[RowId]): Unit
  def removeRoute(route: TypeValue): Unit
}

/*class StreamCacheTableImpl extends StreamCacheTable {
  private val routeMap = mutable.Map.empty[TypeValue, Map[TableRow,]]
  def handleEvents(events: Seq[RowAppendEvent]): Unit = {

  }

  def sync(rows: Set[RowId]): Unit = {

  }

  def removeRoute(route: TypeValue): Unit = {

  }
}*/

trait GatewayPublishHandle {
  def events(routeUpdates: Option[Set[TypeValue]], batch: Seq[StreamEvent]): Unit
  def respond(responses: Seq[ServiceResponse]): Unit
}

class PublisherMgr extends LazyLogging {

  private val publishedRoutes = mutable.Map.empty[TypeValue, PublisherRouteMgr]

  private val cacheTable: StreamCacheTable = null

  private var handleOpt = Option.empty[GatewayPublishHandle]

  def routePublished(route: TypeValue, config: RoutePublishConfig): Unit = {
    val contained = publishedRoutes.contains(route)
    publishedRoutes.update(route, PublisherRouteMgr.build(route, config))
    if (!contained) {
      handleOpt.foreach(_.events(Some(publishedRoutes.keySet.toSet), Seq()))
    }
  }
  def routeUnpublished(route: TypeValue): Unit = {
    val contained = publishedRoutes.contains(route)
    publishedRoutes -= route
    if (contained) {
      handleOpt.foreach(_.events(Some(publishedRoutes.keySet.toSet), Seq()))
    }
  }
  def routeBatch(route: TypeValue, batch: PublishBatch): Unit = {
    publishedRoutes.get(route) match {
      case None => logger.warn(s"Batch for unpublished route: $route")
      case Some(mgr) =>
        val events = mgr.handleBatch(batch)
        cacheTable.handleEvents(events)
    }
  }

  def onConnect(handle: GatewayPublishHandle): Unit = {
    handleOpt = Some(handle)
    handle.events(Some(publishedRoutes.keySet.toSet), Seq())
  }
  def onDisconnect(): Unit = {
    handleOpt = None
  }

  def onSubscriptionUpdate(rows: Set[RowId]): Unit = {

  }
  def onServiceRequest(proxy: GatewayProxyChannel, serviceRequestBatch: Seq[ServiceRequest]): Unit = {

  }

}

case class AppendPublish(key: TableRow, values: Seq[TypeValue])
case class SetPublish(key: TableRow, value: Set[TypeValue])
case class MapPublish(key: TableRow, value: Map[TypeValue, TypeValue])
case class PublishBatch(
  appendUpdates: Seq[AppendPublish],
  mapUpdates: Seq[MapPublish],
  setUpdates: Seq[SetPublish])

class RoutePublishSequencer(route: TypeValue, appends: Map[TableRow, AppendSequencer], sets: Map[TableRow, SetSequencer], maps: Map[TableRow, MapSequencer]) {

  def handleBatch(batch: PublishBatch): Seq[RowAppendEvent] = {

    val appendEvents = batch.appendUpdates.flatMap { update =>
      appends.get(update.key)
        .map(_.handle(update.values))
        .getOrElse(Seq())
        .map(ev => RowAppendEvent(update.key.toRowId(route), ev))
    }
    val setEvents = batch.setUpdates.flatMap { update =>
      sets.get(update.key)
        .map(_.handle(update.value))
        .getOrElse(Seq())
        .map(ev => RowAppendEvent(update.key.toRowId(route), ev))
    }
    val mapEvents = batch.mapUpdates.flatMap { update =>
      maps.get(update.key)
        .map(_.handle(update.value))
        .getOrElse(Seq())
        .map(ev => RowAppendEvent(update.key.toRowId(route), ev))
    }

    appendEvents ++ setEvents ++ mapEvents
  }
}

class AppendSequencer(session: PeerSessionId, ctx: SequenceCtx) {
  private var sequence: Long = 0
  private var uninit = true

  def handle(values: Seq[TypeValue]): Seq[AppendEvent] = {
    assert(values.nonEmpty)

    val start = sequence
    val results = values.zipWithIndex.map {
      case (v, i) => (Int64Val(start + i), v)
    }
    sequence += values.size

    val diffs = results.map { case (i, v) => SequencedDiff(i, AppendValue(v)) }

    if (uninit) {
      val last = diffs.last
      val init = diffs.init
      val snap = AppendSnapshot(last, init)
      uninit = false
      Seq(ResyncSession(session, ctx, Resync(last.sequence, snap)))
    } else {
      Seq(StreamDelta(Delta(diffs)))
    }
  }
}

object SetSequencer {

  def diff(next: Set[TypeValue], prev: Set[TypeValue]): SetDiff = {
    val added = next -- prev
    val removed = prev -- next
    SetDiff(removes = removed, adds = added)
  }

  def toDelta(seq: Long, diff: SetDiff): Delta = {
    Delta(Seq(SequencedDiff(Int64Val(seq), diff)))
  }

  def toSnapshot(seq: Long, current: Set[TypeValue]): Resync = {
    Resync(Int64Val(seq), SetSnapshot(current))
  }
}
class SetSequencer(session: PeerSessionId, ctx: SequenceCtx) {
  import SetSequencer._

  private var sequence: Long = 0
  private var prevOpt = Option.empty[Set[TypeValue]]

  def handle(value: Set[TypeValue]): Seq[AppendEvent] = {
    val seq = sequence
    sequence += 1

    prevOpt match {
      case None =>
        Seq(ResyncSession(session, ctx, SetSequencer.toSnapshot(seq, value)))
      case Some(prev) => {
        Seq(StreamDelta(toDelta(seq, diff(value, prev))))
      }
    }
  }
}

object MapSequencer {

  def diff(next: Map[TypeValue, TypeValue], prev: Map[TypeValue, TypeValue]): MapDiff = {
    val (removed, added, modified) = MapDiffCalc.calculate(prev, next)
    MapDiff(removed, added, modified)
  }

  def toDelta(seq: Long, diff: MapDiff): Delta = {
    Delta(Seq(SequencedDiff(Int64Val(seq), MapDiff(diff.removes, diff.adds, diff.modifies))))
  }

  def toSnapshot(seq: Long, current: Map[TypeValue, TypeValue]): Resync = {
    Resync(Int64Val(seq), MapSnapshot(current))
  }
}
class MapSequencer(session: PeerSessionId, ctx: SequenceCtx) {
  import MapSequencer._

  private var sequence: Long = 0
  private var prevOpt = Option.empty[Map[TypeValue, TypeValue]]

  def handle(value: Map[TypeValue, TypeValue]): Seq[AppendEvent] = {
    val seq = sequence
    sequence += 1

    prevOpt match {
      case None =>
        Seq(ResyncSession(session, ctx, toSnapshot(seq, value)))
      case Some(prev) => {
        Seq(StreamDelta(toDelta(seq, diff(value, prev))))
      }
    }
  }
}
