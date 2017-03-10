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
package io.greenbus.edge.colset

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.collection.BiMultiMap

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable

/*
component: update-able subscriptions require the ability to either recognize a table row sub hasn't changed or
go back and get more columns from a later query. need sub sequence in the subscription control / notification

should all data/output keys optionally just be in the manifest???
is transparent access to remote values necessary stage 1, desirable ultimately?

!!! HOLD ON, why is everything not pull?
- instead of publishers, "sources"
- client connects: traditional pseudo-push
- peer relay has list of "sources": greenbus endpoint protocol
- related question: how do stores work? subscribe to all?

local endpoint publisher:
- establish publish to table row set (auth if amqp client)
- establish initial state/values for all table rows
- in peer, publisher registered for table row set
- user layer??:
  - endpoint manifest updated for peer
  - indexes updated according to descriptor


peer subscriber:
- a two-way channel opens, local peer is the subscription provider
- peer subscribes to tables: (endpoint set, endpoint index set, data index set, output index set) <--- THIS IS THE MANIFEST?? (these sets need to distinguish distance?)
- peer subscribes to a set of rows,
  - if local, snapshot is assembled and issued

local subscriber:
- a two-way channel opens, local peer is the subscription provider
- client subscribes at will
  - manifest tables reflect local, peer, and peer-derived
  - subscriber finds out about peer and peer-derived data rows from indexes or endpoint descs
    - !!! peer must infer presence of/path to remote data row from endpointId
    - if not local and not in master peer manifest, must maintain in map of unresolved endpoints
    - if local or remote publisher drops...? need activity state in notifications?
      - remote peer responds to derived sub with either data or an inactive "marker", which is passed on to client

peer source:
- a two-way channel opens, local peer is the subscriber
- subscribe to manifest table(s)
- update our global manifest
- update endpoint -> source path listing
- check unresolved subscriptions, add subs as necessary

local publisher removed:
- update global manifest
- update publisher-owned table rows with inactive flag

peer remote manifest removes endpoint:
- NO, do it when receive sub event on rows from remote peer // update endpoint table rows with inactive flag

subscriber removed:
- if last for remote row key
  - set timeout to GC this sub

OBJECTS:

- peer source channels
- peer source proxies

- per-peer session/rows?

- per-source manifest
- global manifest

- session -> row

- replicated rows
  - subscriber list

- local (pubbed) rows

- peer subscriber proxies
  - row queues

- peer subscriber channels



peer keyspace
source keyspace


is the subscriber-facing table generic, different peer library systems update rows in different ways?
- problem: tables need to be dynamic (create indexes on demand, ask for things that are in peer)
- solution?: user layer modules own tables, get callbacks
- problem: ordering semantics of session transitions would need to be in generic model
  - discontinuities understood?
    - loss of sync, changeover both refinements of discontinuity

solution: routing key, i.e. cassandra partition key

[peer A generic keyspace] <-[naming convention]-> [peer b remote proxy] <-> APP control/merge <->

subscription keyspace model
-> source/endpoint (with endpoint and row indexes)
-> edge logical endpoint model (proto descriptor, metadata, keys, outputs, data types)

 */

trait OutputRequest
trait OutputResponse

/*
I/O layers:

single closeable multi sender/receiver?? what about credit on multiple senders?
credit managing abstraction?
channel link abstraction (exposes credit?)
amqp
 */

/*
dbs:
links -> synthesizer -> retail

sub => sub mgr -> sourcing (peers, local pubs)

 */

//case class StreamEvent(inactiveFlagSet: Boolean)

/*
stream events:

use cases:
- append
  - delta
  - rebase snapshot
  - rebase session (with snapshot)
- row (routing key?) inactive
- backfill

 */
//case class LinkStreamEvent(rowKey: RoutedTableRowId)

/*
Synthesizer,
Retail stream cache,streams
Subscriber retail

 */

/*

is inactive a client subscriber concept?


Publish:

- routing/partition key
- row values
- row indexes

effects:
- update local manifest with routing as locally sourced, and indexes
- update master manifest / push to direct database with routing/indexes
- push to (routed? as self?) database

update active on inactive subs?
local set keeps updating current value when no subs, append has a window?
could local pubs be in a local zone that is a source link for the routed table, bridged only when a sub happens?
  - is this a better transition to pull-based clients

Subscribe:

- direct subs
- routed subs

procedure:
- foreach row
  - if already active
    - tag that row with sub
    - get cached according to query
    - setup sub for later logs
  - if not active and in unresolved set
    - add to sub table
    - publish inactive flag notification
  -  if not active and not in unresolved set
    - do manifest lookup
      - if unresolved, see above
      - if routing path exists
        - pick a path
        - add to link subscription

Manifest update:
- merge scala tables
- update direct manifest tables
- if routingKey removed, new session may be active

 */

/*

Peer manifest
Local manifest
Already-active peer link subs, and their targets
Unresolved subscriptions


data (cache) table: routed_row -> row_db
subscription table: routed_row -> set[subscriber]
unresolved row set: set[routed_row] OR set[routingKey -> set[row]]

 */

class RetailCacheTable extends LazyLogging {
  private var rows = Map.empty[TypeValue, Map[TableRow, RetailRowCache]]

  def getSync(row: RowId): Seq[StreamEvent] = {
    lookup(row).map(log => log.sync())
      .map(_.map(apEv => RowAppendEvent(row, apEv)).toVector)
      .getOrElse(Seq())
  }

  def handleBatch(events: Seq[StreamEvent]): Unit = {
    events.foreach {
      case ev: RowAppendEvent => {
        lookup(ev.rowId) match {
          case None =>
            addRouted(ev, ev.rowId.routingKey, rows.getOrElse(ev.rowId.routingKey, Map()))
          case Some(log) => log.handle(ev.appendEvent)
        }
      }
      case ev: RouteUnresolved =>
    }
  }

  private def lookup(rowId: RowId): Option[RetailRowCache] = {
    rows.get(rowId.routingKey).flatMap { map =>
      map.get(rowId.tableRow)
    }
  }

  private def addRouted(ev: RowAppendEvent, routingKey: TypeValue, existingRows: Map[TableRow, RetailRowCache]): Unit = {
    ev.appendEvent match {
      case resync: ResyncSession =>
        RetailRowCache.build(ev.rowId, resync.sessionId, resync.snapshot) match {
          case None => logger.warn(s"Initial row event could not create cache: $ev")
          case Some(log) =>
            rows += (routingKey -> (existingRows + (ev.rowId.tableRow -> log)))
        }
      case _ =>
        logger.warn(s"Initial row event was not resync session: $ev")
    }
  }

  def removeRoute(route: TypeValue): Unit = {
    rows -= route
  }
}

/*
Synthesizer,
Retail stream cache,streams
Subscriber retail

 */

object RouteManifestSet {
  def build: UserKeyedSet[TypeValue, RouteManifestEntry] = {
    new RenderedUserKeyedSet[TypeValue, RouteManifestEntry](
      key => Some(key),
      v => RouteManifestEntry.fromTypeValue(v).toOption)
  }
}

trait StreamSource

trait PeerSourceLink extends StreamSource {
  def setSubscriptions(rows: Set[RowId]): Unit
}

/*

Peer A
- peer-local space
  - peer A manifest
  - subscriptions for peer N
- gateway space (publishers)
  - map of publisher -> set[routing key]
  - peer handler: onGatewayUpdated(set[routing key], route source)
- peer-sourced space
  - map of source -> set[routing key]


Peer A connects to Peer B as a source
- Peer A subscribes to Peer B's manifest keys
- Peer B subscribes to Peer A's peer-B-subscription-set key
- the B -> A special replication channel features stream events controlled by peer-B-subscription-set

not better? just put set-change semantics in subscription-set push?

 */

trait RouteSource {
  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit
}

object PeerRouteSource {
  val tablePrefix = "__manifest"

  def peerRouteRow(peerId: PeerSessionId): RowId = {
    RowId(TypeValueConversions.toTypeValue(peerId), SymbolVal(s"$tablePrefix"), SymbolVal("routes"))
  }
  def peerIndexRow(peerId: PeerSessionId): RowId = {
    RowId(TypeValueConversions.toTypeValue(peerId), SymbolVal(s"$tablePrefix"), SymbolVal("indexes"))
  }

  def manifestRows(peerId: PeerSessionId): Set[RowId] = {
    Set(peerRouteRow(peerId) /*, peerIndexRow(peerId)*/ )
  }
}
class PeerRouteSource(peerId: PeerSessionId, source: PeerSourceLink) extends RouteSource with LazyLogging {

  private val routeRow = PeerRouteSource.peerRouteRow(peerId)
  private val indexRow = PeerRouteSource.peerIndexRow(peerId)

  private val routeLog = RouteManifestSet.build

  private var routeToRows: Map[TypeValue, Set[TableRow]] = Map(TypeValueConversions.toTypeValue(peerId) -> Set(routeRow.tableRow /*, indexRow.tableRow*/ ))

  private val keys: Set[RowId] = Set(routeRow /*, indexRow*/ )
  def manifestKeys: Set[RowId] = keys
  def manifestRoute: TypeValue = TypeValueConversions.toTypeValue(peerId)

  def init(): Unit = {
    source.setSubscriptions(manifestKeys)
  }

  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = {
    if (rows.nonEmpty) {
      routeToRows += (route -> rows)
      pushSubscription()
    } else {
      routeToRows -= route
      pushSubscription()
    }
  }

  private def pushSubscription(): Unit = {
    val rowIdSet = routeToRows.flatMap { case (route, rows) => rows.map(_.toRowId(route)) }.toSet
    source.setSubscriptions(rowIdSet)
  }

  def snapshot(): Map[TypeValue, RouteManifestEntry] = {
    routeLog.lastSnapshot
  }

  def handleSelfEvents(events: Seq[StreamEvent]): Option[KeyedSetDiff[TypeValue, RouteManifestEntry]] = {
    events.foreach {
      case RowAppendEvent(row, ev) =>
        row match {
          case `routeRow` => routeLog.handle(Seq(ev))
          //case `indexRow` =>
          case other => logger.debug(s"Unexpected row in $peerId manifest events: $other")
        }
      case sev =>
        logger.warn(s"Unexpected self event for $peerId: $sev")
    }
    routeLog.dequeue()
  }
}

/*

handles:
- sub appears: resolve routes, enqueue unresolved or add subscription to a source
- sub disappears: unmark source subs, remove if unnecessary
- source appears: add to manifest rows to observe
- source manifest update: resolve unresolved, add/remove known routes
- source disappears: check for now-unsourced routes, switch to a different source or emit unresolved.
!!!NOTE: conflict with the synthesizer's concept of unresolved

manifest rows -> source (manifest)

 */

trait SubscriptionTarget {
  def handleBatch(events: Seq[StreamEvent])
}

object RouteManifestEntry {
  def toTypeValue(entry: RouteManifestEntry): TypeValue = {
    UInt64Val(entry.distance)
  }
  def fromTypeValue(tv: TypeValue): Either[String, RouteManifestEntry] = {
    tv match {
      case UInt64Val(v) => Right(RouteManifestEntry(v.toInt))
      case _ => Left("Could not recognize route manifest type: " + tv)
    }
  }
}
case class RouteManifestEntry(distance: Int) {
}

class RouteSourcingMgr(route: TypeValue) extends LazyLogging {

  private var subscribersToRows = BiMultiMap.empty[SubscriptionTarget, TableRow]
  private def subscribedRows: Set[TableRow] = subscribersToRows.valToKey.keySet

  private var currentSource = Option.empty[RouteSource]
  private var standbySources = Set.empty[RouteSource]
  private var manifestMap = Map.empty[RouteSource, RouteManifestEntry]

  def sourceMap: Map[RouteSource, RouteManifestEntry] = manifestMap

  def resolved(): Boolean = currentSource.nonEmpty
  def unused(): Boolean = currentSource.isEmpty && subscribersToRows.keyToVal.keySet.isEmpty

  def handleBatch(events: Seq[StreamEvent]): Unit = {
    logger.trace(s"Route $route handling batch $events")

    val map = mutable.Map.empty[SubscriptionTarget, VectorBuilder[StreamEvent]]

    events.foreach {
      case ev: RowAppendEvent =>
        subscribersToRows.valToKey.get(ev.rowId.tableRow).foreach {
          _.foreach { target =>
            val b = map.getOrElseUpdate(target, new VectorBuilder[StreamEvent])
            b += ev
          }
        }
      case ev: RouteUnresolved =>
        subscribersToRows.keyToVal.keys.foreach { target =>
          val b = map.getOrElseUpdate(target, new VectorBuilder[StreamEvent])
          b += ev
        }
    }

    map.foreach {
      case (target, b) => target.handleBatch(b.result())
    }
  }

  def modifySubscription(subscriber: SubscriptionTarget, removed: Set[TableRow], added: Set[TableRow]): Unit = {
    val prev = subscribedRows
    subscribersToRows = subscribersToRows.removeMappings(subscriber, removed).add(subscriber, added)
    if (prev != subscribedRows) {
      currentSource.foreach(_.updateRowsForRoute(route, subscribedRows))
    }
  }

  def subscriberRemoved(subscriber: SubscriptionTarget): Unit = {
    logger.debug(s"Route $route subscriber removed before $subscribersToRows")
    val prev = subscribedRows
    subscribersToRows = subscribersToRows.removeKey(subscriber)
    if (prev != subscribedRows) {
      currentSource.foreach(_.updateRowsForRoute(route, subscribedRows))
    }
    logger.debug(s"Route $route subscriber removed after $subscribersToRows")
  }

  def sourceAdded(source: RouteSource, desc: RouteManifestEntry): Unit = {
    if (currentSource.isEmpty) {
      currentSource = Some(source)
      if (subscribedRows.nonEmpty) {
        source.updateRowsForRoute(route, subscribedRows)
      }
    } else {
      standbySources += source
    }
    manifestMap += (source -> desc)
  }

  def sourceRemoved(source: RouteSource): Option[RouteUnresolved] = {
    logger.debug(s"Route $route source removed start: $currentSource - $standbySources - $manifestMap")
    val resultOpt = if (currentSource.contains(source)) {
      if (standbySources.nonEmpty) {
        val next = standbySources.head
        standbySources -= next
        currentSource = Some(next)
        if (subscribedRows.nonEmpty) {
          next.updateRowsForRoute(route, subscribedRows)
        }
        None
      } else {
        currentSource = None
        Some(RouteUnresolved(route))
      }
    } else {
      standbySources -= source
      None
    }
    manifestMap -= source
    logger.debug(s"Route $route source removed end: $currentSource - $standbySources - $manifestMap")
    resultOpt
  }
}

trait LocalGateway extends RouteSource with StreamSource

class ManifestDb(selfSession: PeerSessionId) {

  private val routeRow = PeerRouteSource.peerRouteRow(selfSession)

  private var sequence: Long = 0
  private var manifest = Map.empty[TypeValue, RouteManifestEntry]

  def initial(): StreamEvent = {
    val result = RowAppendEvent(routeRow, ResyncSession(selfSession, ModifiedKeyedSetSnapshot(UInt64Val(sequence), Map())))
    sequence += 1
    result
  }

  def routesUpdated(routes: Set[TypeValue], map: Map[TypeValue, RouteSourcingMgr]): Seq[StreamEvent] = {

    val removed = Vector.newBuilder[TypeValue]
    val modified = Vector.newBuilder[(TypeValue, RouteManifestEntry)]
    val added = Vector.newBuilder[(TypeValue, RouteManifestEntry)]

    routes.foreach { route =>
      map.get(route) match {
        case None => {
          if (manifest.contains(route)) removed += route
        }
        case Some(sourcing) => {
          if (!sourcing.resolved()) {
            if (manifest.contains(route)) removed += route
          } else {
            val minimumDistance = sourcing.sourceMap.map(_._2.distance).min
            manifest.get(route) match {
              case None => added += (route -> RouteManifestEntry(minimumDistance))
              case Some(entry) =>
                if (entry.distance != minimumDistance) {
                  modified += (route -> RouteManifestEntry(minimumDistance))
                }
            }
          }
        }
      }
    }

    def writeKv(tup: (TypeValue, RouteManifestEntry)): (TypeValue, TypeValue) = {
      val (route, manifest) = tup
      (route, RouteManifestEntry.toTypeValue(manifest))
    }

    val removedResult = removed.result().toSet
    val modifiedResult = modified.result().toSet
    val addedResult = added.result().toSet

    if (removedResult.nonEmpty || modifiedResult.nonEmpty || addedResult.nonEmpty) {
      val results = Seq(RowAppendEvent(routeRow, StreamDelta(ModifiedKeyedSetDelta(UInt64Val(sequence), removedResult, addedResult.map(writeKv), modifiedResult.map(writeKv)))))
      sequence += 1
      manifest = (manifest -- removedResult) ++ addedResult ++ modifiedResult
      results
    } else {
      Seq()
    }
  }
}

class PeerLocalRouteSource extends RouteSource {
  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = {}
}

class PeerStreamEngine(logId: String, selfSession: PeerSessionId, gateway: LocalGateway) extends LazyLogging {

  private val synthesizer = new SynthesizerTable

  private val retailCacheTable = new RetailCacheTable

  private var routeSourcingMap = Map.empty[TypeValue, RouteSourcingMgr]
  private var rowsForSub = Map.empty[SubscriptionTarget, Set[RowId]]
  private var routesForSource = Map.empty[RouteSource, Set[TypeValue]]

  private var sourceMgrs = Map.empty[PeerSourceLink, PeerRouteSource]

  private val manifestDb = new ManifestDb(selfSession)

  private val peerLocalRoute = new PeerLocalRouteSource
  addOrUpdateSourceRoute(peerLocalRoute, TypeValueConversions.toTypeValue(selfSession), RouteManifestEntry(0))
  retailCacheTable.handleBatch(Seq(manifestDb.initial()))

  private def addOrUpdateSourceRoute(source: RouteSource, route: TypeValue, entry: RouteManifestEntry): Unit = {
    val mgr = routeSourcingMap.getOrElse(route, {
      val sourcing = new RouteSourcingMgr(route)
      routeSourcingMap += (route -> sourcing)
      sourcing
    })

    mgr.sourceAdded(source, entry)
  }

  private def handleSourcingUpdate(mgr: PeerRouteSource, events: Seq[StreamEvent]): Seq[StreamEvent] = {
    val manifestEvents = events.filter(_.routingKey == mgr.manifestRoute)
    if (manifestEvents.nonEmpty) {

      logger.debug(s"$logId peer sourcing update events: $manifestEvents")
      val diffOpt = mgr.handleSelfEvents(manifestEvents)
      logger.trace(s"$logId peer sourcing update diff: $diffOpt")

      diffOpt match {
        case None => Seq()
        case Some(diff) =>
          val removeEvents = diff.removed.flatMap(removeSourceForRoute(mgr, _)).toVector

          val updates = diff.added ++ diff.modified
          updates.foreach {
            case (route, entry) => addOrUpdateSourceRoute(mgr, route, entry.copy(distance = entry.distance + 1))
          }

          routesForSource += (mgr -> diff.snapshot.keySet)

          val allUpdated = diff.removed ++ diff.modified.map(_._1) ++ diff.added.map(_._1)
          val manifestUpdates = manifestDb.routesUpdated(allUpdated, routeSourcingMap)

          removeEvents ++ manifestUpdates
      }
    } else {
      Seq()
    }
  }

  private def removeSourceForRoute(source: RouteSource, route: TypeValue): Option[StreamEvent] = {
    routeSourcingMap.get(route).flatMap { sourcing =>
      val resultOpt = sourcing.sourceRemoved(source)
      if (sourcing.unused()) {
        routeSourcingMap -= route
        retailCacheTable.removeRoute(route)
      }
      resultOpt
    }
  }

  private def handleRetailEvents(events: Seq[StreamEvent]): Unit = {
    retailCacheTable.handleBatch(events)

    // TODO: maintain ordering across routes?
    events.groupBy(_.routingKey).foreach {
      case (route, routeEvents) =>
        routeSourcingMap.get(route).foreach { mgr => mgr.handleBatch(routeEvents) }
    }
  }

  def localGatewayEvents(routeUpdate: Option[Set[TypeValue]], events: Seq[StreamEvent]): Unit = {
    routeUpdate.foreach(route => logger.trace(s"$logId local gateway route update: $route"))
    if (events.nonEmpty) logger.trace(s"$logId local gateway events: $events")

    val synthesizedEvents = synthesizer.handleBatch(gateway, events)

    val routingEvents = routeUpdate.map(localGatewayRoutingUpdate).getOrElse(Seq())

    val allEvents = routingEvents ++ synthesizedEvents
    if (allEvents.nonEmpty) {
      handleRetailEvents(allEvents)
    }
  }

  private def localGatewayRoutingUpdate(routes: Set[TypeValue]): Seq[StreamEvent] = {
    val prev = routesForSource.getOrElse(gateway, Set())
    routesForSource += (gateway -> routes)
    val adds = routes -- prev
    val removes = prev -- routes
    removes.foreach(remove => removeSourceForRoute(gateway, remove))
    adds.foreach(add => addOrUpdateSourceRoute(gateway, add, RouteManifestEntry(distance = 0)))

    val manifestEvents = manifestDb.routesUpdated(adds ++ removes, routeSourcingMap)
    val unresolvedEvents = removes.map(RouteUnresolved).toVector

    unresolvedEvents ++ manifestEvents
  }

  /*
    update manifest
      - resolve resolveable unresolved subscriptions
      - apply to manifest subscribers' logs

    get events yielded by applying this source's events
      - apply to retail caches
      - apply to subscribers' logs

    manifest component just another subscriber, do event before commit to subscribers?
   */
  def peerSourceEvents(link: PeerSourceLink, events: Seq[StreamEvent]): Unit = {
    logger.trace(s"$logId peer source events $events")
    val emitted = synthesizer.handleBatch(link, events)
    logger.trace(s"$logId peer source events synthesized: $emitted")

    val manifestEvents = sourceMgrs.get(link) match {
      case None =>
        logger.warn(s"$logId no source manager for link: $link"); Seq()
      case Some(mgr) => handleSourcingUpdate(mgr, emitted)
    }

    handleRetailEvents(manifestEvents ++ emitted)
  }

  def peerSourceConnected(peerSessionId: PeerSessionId, link: PeerSourceLink): Unit = {
    logger.debug(s"$logId source connected: $peerSessionId")
    sourceMgrs.get(link) match {
      case None => {
        val mgr = new PeerRouteSource(peerSessionId, link)
        mgr.init()
        sourceMgrs += (link -> mgr)
      }
      case Some(mgr) =>
        logger.warn(s"$logId link for $peerSessionId had connect event but already existed")
    }
  }

  def sourceDisconnected(link: PeerSourceLink): Unit = {

    val sourcingEvents = sourceMgrs.get(link) match {
      case None => Seq()
      case Some(mgr) =>
        val routes = routesForSource.getOrElse(mgr, Set())
        val unresolvedEvents = routes.flatMap(removeSourceForRoute(mgr, _)).toVector
        routesForSource -= mgr
        val manifestEvents = manifestDb.routesUpdated(routes, routeSourcingMap)

        unresolvedEvents ++ manifestEvents
    }

    sourceMgrs -= link

    val emitted = synthesizer.sourceRemoved(link)
    logger.debug(s"$logId source removed sourcing events: $sourcingEvents, emitted events: $emitted")
    handleRetailEvents(sourcingEvents ++ emitted)
  }

  // - sub appears: resolve routes, enqueue unresolved or add subscription to a source
  def subscriptionsRegistered(subscriber: SubscriptionTarget, params: StreamSubscriptionParams): Unit = {

    val rows = params.rows.toSet

    val existing = rowsForSub.getOrElse(subscriber, Set())

    val added = rows -- existing
    val removed = existing -- rows

    val removedRouteMap: Map[TypeValue, Set[TableRow]] = RowId.setToRouteMap(removed)
    val addedRouteMap: Map[TypeValue, Set[TableRow]] = RowId.setToRouteMap(added)

    logger.debug(s"$logId subscriptions params: $params, added: $addedRouteMap, removed: $removedRouteMap")
    val updatedRoutes = removedRouteMap.keySet ++ addedRouteMap.keySet

    val streamEvents = updatedRoutes.flatMap { route =>

      val removes = removedRouteMap.getOrElse(route, Set())
      val adds = addedRouteMap.getOrElse(route, Set())

      val sourcing = routeSourcingMap.getOrElse(route, {
        val routeSourcingMgr = new RouteSourcingMgr(route)
        routeSourcingMap += (route -> routeSourcingMgr)
        routeSourcingMgr
      })

      // TODO: these two calls need to be merged for one transactional subscription update
      if (removes.nonEmpty || adds.nonEmpty) {
        sourcing.modifySubscription(subscriber, removes, adds)
      }

      // TODO: branch above to prevent dumb add/remove
      if (sourcing.unused()) {
        routeSourcingMap -= route
        retailCacheTable.removeRoute(route)
      }

      val addIds = adds.map(tr => RowId(route, tr.table, tr.rowKey))

      // For every row, either route is unresolved, sub was already active and cached, or subscribe above will
      // cause a sync/unavail event at a later time, meanwhile that row is pending for the subscriber
      if (sourcing.resolved()) {
        addIds.flatMap(row => retailCacheTable.getSync(row))
      } else {
        Seq(RouteUnresolved(route))
      }
    }

    rowsForSub += (subscriber -> rows)

    if (streamEvents.nonEmpty) {
      logger.debug(s"$logId initial subscription events: $streamEvents")
      subscriber.handleBatch(streamEvents.toSeq)
    }

  }

  def subscriberRemoved(subscriber: SubscriptionTarget): Unit = {
    val routesForSub = rowsForSub.getOrElse(subscriber, Set()).map(_.routingKey).toSet
    routesForSub.foreach { route =>
      routeSourcingMap.get(route).foreach { sourcing =>
        sourcing.subscriberRemoved(subscriber)
        if (sourcing.unused()) {
          routeSourcingMap -= route
          retailCacheTable.removeRoute(route)
        }
      }
    }
    rowsForSub -= subscriber

  }

}

/*
Highest level

App Entry
- Peer
  - Subscription protocol engine
  - Output forwarding engine
- Link management / I/O

 */

object SourcedEndpointPeer {
  val tablePrefix = "sep"
  val endpointTable = SymbolVal(s"$tablePrefix.endpoints")
  val endpointIndexTable = SymbolVal(s"$tablePrefix.endpointIndexes")
  val keyIndexTable = SymbolVal(s"$tablePrefix.keyIndexes")

}

class SourcedEndpointPeer {

  //private val keySpace: Database = null

  def onLocalPublisherOpened(): Unit = ???
  def onLocalPublisherClosed(): Unit = ???

  //def onRemotePeerOpened(proxy: RemotePeerProxy): Unit = ???
  def onRemotePeerClosed(): Unit = ???

  //def onPeerRemoteManifestUpdate()
  def onRemotePeerNotifications(notifications: SubscriptionNotifications): Unit = ???

  def onPeerSubscriber(): Unit = ???
  def onLocalSubscriber(): Unit = ???

}