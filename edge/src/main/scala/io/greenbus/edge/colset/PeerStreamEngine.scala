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

object RouteManifestSet {
  def build: UserKeyedSet[TypeValue, RouteManifestEntry] = {
    new RenderedUserKeyedSet[TypeValue, RouteManifestEntry](
      key => Some(key),
      v => RouteManifestEntry.fromTypeValue(v).toOption)
  }
}

trait PeerLink {
  def setSubscriptions(rows: Set[RowId]): Unit
  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit
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
    Int64Val(entry.distance)
  }
  def fromTypeValue(tv: TypeValue): Either[String, RouteManifestEntry] = {
    tv match {
      case Int64Val(v) => Right(RouteManifestEntry(v.toInt))
      case _ => Left("Could not recognize route manifest type: " + tv)
    }
  }
}
case class RouteManifestEntry(distance: Int)

trait LocalGateway extends ManagedRouteSource

// Provides the manifest, which is always cached.
class LocalPeerRouteSource extends ManagedRouteSource {
  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = {}

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {}
}

trait RoutingManager {
  def routeToSourcing: Map[TypeValue, ServiceRouteProvider]
}

trait GatewayEventHandler {
  def localGatewayEvents(routeUpdate: Option[Set[TypeValue]], events: Seq[StreamEvent]): Unit
}

class PeerStreamEngine(logId: String, selfSession: PeerSessionId, gateway: LocalGateway) extends RoutingManager with GatewayEventHandler with LazyLogging {

  private val synthesizer = new SynthesizerTable[ManagedRouteSource]

  private val retailCacheTable = new RetailCacheTable

  private var routeSourcingMap = Map.empty[TypeValue, SourcingForRoute]
  private var rowsForSub = Map.empty[SubscriptionTarget, Set[RowId]]
  private var routesForSource = Map.empty[ManagedRouteSource, Set[TypeValue]]

  private var sourceMgrs = Map.empty[PeerLink, PeerRouteSource]

  private val manifestDb = new LocalPeerManifestDb(selfSession)

  private val localPeerRoute = new LocalPeerRouteSource
  addOrUpdateSourceRoute(localPeerRoute, TypeValueConversions.toTypeValue(selfSession), RouteManifestEntry(0))
  retailCacheTable.handleBatch(Seq(manifestDb.initial()))

  def routeToSourcing: Map[TypeValue, SourcingForRoute] = routeSourcingMap

  private def addOrUpdateSourceRoute(source: ManagedRouteSource, route: TypeValue, entry: RouteManifestEntry): Unit = {
    val mgr = routeSourcingMap.getOrElse(route, {
      val sourcing = new SourcingForRoute(route)
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

  private def removeSourceForRoute(source: ManagedRouteSource, route: TypeValue): Option[StreamEvent] = {
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
  def peerSourceEvents(link: PeerLink, events: Seq[StreamEvent]): Unit = {
    logger.trace(s"$logId peer source events $events")
    sourceMgrs.get(link) match {
      case None => logger.warn(s"$logId no source manager for link: $link")
      case Some(mgr) =>

        val emitted = synthesizer.handleBatch(mgr, events)
        logger.trace(s"$logId peer source events synthesized: $emitted")

        val manifestEvents = handleSourcingUpdate(mgr, emitted)

        handleRetailEvents(manifestEvents ++ emitted)
    }
  }

  def peerSourceConnected(peerSessionId: PeerSessionId, link: PeerLink): Unit = {
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

  def sourceDisconnected(link: PeerLink): Unit = {
    sourceMgrs.get(link) match {
      case None => logger.warn(s"$logId no source manager for link: $link")
      case Some(mgr) => {
        sourceMgrs -= link

        val routes = routesForSource.getOrElse(mgr, Set())
        val unresolvedEvents = routes.flatMap(removeSourceForRoute(mgr, _)).toVector
        routesForSource -= mgr
        val manifestEvents = manifestDb.routesUpdated(routes, routeSourcingMap)

        val sourcingEvents = unresolvedEvents ++ manifestEvents

        val emitted = synthesizer.sourceRemoved(mgr)
        logger.debug(s"$logId source removed sourcing events: $sourcingEvents, emitted events: $emitted")
        handleRetailEvents(sourcingEvents ++ emitted)
      }
    }
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
        val routeSourcingMgr = new SourcingForRoute(route)
        routeSourcingMap += (route -> routeSourcingMgr)
        routeSourcingMgr
      })

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
