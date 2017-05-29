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
package io.greenbus.edge.stream.engine2

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow.{ Sink, Source }
import io.greenbus.edge.stream._

import scala.collection.mutable

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

case class SourceEvents(routeUpdatesOpt: Option[Map[TypeValue, RouteManifestEntry]], events: Seq[StreamEvent])

trait StreamTarget {
  //def flush(): Unit
}

/*

def targetUpdate(target: StreamObserver, subscription: Map[TableRow, KeyStreamObserver]): Unit = {
 */

trait RouteServiceProvider {
  //def observeForDelivery(events: Seq[StreamEvent]): Unit
  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit
}

/*

Source mgr
<- register observers
-> notify flush
Target mgr

 */

class RouteSourcingMgr(route: TypeValue) extends LazyLogging {

  private val sourcesMap = mutable.Map.empty[RouteStreamSource, RouteManifestEntry]
  private var subscription = Set.empty[TableRow]
  private var streamOpt = Option.empty[RouteStreamMgr]
  private var currentOpt = Option.empty[RouteStreamSource]

  def isSourced: Boolean = sourcesMap.nonEmpty

  def bind(streams: RouteStreamMgr): Unit = {
    streamOpt = Some(streams)
  }
  def unbind(): Unit = {
    streamOpt = None
  }

  def subscriptionUpdate(keys: Set[TableRow]): Unit = {
    logger.debug(s"Subscription update for route $route: $keys")
    subscription = keys
    currentOpt.foreach(_.updateSourcing(route, keys))
  }

  //def issueServiceRequest()

  private def sourceAdded(source: RouteStreamSource): Unit = {
    if (currentOpt.isEmpty) {
      currentOpt = Some(source)
      if (subscription.nonEmpty) {
        source.updateSourcing(route, subscription)
      }
    }
  }

  def sourceRegistered(source: RouteStreamSource, details: RouteManifestEntry): Unit = {
    sourcesMap.get(source) match {
      case None =>
        sourcesMap.update(source, details)
        sourceAdded(source)
      case Some(prevEntry) =>
        if (prevEntry != details) {
          sourcesMap.update(source, details)
        }
    }
  }
  def sourceRemoved(source: RouteStreamSource): Unit = {
    sourcesMap -= source
    streamOpt.foreach(_.sourceRemoved(source))
    logger.debug(s"Removed: $source, $currentOpt, $sourcesMap")
    if (currentOpt.contains(source)) {
      if (sourcesMap.isEmpty) {
        currentOpt = None
      } else {
        val (nominated, _) = sourcesMap.minBy {
          case (_, entry) => entry.distance
        }
        currentOpt = Some(nominated)
        if (subscription.nonEmpty) {
          nominated.updateSourcing(route, subscription)
        }
      }
    }
  }
}

class StreamEngine(
    logId: String,
    session: PeerSessionId,
    sourceStrategyFactory: TypeValue => RouteSourcingStrategy) extends LazyLogging {

  private val routeMap = mutable.Map.empty[TypeValue, RouteStreamMgr]
  private val sourcingMap = mutable.Map.empty[TypeValue, RouteSourcingMgr]
  private val sourceToRouteMap = mutable.Map.empty[RouteStreamSource, Map[TypeValue, RouteManifestEntry]]
  private val targetToRouteMap = mutable.Map.empty[StreamTarget, Map[TypeValue, RouteObservers]]
  private val selfRouteMgr = new PeerManifestMgr(session)

  routeMap.update(selfRouteMgr.route, selfRouteMgr.routeManager)
  doManifestUpdate()

  private def doManifestUpdate(): Unit = {
    val result = mutable.Map.empty[TypeValue, RouteManifestEntry]
    sourceToRouteMap.values.foreach { sourceEntry =>
      sourceEntry.foreach {
        case (route, entry) =>
          result.get(route) match {
            case None => result.update(route, RouteManifestEntry(entry.distance))
            case Some(prev) =>
              val dist = entry.distance
              if (prev.distance < dist) {
                result.update(route, RouteManifestEntry(dist))
              }
          }
      }
    }
    val manifest = result.toMap
    logger.debug(s"$logId did manifest update: " + manifest)
    selfRouteMgr.update(manifest)
  }

  private def routeSourceAdded(source: RouteStreamSource, route: TypeValue, manifest: RouteManifestEntry): Unit = {
    val mgr = new RouteSourcingMgr(route)
    mgr.sourceRegistered(source, manifest)
    sourcingMap.update(route, mgr)
    routeMap.get(route).foreach { streamMgr =>
      streamMgr.sourced(mgr)
      mgr.bind(streamMgr)
    }
  }

  private def routeSourceRemoved(source: RouteStreamSource, route: TypeValue): Unit = {
    sourcingMap.get(route).foreach { mgr =>
      mgr.sourceRemoved(source)
      if (!mgr.isSourced) {
        sourcingMap -= route
        routeMap.get(route).foreach(_.unsourced())
      }
    }
  }

  def sourceUpdate(source: RouteStreamSource, update: SourceEvents): Unit = {
    logger.debug(s"$logId source updates $source : $update")
    update.routeUpdatesOpt.foreach { routesUpdate =>
      val prev = sourceToRouteMap.getOrElse(source, Map())
      sourceToRouteMap.update(source, routesUpdate)
      val removes = prev.keySet -- routesUpdate.keySet

      removes.foreach(route => routeSourceRemoved(source, route))

      routesUpdate.foreach {
        case (route, manifest) =>
          sourcingMap.get(route) match {
            case None => routeSourceAdded(source, route, manifest)
            case Some(mgr) => mgr.sourceRegistered(source, manifest)
          }
      }

      doManifestUpdate()
    }

    update.events.foreach {
      case ev: RowAppendEvent => routeMap.get(ev.rowId.routingKey).foreach(_.events(source, Seq(ev))) // TODO: fix this seq
      //case ev: RowResolvedAbsent => routeMap.get(ev.rowId.routingKey).foreach(_.events(source, Seq(ev))) // TODO: fix this seq
      case ev: RouteUnresolved => routeMap.get(ev.routingKey).foreach(_.events(source, Seq(ev)))
    }

    // TODO: flush
  }

  def sourceRemoved(source: RouteStreamSource): Unit = {
    logger.debug(s"$logId source removed $source")
    sourceToRouteMap.get(source).foreach { manifest =>
      manifest.keys.foreach(route => routeSourceRemoved(source, route))
    }
    sourceToRouteMap -= source
    doManifestUpdate()

    // TODO: flush
  }

  private def routeAdded(route: TypeValue): RouteStreams = {
    logger.trace(s"$logId route added $route")
    val routeStream = new RouteStreams(route, _ => new SynthesizedKeyStream[RouteStreamSource])
    sourcingMap.get(route).foreach { src =>
      routeStream.sourced(src)
      src.bind(routeStream)
    }
    routeStream
  }

  private def routeRemoved(route: TypeValue, routeStreams: RouteStreamMgr): Unit = {
    routeMap -= route
    sourcingMap.get(route).foreach(_.unbind())
  }

  def targetSubscriptionUpdate(target: StreamTarget, subscription: Map[TypeValue, RouteObservers]): Unit = {
    logger.debug(s"$logId target added $subscription")
    val prev = targetToRouteMap.getOrElse(target, Map())

    val removed = prev.keySet -- subscription.keySet
    removed.foreach { route =>
      prev.get(route).foreach { entry =>
        routeMap.get(route).foreach { routeStreams =>
          routeStreams.targetRemoved(entry.streamObserver)
          if (!routeStreams.targeted()) {
            routeRemoved(route, routeStreams)
          }
        }
      }
    }
    subscription.foreach {
      case (route, observers) =>
        val routeStreams = routeMap.getOrElseUpdate(route, routeAdded(route))
        // TODO: add all current sources
        routeStreams.targetUpdate(observers.streamObserver, observers.rowObserverMap)
    }
    targetToRouteMap.update(target, subscription)

    // TODO: flush?
    //target.flush()
  }
  def targetRemoved(target: StreamTarget): Unit = {
    logger.debug(s"$logId target removed $target")
    val prev = targetToRouteMap.getOrElse(target, Map())
    prev.foreach {
      case (route, obs) =>
        routeMap.get(route).foreach { routeStreams =>
          routeStreams.targetRemoved(obs.streamObserver)
          if (!routeStreams.targeted()) {
            routeRemoved(route, routeStreams)
          }
        }
    }
    targetToRouteMap -= target
  }
}

trait StreamTargetSubject[A <: RouteTargetSubject] {

  protected val routeMap = mutable.Map.empty[TypeValue, A]
  private val targetToRouteMap = mutable.Map.empty[StreamTarget, Map[TypeValue, RouteObservers]]

  protected def buildRouteManager(route: TypeValue): A

  def targetSubscriptionUpdate(target: StreamTarget, subscription: Map[TypeValue, RouteObservers]): Unit = {
    val prev = targetToRouteMap.getOrElse(target, Map())

    val removed = prev.keySet -- subscription.keySet
    removed.foreach { route =>
      prev.get(route).foreach { entry =>
        routeMap.get(route).foreach { routeStreams =>
          routeStreams.targetRemoved(entry.streamObserver)
          if (!routeStreams.targeted()) {
            routeMap -= route
          }
        }
      }
    }
    subscription.foreach {
      case (route, observers) =>
        val routeStreams = routeMap.getOrElseUpdate(route, buildRouteManager(route))
        routeStreams.targetUpdate(observers.streamObserver, observers.rowObserverMap)
    }
    targetToRouteMap.update(target, subscription)

    // TODO: flush?
    //target.flush()
  }
  def targetRemoved(target: StreamTarget): Unit = {
    val prev = targetToRouteMap.getOrElse(target, Map())
    prev.foreach {
      case (route, obs) =>
        routeMap.get(route).foreach { routeStreams =>
          routeStreams.targetRemoved(obs.streamObserver)
          if (!routeStreams.targeted()) {
            routeMap -= route
          }
        }
    }
    targetToRouteMap -= target
  }
}

trait StreamObserver {
  def handle(routeEvent: StreamEvent): Unit
}

trait RouteStreamSource extends RouteServiceProvider {
  def updateSourcing(route: TypeValue, rows: Set[TableRow]): Unit
}

