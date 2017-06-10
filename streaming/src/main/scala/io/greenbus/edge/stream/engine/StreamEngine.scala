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
package io.greenbus.edge.stream.engine

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.stream._

import scala.collection.mutable

case class SourceEvents(routeUpdatesOpt: Option[Map[TypeValue, RouteManifestEntry]], events: Seq[StreamEvent])

trait StreamObserver {
  def handle(routeEvent: StreamEvent): Unit
}

trait RouteStreamSource extends RouteServiceProvider {
  def updateSourcing(route: TypeValue, rows: Set[TableRow]): Unit
}

trait StreamTarget {
  def flush(): Unit
}

trait RouteServiceProvider {
  //def observeForDelivery(events: Seq[StreamEvent]): Unit
  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit
}

class StreamEngine(
    logId: String,
    session: PeerSessionId,
    appendLimitDefault: Int) extends LazyLogging {

  private val streamMap = mutable.Map.empty[TypeValue, RouteStreamMgr]
  private val sourcingMap = mutable.Map.empty[TypeValue, RouteSourcingMgr]
  private val sourceToRouteMap = mutable.Map.empty[RouteStreamSource, Map[TypeValue, RouteManifestEntry]]
  private val targetToRouteMap = mutable.Map.empty[StreamTarget, Map[TypeValue, RouteObservers]]
  private val selfRouteMgr = new PeerManifestMgr(session)

  streamMap.update(selfRouteMgr.route, selfRouteMgr.routeManager)
  doManifestUpdate()

  def getSourcing(route: TypeValue): Option[RouteServiceProvider] = {
    sourcingMap.get(route)
  }

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
    logger.debug(s"$logId route source added: $route - $source")
    val mgr = new RouteSourcingMgr(route)
    mgr.sourceRegistered(source, manifest)
    sourcingMap.update(route, mgr)
    streamMap.get(route).foreach { streamMgr =>
      streamMgr.sourced(mgr)
      mgr.bind(streamMgr)
    }
  }

  private def routeSourceRemoved(source: RouteStreamSource, route: TypeValue): Unit = {
    logger.debug(s"$logId route source removed: $route - $source")
    sourcingMap.get(route).foreach { mgr =>
      mgr.sourceRemoved(source)
      if (!mgr.isSourced) {
        sourcingMap -= route
        streamMap.get(route).foreach(_.unsourced())
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
      case ev: RowAppendEvent => streamMap.get(ev.rowId.routingKey).foreach(_.events(source, Seq(ev))) // TODO: fix this seq
      //case ev: RowResolvedAbsent => routeMap.get(ev.rowId.routingKey).foreach(_.events(source, Seq(ev))) // TODO: fix this seq
      case ev: RouteUnresolved => streamMap.get(ev.routingKey).foreach(_.events(source, Seq(ev)))
    }
  }

  def sourceRemoved(source: RouteStreamSource): Unit = {
    logger.debug(s"$logId source removed $source")
    sourceToRouteMap.get(source).foreach { manifest =>
      manifest.keys.foreach(route => routeSourceRemoved(source, route))
    }
    sourceToRouteMap -= source
    doManifestUpdate()
  }

  private def routeAdded(route: TypeValue): RouteStreams = {
    logger.trace(s"$logId route added $route")
    val routeStream = new RouteStreams(route, _ => new SynthesizedKeyStream[RouteStreamSource](appendLimitDefault))
    sourcingMap.get(route).foreach { src =>
      routeStream.sourced(src)
      src.bind(routeStream)
    }
    routeStream
  }

  private def routeRemoved(route: TypeValue, routeStreams: RouteStreamMgr): Unit = {
    streamMap -= route
    sourcingMap.get(route).foreach(_.unbind())
  }

  def targetSubscriptionUpdate(target: StreamTarget, subscription: Map[TypeValue, RouteObservers]): Unit = {
    logger.debug(s"$logId target added $subscription")
    val prev = targetToRouteMap.getOrElse(target, Map())
    targetToRouteMap.update(target, subscription)

    val removed = prev.keySet -- subscription.keySet
    removed.foreach { route =>
      prev.get(route).foreach { entry =>
        streamMap.get(route).foreach { routeStreams =>
          routeStreams.targetRemoved(entry.streamObserver)
          if (!routeStreams.targeted()) {
            routeRemoved(route, routeStreams)
          }
        }
      }
    }
    subscription.foreach {
      case (route, observers) =>
        val routeStreams = streamMap.getOrElseUpdate(route, routeAdded(route))
        // TODO: add all current sources
        routeStreams.targetUpdate(observers.streamObserver, observers.rowObserverMap)
    }
  }
  def targetRemoved(target: StreamTarget): Unit = {
    logger.debug(s"$logId target removed $target")
    val prev = targetToRouteMap.getOrElse(target, Map())
    prev.foreach {
      case (route, obs) =>
        streamMap.get(route).foreach { routeStreams =>
          routeStreams.targetRemoved(obs.streamObserver)
          if (!routeStreams.targeted()) {
            routeRemoved(route, routeStreams)
          }
        }
    }
    targetToRouteMap -= target
  }
}
