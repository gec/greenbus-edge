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
  def flush(): Unit
}

/*

def targetUpdate(target: StreamObserver, subscription: Map[TableRow, KeyStreamObserver]): Unit = {
 */

trait RouteSourcing {
  //def observeForDelivery(events: Seq[StreamEvent]): Unit
  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit
}

/*

Source mgr
<- register observers
-> notify flush
Target mgr

 */

trait SourceManager {
  def flushNotifications: Source[Set[TypeValue]]
  def registerTargetObservers(target: StreamTarget, subscription: Map[TypeValue, RouteObservers]): Unit
  def targetRemoved(target: StreamTarget): Unit
}

class StreamEngine(
    logId: String,
    session: PeerSessionId,
    sourceStrategyFactory: TypeValue => RouteSourcingStrategy) extends LazyLogging {

  private val routeMap = mutable.Map.empty[TypeValue, RouteStreamMgr]
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

  def sourceUpdate(source: RouteStreamSource, update: SourceEvents): Unit = {
    logger.debug(s"Source updates $source : $update")
    update.routeUpdatesOpt.foreach { routesUpdate =>
      val prev = sourceToRouteMap.getOrElse(source, Map())
      sourceToRouteMap.update(source, routesUpdate)
      val removes = prev.keySet -- routesUpdate.keySet
      removes.foreach(route => routeMap.get(route).foreach(_.sourceRemoved(source)))
      routesUpdate.foreach {
        case (route, details) => routeMap.get(route).foreach(_.sourceAdded(source, details))
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
    logger.debug(s"Source removed $source")
    sourceToRouteMap.get(source).foreach { routes =>
      routes.keys.foreach { route => routeMap.get(route).foreach(_.sourceRemoved(source)) }
    }
    sourceToRouteMap -= source
    doManifestUpdate()

    // TODO: flush
  }

  private def routeAdded(route: TypeValue): RouteStreams = {
    logger.trace(s"Route added $route")
    val routeStream = new RouteStreams(route, sourceStrategyFactory(route), _ => new SynthesizedKeyStream[RouteStreamSource])
    sourceToRouteMap.foreach {
      case (source, map) => map.find(_._1 == route).foreach {
        case (_, details) =>
          routeStream.sourceAdded(source, details)
      }
    }
    routeStream
  }

  def targetSubscriptionUpdate(target: StreamTarget, subscription: Map[TypeValue, RouteObservers]): Unit = {
    logger.debug(s"Target added $subscription")
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
        val routeStreams = routeMap.getOrElseUpdate(route, routeAdded(route))
        // TODO: add all current sources
        routeStreams.targetUpdate(observers.streamObserver, observers.rowObserverMap)
    }
    targetToRouteMap.update(target, subscription)

    // TODO: flush?
    //target.flush()
  }
  def targetRemoved(target: StreamTarget): Unit = {
    logger.debug(s"Target removed $target")
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

trait RouteStreamSource {
  def updateSourcing(route: TypeValue, rows: Set[TableRow]): Unit
}

