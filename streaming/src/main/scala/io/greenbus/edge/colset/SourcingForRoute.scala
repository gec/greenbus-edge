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
import io.greenbus.edge.colset.old.{RouteUnresolved, RowAppendEvent, StreamEvent}

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable

trait ServiceRouteProvider {
  def serviceTargets: Seq[ManagedRouteSource]
}

class SourcingForRoute(route: TypeValue) extends ServiceRouteProvider with LazyLogging {

  private var subscribersToRows = BiMultiMap.empty[SubscriptionTarget, TableRow]
  private def subscribedRows: Set[TableRow] = subscribersToRows.valToKey.keySet

  private var currentSource = Option.empty[ManagedRouteSource]
  private var standbySources = Set.empty[ManagedRouteSource]
  private var manifestMap = Map.empty[ManagedRouteSource, RouteManifestEntry]

  def sourceMap: Map[ManagedRouteSource, RouteManifestEntry] = manifestMap

  def resolved(): Boolean = currentSource.nonEmpty
  def unused(): Boolean = currentSource.isEmpty && subscribersToRows.keyToVal.keySet.isEmpty

  def serviceTargets: Seq[ManagedRouteSource] = {
    currentSource.map(src => Seq(src)).getOrElse(Seq())
  }

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

  def sourceAdded(source: ManagedRouteSource, desc: RouteManifestEntry): Unit = {
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

  def sourceRemoved(source: ManagedRouteSource): Option[RouteUnresolved] = {
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