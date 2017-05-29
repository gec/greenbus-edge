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
import io.greenbus.edge.stream.{ RouteManifestEntry, ServiceRequest, TableRow, TypeValue }

import scala.collection.mutable

class RouteSourcingMgr(route: TypeValue) extends RouteServiceProvider with LazyLogging {

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

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {
    currentOpt.foreach(_.issueServiceRequests(requests))
  }

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
