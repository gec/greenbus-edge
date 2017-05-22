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
import io.greenbus.edge.stream._

import scala.collection.mutable

trait RouteSourcingStrategy {
  def subscriptionUpdate(keys: Set[TableRow]): Unit
  def sourceAdded(source: RouteStreamSource, details: RouteManifestEntry): Unit
  def sourceRemoved(source: RouteStreamSource): Seq[StreamEvent]
}

class SingleSubscribeSourcingStrategy(route: TypeValue) extends RouteSourcingStrategy with LazyLogging {

  private var subscription = Set.empty[TableRow]
  private var currentOpt = Option.empty[RouteStreamSource]
  private var sourcesMap = mutable.Map.empty[RouteStreamSource, RouteManifestEntry]

  def subscriptionUpdate(keys: Set[TableRow]): Unit = {
    subscription = keys
    if (subscription.nonEmpty) {
      currentOpt.foreach(_.updateSourcing(route, keys))
    }
  }

  def sourceAdded(source: RouteStreamSource, details: RouteManifestEntry): Unit = {
    if (currentOpt.isEmpty) {
      currentOpt = Some(source)
      if (subscription.nonEmpty) {
        source.updateSourcing(route, subscription)
      }
    }
    sourcesMap.update(source, details)
  }

  def sourceRemoved(source: RouteStreamSource): Seq[StreamEvent] = {
    sourcesMap -= source
    if (currentOpt.contains(source)) {
      if (sourcesMap.isEmpty) {
        Seq(RouteUnresolved(route))
      } else {
        val (nominated, _) = sourcesMap.minBy {
          case (_, entry) => entry.distance
        }
        currentOpt = Some(nominated)
        if (subscription.nonEmpty) {
          nominated.updateSourcing(route, subscription)
        }
        Seq()
      }
    } else {
      Seq()
    }
  }
}