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
package io.greenbus.edge.stream.gateway

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.stream.TypeValue
import io.greenbus.edge.stream.engine.{ RouteObservers, RouteTargetSubject, StreamTarget }

import scala.collection.mutable

trait StreamTargetSubject[A <: RouteTargetSubject] extends LazyLogging {

  protected val routeMap: mutable.Map[TypeValue, A]
  private val targetToRouteMap = mutable.Map.empty[StreamTarget, Map[TypeValue, RouteObservers]]

  protected def buildRouteManager(route: TypeValue): A

  def targetSubscriptionUpdate(target: StreamTarget, subscription: Map[TypeValue, RouteObservers]): Unit = {
    logger.debug(s"targetSubscriptionUpdate: $subscription")
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
        logger.debug(s"sub for route: $route, obs: $observers")
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
