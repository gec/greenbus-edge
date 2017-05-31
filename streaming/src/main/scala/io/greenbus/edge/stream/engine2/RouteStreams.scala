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
import io.greenbus.edge.stream.filter.StreamCacheImpl
import io.greenbus.edge.stream.gateway2.MapSequencer

import scala.collection.mutable

trait RouteTargetSubject {

  def targeted(): Boolean

  def targetUpdate(target: StreamObserver, subscription: Map[TableRow, KeyStreamObserver]): Unit

  def targetRemoved(target: StreamObserver): Unit
}

trait RouteTargetSubjectBasic[A <: KeyStreamSubject] extends RouteTargetSubject {
  protected val streamMap = mutable.Map.empty[TableRow, A]
  private val subscriptionMap = mutable.Map.empty[StreamObserver, Map[TableRow, KeyStreamObserver]]

  protected def streamFactory(key: TableRow): A
  protected def streamUpdate(update: Set[TableRow]): Unit = {}

  def targeted(): Boolean = {
    streamMap.keySet.nonEmpty
  }

  def targetUpdate(target: StreamObserver, subscription: Map[TableRow, KeyStreamObserver]): Unit = {
    val previous = subscriptionMap.getOrElse(target, Map())
    val removes = previous.keySet -- subscription.keySet

    removes.flatMap(row => previous.get(row).map(obs => (row, obs))).foreach {
      case (key, obs) => streamMap.get(key).foreach(_.targetRemoved(obs))
    }

    subscription.foreach {
      case (key, obs) =>
        streamMap.get(key) match {
          case None => {
            val stream = streamFactory(key)
            streamMap.update(key, stream)
            stream.targetAdded(obs)
          }
          case Some(stream) => stream.targetAdded(obs)
        }
    }

    val untargetedStreams = removes.filter(key => streamMap.get(key).exists(!_.targeted()))
    streamMap --= untargetedStreams

    streamUpdate(streamMap.keySet.toSet)
  }

  def targetRemoved(target: StreamObserver): Unit = {
    val previous = subscriptionMap.getOrElse(target, Map())

    val untargetedStreams = previous.flatMap {
      case (key, obs) =>
        streamMap.get(key).flatMap { stream =>
          stream.targetRemoved(obs)
          if (stream.targeted()) {
            None
          } else {
            Some(key)
          }
        }
    }

    streamMap --= untargetedStreams
    streamUpdate(streamMap.keySet.toSet)
  }
}

trait RouteStreamMgr extends RouteTargetSubject {
  def events(source: RouteStreamSource, events: Seq[StreamEvent]): Unit
  def sourceRemoved(source: RouteStreamSource): Unit
  def sourced(sourcing: RouteSourcingMgr): Unit
  def unsourced(): Unit
}

class RouteStreams(route: TypeValue, streamFactory: TableRow => KeyStream[RouteStreamSource]) extends RouteStreamMgr with LazyLogging {

  private val streamMap = mutable.Map.empty[TableRow, KeyStream[RouteStreamSource]]
  private val subscriptionMap = mutable.Map.empty[StreamObserver, Map[TableRow, KeyStreamObserver]]
  private var sourcingOpt = Option.empty[RouteSourcingMgr]

  def events(source: RouteStreamSource, events: Seq[StreamEvent]): Unit = {
    events.foreach {
      case append: RowAppendEvent => {
        streamMap.get(append.rowId.tableRow).foreach(_.handle(source, append.appendEvent))
      }
      /*case absent: RowResolvedAbsent => {
        // ???
      }*/
      case un: RouteUnresolved => {
        streamMap.values.foreach(_.sourceRemoved(source))
      }
    }
  }

  def sourced(sourcing: RouteSourcingMgr): Unit = {
    sourcingOpt = Some(sourcing)
    if (streamMap.nonEmpty) {
      sourcing.subscriptionUpdate(streamMap.keySet.toSet)
    }
  }
  def unsourced(): Unit = {
    sourcingOpt = None
    subscriptionMap.keys.foreach(_.handle(RouteUnresolved(route)))
  }

  def sourceRemoved(source: RouteStreamSource): Unit = {
    streamMap.values.foreach(_.sourceRemoved(source))
  }

  def targeted(): Boolean = {
    streamMap.keySet.nonEmpty
  }

  def targetUpdate(target: StreamObserver, subscription: Map[TableRow, KeyStreamObserver]): Unit = {
    logger.debug(s"targetUpdate: $target, sub: $subscription")
    val previous = subscriptionMap.getOrElse(target, Map())
    subscriptionMap.update(target, subscription)
    val removes = previous.keySet -- subscription.keySet

    removes.flatMap(row => previous.get(row).map(obs => (row, obs))).foreach {
      case (key, obs) => streamMap.get(key).foreach(_.targetRemoved(obs))
    }

    subscription.foreach {
      case (key, obs) =>
        streamMap.get(key) match {
          case None => {
            val stream = streamFactory(key)
            streamMap.update(key, stream)
            stream.targetAdded(obs)
          }
          case Some(stream) => stream.targetAdded(obs)
        }
    }

    val untargetedStreams = removes.filter(key => streamMap.get(key).exists(!_.targeted()))
    streamMap --= untargetedStreams

    val activeKeys = streamMap.keySet.toSet

    sourcingOpt.foreach(_.subscriptionUpdate(activeKeys))
    logger.debug(s"sourcing: $sourcingOpt")
    if (sourcingOpt.isEmpty) {
      target.handle(RouteUnresolved(route))
    }

  }

  def targetRemoved(target: StreamObserver): Unit = {
    val previous = subscriptionMap.getOrElse(target, Map())

    val untargetedStreams = previous.flatMap {
      case (key, obs) =>
        streamMap.get(key).flatMap { stream =>
          stream.targetRemoved(obs)
          if (stream.targeted()) {
            None
          } else {
            Some(key)
          }
        }
    }

    streamMap --= untargetedStreams

    val activeKeys = streamMap.keySet.toSet
    sourcingOpt.foreach(_.subscriptionUpdate(activeKeys))
  }
}
