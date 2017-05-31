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
package io.greenbus.edge.stream.gateway2

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow.Sink
import io.greenbus.edge.stream.engine2.{ CachingKeyStreamSubject, RouteTargetSubjectBasic, StreamObserverSet, StreamTargetSubject }
import io.greenbus.edge.stream.filter.{ StreamCache, StreamCacheImpl }
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.gateway.RouteServiceRequest

trait DynamicTable {
  def subscribed(key: TypeValue): Unit
  def unsubscribed(key: TypeValue): Unit
}

case class AppendPublish(key: TableRow, values: Seq[TypeValue])
case class SetPublish(key: TableRow, value: Set[TypeValue])
case class MapPublish(key: TableRow, value: Map[TypeValue, TypeValue])
case class PublishBatch(
  appendUpdates: Seq[AppendPublish],
  mapUpdates: Seq[MapPublish],
  setUpdates: Seq[SetPublish])

case class RoutePublishConfig(
  appendKeys: Seq[(TableRow, SequenceCtx)],
  setKeys: Seq[(TableRow, SequenceCtx)],
  mapKeys: Seq[(TableRow, SequenceCtx)],
  dynamicTables: Seq[(String, DynamicTable)],
  handler: Sink[RouteServiceRequest])

class ProducerStreamSubject extends CachingKeyStreamSubject with LazyLogging {
  private var streamOpt = Option.empty[StreamCache]

  protected def sync(): Seq[AppendEvent] = {
    logger.debug(s"ProducerStreamSubject sync()")
    streamOpt.map(_.resync()).getOrElse {
      logger.debug(s"No sync event on subscribe")
      Seq()
    } // TODO: row absent?
  }

  def bind(cache: StreamCache): Unit = {
    streamOpt = Some(cache)
  }
  def unbind(): Unit = {
    streamOpt = None
    // TODO: row absent?
  }

  def targeted(): Boolean = {
    observers.nonEmpty /*|| streamOpt.nonEmpty*/
  }
}

class ObservableProducerStream[A](sequencer: A => Seq[AppendEvent]) /* extends CachingKeyStreamSubject */ {

  private var observerOpt = Option.empty[StreamObserverSet]

  protected val streamCache = new StreamCacheImpl

  def cache: StreamCache = streamCache

  def handle(obj: A): Unit = {
    val sequenced = sequencer(obj)
    sequenced.foreach(streamCache.handle)
    observerOpt.foreach { set =>
      set.observers.foreach { observer =>
        sequenced.foreach(observer.handle)
      }
    }
  }

  def bind(set: StreamObserverSet): Unit = {
    observerOpt = Some(set)
  }

  def unbind(): Unit = {
    observerOpt = None
  }
}

object RoutePublisher {
  def build(config: RoutePublishConfig): RoutePublisher = {
    val uuid = UUID.randomUUID()
    //config.appendKeys
    ???
  }
}
class RoutePublisher(
    val appends: Map[TableRow, ObservableProducerStream[Seq[TypeValue]]],
    val sets: Map[TableRow, ObservableProducerStream[Set[TypeValue]]],
    val maps: Map[TableRow, ObservableProducerStream[Map[TypeValue, TypeValue]]]) {

  def allRows: Set[TableRow] = appends.keySet ++ sets.keySet ++ maps.keySet

  def handleBatch(batch: PublishBatch): Unit = {
    batch.appendUpdates.foreach { update =>
      appends.get(update.key)
        .foreach(_.handle(update.values))
    }
    batch.setUpdates.foreach { update =>
      sets.get(update.key)
        .foreach(_.handle(update.value))
    }
    batch.mapUpdates.foreach { update =>
      maps.get(update.key)
        .foreach(_.handle(update.value))
    }
  }
}

/*


 */

// TODO: the whole keep-it-alive thing is pointless when the "container" doesn't matter anyway
class ProducerRouteManager(route: TypeValue) extends RouteTargetSubjectBasic[ProducerStreamSubject] {

  private var currentlyBound = Set.empty[TableRow]

  def batch(publishBatch: PublishBatch): Unit = {

  }

  def bindPublisher(publisher: RoutePublisher): Unit = {

    val next = publisher.allRows
    val removed = currentlyBound -- next
    currentlyBound = next

    def bindStream(row: TableRow, stream: ObservableProducerStream[_]): Unit = {
      val bindable = streamMap.getOrElseUpdate(row, streamFactory(row))
      bindable.bind(stream.cache)
      stream.bind(bindable)
    }

    publisher.appends.foreach { case (row, stream) => bindStream(row, stream) }

    removed.foreach(row => streamMap.get(row).foreach(_.unbind()))
  }

  def unbind(): Unit = {
    streamMap.foreach(_._2.unbind())
  }

  protected def streamFactory(key: TableRow): ProducerStreamSubject = {
    new ProducerStreamSubject()
  }
}

object ProducerManager {

}
class ProducerManager extends StreamTargetSubject[ProducerRouteManager] {

  protected def buildRouteManager(route: TypeValue): ProducerRouteManager = {
    new ProducerRouteManager(route)
  }

  def routePublished(route: TypeValue, config: RoutePublishConfig): Unit = {

    val mgr = routeMap.getOrElseUpdate(route, buildRouteManager(route))

    /*val contained = publishedRoutes.contains(route)
    publishedRoutes.update(route, PublisherRouteMgr.build(route, config))
    if (!contained) {
      handle.events(Some(publishedRoutes.keySet.toSet), Seq())
    }*/
  }
  def routeUnpublished(route: TypeValue): Unit = {
    /* val contained = publishedRoutes.contains(route)
     publishedRoutes -= route
     if (contained) {
       handle.events(Some(publishedRoutes.keySet.toSet), Seq())
     }*/
  }
  def routeBatch(route: TypeValue, batch: PublishBatch): Unit = {
    /*publishedRoutes.get(route) match {
      case None => logger.warn(s"Batch for unpublished route: $route")
      case Some(mgr) =>
        val events = mgr.handleBatch(batch)
        handle.events(None, events)
    }*/

  }

}