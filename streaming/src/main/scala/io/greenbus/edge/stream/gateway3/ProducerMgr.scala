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
package io.greenbus.edge.stream.gateway3

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow.Sink
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.engine2._
import io.greenbus.edge.stream.filter.{ StreamCache, StreamCacheImpl }
import io.greenbus.edge.stream.gateway.RouteServiceRequest
import io.greenbus.edge.stream.gateway2._

import scala.collection.mutable

sealed trait ProducerDataUpdate
case class AppendProducerUpdate(values: Seq[TypeValue]) extends ProducerDataUpdate
case class SetProducerUpdate(value: Set[TypeValue]) extends ProducerDataUpdate
case class MapProducerUpdate(value: Map[TypeValue, TypeValue]) extends ProducerDataUpdate

sealed trait ProducerKeyEvent
case class AddRow(key: TableRow, ctx: SequenceCtx) extends ProducerKeyEvent
case class RowUpdate(key: TableRow, update: ProducerDataUpdate) extends ProducerKeyEvent
case class DrowRow(key: TableRow) extends ProducerKeyEvent

class ProducerUpdateStream(sessionId: PeerSessionId, ctx: SequenceCtx) {

  private var sequencerOpt = Option.empty[UpdateSequencer]
  private var observerOpt = Option.empty[StreamObserverSet]

  protected val streamCache = new StreamCacheImpl

  def cache: StreamCache = streamCache

  def handle(update: ProducerDataUpdate): Unit = {

    val sequencer = sequencerOpt.getOrElse {
      val seq = UpdateSequencer.build(update, sessionId, ctx)
      sequencerOpt = Some(seq)
      seq
    }

    val sequenced = sequencer.handle(update)
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

object UpdateSequencer {

  def build(update: ProducerDataUpdate, session: PeerSessionId, ctx: SequenceCtx): UpdateSequencer = {
    update match {
      case _: AppendProducerUpdate => new AppendUpdateSequencer(session, ctx)
      case _: SetProducerUpdate => new SetUpdateSequencer(session, ctx)
      case _: MapProducerUpdate => new MapUpdateSequencer(session, ctx)
    }
  }
}
trait UpdateSequencer {
  def handle(update: ProducerDataUpdate): Seq[AppendEvent]
}

class AppendUpdateSequencer(session: PeerSessionId, ctx: SequenceCtx) extends UpdateSequencer with LazyLogging {

  private val sequencer = new AppendSequencer(session, ctx)

  def handle(update: ProducerDataUpdate): Seq[AppendEvent] = {
    update match {
      case AppendProducerUpdate(values) => sequencer.handle(values)
      case _ =>
        logger.warn(s"Append sequencer got incorrect update type: $update")
        Seq()
    }
  }
}
class SetUpdateSequencer(session: PeerSessionId, ctx: SequenceCtx) extends UpdateSequencer with LazyLogging {

  private val sequencer = new SetSequencer(session, ctx)

  def handle(update: ProducerDataUpdate): Seq[AppendEvent] = {
    update match {
      case SetProducerUpdate(values) => sequencer.handle(values)
      case _ =>
        logger.warn(s"Append sequencer got incorrect update type: $update")
        Seq()
    }
  }
}
class MapUpdateSequencer(session: PeerSessionId, ctx: SequenceCtx) extends UpdateSequencer with LazyLogging {

  private val sequencer = new MapSequencer(session, ctx)

  def handle(update: ProducerDataUpdate): Seq[AppendEvent] = {
    update match {
      case MapProducerUpdate(values) => sequencer.handle(values)
      case _ =>
        logger.warn(s"Append sequencer got incorrect update type: $update")
        Seq()
    }
  }
}

sealed trait ProducerEvent

case class RouteBatchEvent(route: TypeValue, events: Seq[ProducerKeyEvent]) extends ProducerEvent

case class RouteBindEvent(route: TypeValue,
  initialEvents: Seq[ProducerKeyEvent],
  dynamic: Map[String, DynamicTable],
  handler: Sink[RouteServiceRequest]) extends ProducerEvent

case class RouteUnbindEvent(route: TypeValue) extends ProducerEvent

class ProducerRouteStreamMgr(route: TypeValue, dynamic: Map[String, DynamicTable]) {

}

class ProducerRouteMgr extends RouteTargetSubject {

  private var produced = false
  private val updateMap = mutable.Map.empty[TableRow, ProducerUpdateStream]
  private var dynamicTableMap = Map.empty[String, DynamicTable]

  private val subjectMap = mutable.Map.empty[TableRow, ProducerStreamSubject]
  private val subscriptionMap = mutable.Map.empty[StreamObserver, Map[TableRow, KeyStreamObserver]]

  def bind(events: Seq[ProducerKeyEvent], dynamic: Map[String, DynamicTable]): Unit = {
    unbind()
    bindTables(dynamic)
    events.foreach(handleEvent)
    produced = true
  }

  def batch(events: Seq[ProducerKeyEvent]): Unit = {
    events.foreach(handleEvent)
  }

  def unbind(): Unit = {
    updateMap.clear()
    dynamicTableMap = Map()
    subjectMap.foreach { case (row, subj) => subj.unbind() }
    produced = false
  }

  private def bindTables(dynamic: Map[String, DynamicTable]): Unit = {
    dynamicTableMap = dynamic

    val tableSubjects: Map[String, collection.Map[TableRow, ProducerStreamSubject]] =
      subjectMap.filterKeys(row => dynamicTableMap.keySet.contains(row.table))
        .groupBy(_._1.table)

    tableSubjects.foreach {
      case (table, rowMap) =>
        dynamic.get(table).foreach { t =>
          rowMap.keys.foreach(row => t.subscribed(row.rowKey))
        }
    }
  }

  private def handleEvent(ev: ProducerKeyEvent): Unit = {
    ev match {
      case AddRow(key, ctx) => {
        //updateMap.get(key).foreach { stream => } // TODO: remove row?
        val stream = new ProducerUpdateStream(PeerSessionId(UUID.randomUUID(), 0), ctx)
        streamAdded(key, stream)
      }
      case RowUpdate(key, update) => {
        updateMap.get(key).foreach(_.handle(update))
      }
      case DrowRow(key) =>
        updateMap.get(key).foreach(s => streamRemoved(key, s))
    }
  }

  private def streamAdded(key: TableRow, stream: ProducerUpdateStream): Unit = {
    updateMap.update(key, stream)
    subjectMap.get(key).foreach(subj => subj.bind(stream.cache))
  }
  private def streamRemoved(key: TableRow, stream: ProducerUpdateStream): Unit = {
    updateMap -= key
    subjectMap.get(key).foreach(subj => subj.unbind())
  }

  private def observerAdded(key: TableRow, observer: KeyStreamObserver): Unit = {
    subjectMap.get(key) match {
      case None =>
        dynamicTableMap.get(key.table) match {
          case None => staticSubjectAdded(key, observer)
          case Some(table) => dynamicSubjectAdded(key, table, observer)
        }
      case Some(subject) =>
        subject.targetAdded(observer)
    }
  }
  private def observerRemoved(key: TableRow, observer: KeyStreamObserver): Unit = {
    subjectMap.get(key).foreach { subject =>
      subject.targetRemoved(observer)
      if (!subject.targeted()) {
        dynamicTableMap.get(key.table) match {
          case None => staticSubjectRemoved(key, subject)
          case Some(table) => dynamicSubjectRemoved(key, table, subject)
        }
      }
    }
  }

  private def buildAndBindSubject(key: TableRow): ProducerStreamSubject = {
    val subject = new ProducerStreamSubject
    updateMap.get(key).foreach { producer =>
      subject.bind(producer.cache)
      producer.bind(subject)
    }
    subject
  }

  private def staticSubjectAdded(key: TableRow, observer: KeyStreamObserver): Unit = {
    val subject = buildAndBindSubject(key)
    subject.targetAdded(observer)
  }
  private def dynamicSubjectAdded(key: TableRow, table: DynamicTable, observer: KeyStreamObserver): Unit = {
    val subject = buildAndBindSubject(key)
    subject.targetAdded(observer)
    table.subscribed(key.rowKey)
  }
  private def staticSubjectRemoved(key: TableRow, subject: ProducerStreamSubject): Unit = {
    updateMap.get(key).foreach { stream => stream.unbind() }
    subjectMap -= key
  }
  private def dynamicSubjectRemoved(key: TableRow, table: DynamicTable, subject: ProducerStreamSubject): Unit = {
    updateMap.get(key).foreach { stream => stream.unbind() }
    subjectMap -= key
    table.unsubscribed(key.rowKey)
  }

  def targeted(): Boolean = {
    subjectMap.keySet.nonEmpty || produced
  }

  def targetUpdate(target: StreamObserver, subscription: Map[TableRow, KeyStreamObserver]): Unit = {
    val previous = subscriptionMap.getOrElse(target, Map())
    val removes = previous.keySet -- subscription.keySet

    removes.flatMap(row => previous.get(row).map(obs => (row, obs))).foreach {
      case (key, obs) =>
        observerRemoved(key, obs)
    }

    subscription.foreach {
      case (key, obs) =>
        observerAdded(key, obs)
    }

    val untargetedStreams = removes.filter(key => subjectMap.get(key).exists(!_.targeted()))
    subjectMap --= untargetedStreams

    subscriptionMap.update(target, subscription)
  }

  def targetRemoved(target: StreamObserver): Unit = {
    val previous = subscriptionMap.getOrElse(target, Map())

    previous.foreach {
      case (key, obs) =>
        observerRemoved(key, obs)
    }

    subscriptionMap -= target
  }
}

class ProducerMgr extends StreamTargetSubject2[ProducerRouteMgr] {

  protected val routeMap = mutable.Map.empty[TypeValue, ProducerRouteMgr]

  def handleEvent(event: ProducerEvent): Unit = {
    event match {
      case ev: RouteBindEvent => {
        val mgr = routeMap.getOrElseUpdate(ev.route, new ProducerRouteMgr)
        mgr.bind(ev.initialEvents, ev.dynamic)
      }
      case ev: RouteBatchEvent => {
        routeMap.get(ev.route).foreach(_.batch(ev.events))
      }
      case ev: RouteUnbindEvent =>
        routeMap.get(ev.route).foreach { routeMgr =>
          routeMgr.unbind()
          if (!routeMgr.targeted()) {
            routeMap -= ev.route
          }
        }
    }
  }

  protected def buildRouteManager(route: TypeValue): ProducerRouteMgr = {
    new ProducerRouteMgr
  }
}

trait StreamTargetSubject2[A <: RouteTargetSubject] {

  protected val routeMap: mutable.Map[TypeValue, A]
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

