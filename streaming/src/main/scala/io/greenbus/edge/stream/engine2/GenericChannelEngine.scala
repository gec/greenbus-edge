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

import io.greenbus.edge.flow._
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.consume.ValueUpdateSynthesizerImpl
import io.greenbus.edge.stream.subscribe._
import io.greenbus.edge.util.EitherUtil

import scala.collection.mutable

trait GenericSource {
  def subscriptions: Sink[Set[RowId]]
  def events: Source[SourceEvents]
  def requests: Sink[Seq[ServiceRequest]]
  def responses: Source[Seq[ServiceResponse]]
}

trait GenericSourceChannel extends GenericSource with CloseObservable

trait GenericTarget {
  def subscriptions: Source[Set[RowId]]
  def events: Sink[Seq[StreamEvent]]
  def requests: Source[Seq[ServiceRequest]]
  def responses: Sink[Seq[ServiceResponse]]
}

trait GenericTargetChannel extends GenericTarget with CloseObservable

class RouteStreamSourceImpl(source: GenericSource) extends RouteStreamSource {
  private val routeMap = mutable.Map.empty[TypeValue, Set[TableRow]]

  def updateSourcing(route: TypeValue, rows: Set[TableRow]): Unit = {
    routeMap += (route -> rows)
    pushSnapshot()
  }

  private def pushSnapshot(): Unit = {
    val all = routeMap.flatMap {
      case (routingKey, routeRows) => routeRows.map(_.toRowId(routingKey))
    }
    source.subscriptions.push(all.toSet)
  }
}

class GenericChannelEngine(streamEngine: StreamEngine) {

  def sourceChannel(source: GenericSourceChannel): Unit = {
    val streamSource = new RouteStreamSourceImpl(source)
    source.events.bind(events => streamEngine.sourceUpdate(streamSource, events))
    source.onClose.subscribe(() => sourceChannelClosed(source, streamSource))
  }

  private def sourceChannelClosed(source: GenericSourceChannel, handle: RouteStreamSource): Unit = {
    streamEngine.sourceRemoved(handle)
  }

  def targetChannel(target: GenericTargetChannel): Unit = {

    val queueMgr = new TargetQueueMgr

    val st: StreamTarget = null

    target.subscriptions.bind { rows =>
      val observers = queueMgr.subscriptionUpdate(rows)
      streamEngine.targetSubscriptionUpdate(st, observers)
    }

    target.onClose.subscribe(() => streamEngine.targetRemoved(st))
  }
}

class GatewaySourceChannel(link: GatewayClientProxyChannel) extends GenericSourceChannel {
  def onClose: LatchSubscribable = link.onClose

  def subscriptions: Sink[Set[RowId]] = link.subscriptions

  def events: Source[SourceEvents] = {
    new Source[SourceEvents] {
      def bind(handler: Handler[SourceEvents]): Unit = {
        link.events.bind { events =>
          val routeMapOpt = events.routesUpdate.map { set => set.map(route => (route, RouteManifestEntry(0))).toMap }
          handler.handle(SourceEvents(routeMapOpt, events.events))
        }
      }
    }
  }

  def requests: Sink[Seq[ServiceRequest]] = link.requests

  def responses: Source[Seq[ServiceResponse]] = link.responses
}

class PeerLinkEventProcessor(session: PeerSessionId, handler: SourceEvents => Unit) {

  private val routeManifestRow = PeerRouteSource.peerRouteRow(session)
  private val manifestSynth = new ValueUpdateSynthesizerImpl

  private def handleManifest(ev: AppendEvent): Option[Map[TypeValue, RouteManifestEntry]] = {

    val dvu = manifestSynth.handle(ev) match {
      case None => None
      case Some(ValueSync(_, value)) => Some(value)
      case Some(ValueDelta(value)) => Some(value)

    }

    val optMapUpdate = dvu.flatMap {
      case mu: MapUpdated => Some(mu)
      case _ => None
    }

    optMapUpdate.map(_.value).flatMap { routeMap =>
      val eitherEntries = routeMap.toSeq.map {
        case (key, v) => RouteManifestEntry.fromTypeValue(v).map(rv => (key, rv))
      }

      EitherUtil.rightSequence(eitherEntries).toOption.map(_.toMap).map { m =>
        m.map {
          case (route, entry) => (route, entry.copy(distance = entry.distance + 1))
        }
      }
    }
  }

  def handleStreamEvents(events: Seq[StreamEvent]): Unit = {

    var manifestUpdateOpt = Option.empty[Map[TypeValue, RouteManifestEntry]]

    events.foreach {
      case ev: RowAppendEvent =>
        if (ev.rowId == routeManifestRow) {
          manifestUpdateOpt = handleManifest(ev.appendEvent)
        }
      case ev: RouteUnresolved =>
        if (ev.routingKey == routeManifestRow.routingKey) {
          manifestUpdateOpt = Some(Map())
        }
    }

    handler(SourceEvents(manifestUpdateOpt, events))
  }
}

class PeerLinkSourceChannel(session: PeerSessionId, link: PeerLinkProxyChannel) extends GenericSourceChannel {

  private val routeManifestRow = PeerRouteSource.peerRouteRow(session)

  def init(): Unit = {
    link.subscriptions.push(Set(routeManifestRow))
  }

  def onClose: LatchSubscribable = link.onClose

  def subscriptions: Sink[Set[RowId]] = {
    new Sink[Set[RowId]] {
      def push(rows: Set[RowId]): Unit = {
        link.subscriptions.push(rows + routeManifestRow)
      }
    }
  }

  def events: Source[SourceEvents] = {
    new Source[SourceEvents] {
      def bind(handler: Handler[SourceEvents]): Unit = {
        val processor = new PeerLinkEventProcessor(session, handler.handle)
        link.events.bind(processor.handleStreamEvents)
      }
    }
  }

  def requests: Sink[Seq[ServiceRequest]] = link.requests

  def responses: Source[Seq[ServiceResponse]] = link.responses
}
