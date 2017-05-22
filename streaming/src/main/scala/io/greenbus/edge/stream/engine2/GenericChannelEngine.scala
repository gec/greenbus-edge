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
import io.greenbus.edge.thread.CallMarshaller

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

class PeerLinkSourceChannel(session: PeerSessionId, link: PeerLinkProxyChannel) extends GenericSourceChannel {

  private val routeManifestRow = PeerRouteSource.peerRouteRow(session)
  private val routeLog = RouteManifestSet.build

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
        link.events.bind(handleStreamEvents)
      }
    }
  }

  private def handleStreamEvents(events: Seq[StreamEvent]): Unit = {
    events.foreach {
      case ev: RowAppendEvent =>
        if (ev.rowId == routeManifestRow) {
          routeLog.handle(Seq(ev.appendEvent))
        }
      case ev: RouteUnresolved =>
        if (ev.routingKey == routeManifestRow.routingKey) {

        }
    }

    routeLog.dequeue()
  }

  def requests: Sink[Seq[ServiceRequest]] = link.requests

  def responses: Source[Seq[ServiceResponse]] = link.responses
}
