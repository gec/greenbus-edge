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

class GenericChannelEngine(streamEngine: StreamEngine, serviceEngine: ServiceEngine) {

  private val queueSet = mutable.Map.empty[TargetQueueMgr, GenericTargetChannel]

  def sourceChannel(source: GenericSourceChannel): Unit = {
    val streamSource = new RouteStreamSourceImpl(source)
    source.events.bind(events => sourceUpdates(streamSource, events))
    source.responses.bind(serviceEngine.handleResponses)
    source.onClose.subscribe(() => sourceChannelClosed(source, streamSource))
    flush()
  }

  private def sourceUpdates(source: RouteStreamSource, events: SourceEvents): Unit = {
    streamEngine.sourceUpdate(source, events)
    flush()
  }

  private def sourceChannelClosed(source: GenericSourceChannel, handle: RouteStreamSource): Unit = {
    streamEngine.sourceRemoved(handle)
    flush()
  }

  private def flush(): Unit = {
    queueSet.foreach {
      case (queue, channel) =>
        val events = queue.dequeue()
        if (events.nonEmpty) {
          channel.events.push(events)
        }
    }
  }

  def targetChannel(target: GenericTargetChannel): Unit = {

    val queueMgr = new TargetQueueMgr
    queueSet += (queueMgr -> target)

    target.subscriptions.bind { rows =>
      val observers = queueMgr.subscriptionUpdate(rows)
      streamEngine.targetSubscriptionUpdate(queueMgr, observers)
    }

    val issuer = new TargetRequestIssuer(target)
    target.requests.bind(serviceEngine.requestsIssued(issuer, _))

    target.onClose.subscribe(() => {
      targetRemoved(queueMgr)
      serviceEngine.issuerClosed(issuer)
    })
  }

  private def targetRemoved(queueMgr: TargetQueueMgr): Unit = {
    streamEngine.targetRemoved(queueMgr)
    queueSet -= queueMgr
  }
}

class TargetRequestIssuer(proxy: GenericTargetChannel) extends ServiceIssuer {
  def handleResponses(responses: Seq[ServiceResponse]): Unit = {
    proxy.responses.push(responses)
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
