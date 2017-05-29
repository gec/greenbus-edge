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

import io.greenbus.edge.stream._
import io.greenbus.edge.stream.consume.ValueUpdateSynthesizerImpl
import io.greenbus.edge.stream.subscribe.{ MapUpdated, ValueDelta, ValueSync }
import io.greenbus.edge.util.EitherUtil

import scala.collection.mutable

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

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {
    source.requests.push(requests)
  }
}

