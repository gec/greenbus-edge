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
package io.greenbus.edge.colset

import com.typesafe.scalalogging.LazyLogging

trait ManagedRouteSource {
  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit
  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit
}

object PeerRouteSource {
  val tablePrefix = "__manifest"

  def peerRouteRow(peerId: PeerSessionId): RowId = {
    RowId(TypeValueConversions.toTypeValue(peerId), SymbolVal(s"$tablePrefix"), SymbolVal("routes"))
  }
  def peerIndexRow(peerId: PeerSessionId): RowId = {
    RowId(TypeValueConversions.toTypeValue(peerId), SymbolVal(s"$tablePrefix"), SymbolVal("indexes"))
  }

  def manifestRows(peerId: PeerSessionId): Set[RowId] = {
    Set(peerRouteRow(peerId) /*, peerIndexRow(peerId)*/ )
  }
}
class PeerRouteSource(peerId: PeerSessionId, source: PeerLink) extends ManagedRouteSource with LazyLogging {

  private val routeRow = PeerRouteSource.peerRouteRow(peerId)
  private val indexRow = PeerRouteSource.peerIndexRow(peerId)

  private val routeLog = RouteManifestSet.build

  private var routeToRows: Map[TypeValue, Set[TableRow]] = Map(TypeValueConversions.toTypeValue(peerId) -> Set(routeRow.tableRow /*, indexRow.tableRow*/ ))

  private val keys: Set[RowId] = Set(routeRow /*, indexRow*/ )
  def manifestKeys: Set[RowId] = keys
  def manifestRoute: TypeValue = TypeValueConversions.toTypeValue(peerId)

  def init(): Unit = {
    source.setSubscriptions(manifestKeys)
  }

  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = {
    if (rows.nonEmpty) {
      routeToRows += (route -> rows)
      pushSubscription()
    } else {
      routeToRows -= route
      pushSubscription()
    }
  }

  private def pushSubscription(): Unit = {
    val rowIdSet = routeToRows.flatMap { case (route, rows) => rows.map(_.toRowId(route)) }.toSet
    source.setSubscriptions(rowIdSet)
  }

  def snapshot(): Map[TypeValue, RouteManifestEntry] = {
    routeLog.lastSnapshot
  }

  def handleSelfEvents(events: Seq[StreamEvent]): Option[KeyedSetDiff[TypeValue, RouteManifestEntry]] = {
    events.foreach {
      case RowAppendEvent(row, ev) =>
        row match {
          case `routeRow` => routeLog.handle(Seq(ev))
          //case `indexRow` =>
          case other => logger.debug(s"Unexpected row in $peerId manifest events: $other")
        }
      case sev =>
        logger.warn(s"Unexpected self event for $peerId: $sev")
    }
    routeLog.dequeue()
  }

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {

  }
}

