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

class LocalPeerManifestDb(selfSession: PeerSessionId) {

  private val routeRow = PeerRouteSource.peerRouteRow(selfSession)

  private var sequence: Long = 0
  private var manifest = Map.empty[TypeValue, RouteManifestEntry]

  def initial(): StreamEvent = {
    val result = RowAppendEvent(routeRow, ResyncSession(selfSession, SequenceCtx.empty, Resync(Int64Val(sequence), MapSnapshot(Map()))))
    sequence += 1
    result
  }

  def routesUpdated(routes: Set[TypeValue], map: Map[TypeValue, SourcingForRoute]): Seq[StreamEvent] = {

    val removed = Vector.newBuilder[TypeValue]
    val modified = Vector.newBuilder[(TypeValue, RouteManifestEntry)]
    val added = Vector.newBuilder[(TypeValue, RouteManifestEntry)]

    routes.foreach { route =>
      map.get(route) match {
        case None => {
          if (manifest.contains(route)) removed += route
        }
        case Some(sourcing) => {
          if (!sourcing.resolved()) {
            if (manifest.contains(route)) removed += route
          } else {
            val minimumDistance = sourcing.sourceMap.map(_._2.distance).min
            manifest.get(route) match {
              case None => added += (route -> RouteManifestEntry(minimumDistance))
              case Some(entry) =>
                if (entry.distance != minimumDistance) {
                  modified += (route -> RouteManifestEntry(minimumDistance))
                }
            }
          }
        }
      }
    }

    def writeKv(tup: (TypeValue, RouteManifestEntry)): (TypeValue, TypeValue) = {
      val (route, manifest) = tup
      (route, RouteManifestEntry.toTypeValue(manifest))
    }

    val removedResult = removed.result().toSet
    val modifiedResult = modified.result().toSet
    val addedResult = added.result().toSet

    if (removedResult.nonEmpty || modifiedResult.nonEmpty || addedResult.nonEmpty) {
      val diff = SequencedDiff(Int64Val(sequence), MapDiff(removedResult, addedResult.map(writeKv), modifiedResult.map(writeKv)))
      val results = Seq(RowAppendEvent(routeRow, StreamDelta(Delta(Seq(diff)))))
      sequence += 1
      manifest = (manifest -- removedResult) ++ addedResult ++ modifiedResult
      results
    } else {
      Seq()
    }
  }
}