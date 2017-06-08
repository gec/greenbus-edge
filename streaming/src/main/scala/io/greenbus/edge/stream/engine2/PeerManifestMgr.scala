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
import io.greenbus.edge.stream.filter.StreamCacheImpl
import io.greenbus.edge.stream.gateway3.MapSequencer

class StaticStream(appendLimitDefault: Int) extends CachingKeyStreamSubject {
  private val cache = new StreamCacheImpl(appendLimitDefault)

  def handle(events: Seq[AppendEvent]): Unit = {
    events.foreach(cache.handle)
    observers.foreach(obs => events.foreach(obs.handle))
  }

  protected def sync(): Seq[AppendEvent] = {
    cache.resync()
  }

  def targeted(): Boolean = true
}

class PeerSelfStreams(streams: Map[TableRow, StaticStream]) extends RouteStreamMgr with RouteTargetSubjectBasic[StaticStream] {

  streams.foreach { case (k, v) => streamMap.update(k, v) }

  def events(source: RouteStreamSource, events: Seq[StreamEvent]): Unit = {}

  def sourceAdded(source: RouteStreamSource, details: RouteManifestEntry): Unit = {}

  def sourceRemoved(source: RouteStreamSource): Unit = {}

  def sourced(sourcing: RouteSourcingMgr): Unit = {}

  def unsourced(): Unit = {}

  protected def streamFactory(key: TableRow): StaticStream = {
    new StaticStream(appendLimitDefault = 1)
  }
}

class PeerManifestMgr(session: PeerSessionId) {
  private val manifestRow = PeerRouteSource.peerRouteRow(session)
  private val manifestSequencer = new MapSequencer(session, SequenceCtx(None, None))
  private val manifestStream = new StaticStream(appendLimitDefault = 1)

  private val routeMgr = new PeerSelfStreams(Map(manifestRow.tableRow -> manifestStream))

  def routeManager: RouteStreamMgr = routeMgr
  def route: TypeValue = manifestRow.routingKey

  def update(manifest: Map[TypeValue, RouteManifestEntry]): Unit = {
    val encoded = manifest.map { case (route, entry) => (route, RouteManifestEntry.toTypeValue(entry)) }
    val events = manifestSequencer.handle(encoded)
    manifestStream.handle(events)
  }

}
