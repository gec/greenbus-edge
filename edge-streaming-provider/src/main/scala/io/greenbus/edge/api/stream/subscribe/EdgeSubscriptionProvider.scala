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
package io.greenbus.edge.api.stream.subscribe

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.api.stream._
import io.greenbus.edge.flow.{ Handler, Source }
import io.greenbus.edge.stream.consume._
import io.greenbus.edge.stream.peer.{ StreamPeer, StreamUserSubscription }
import io.greenbus.edge.stream.{ PeerRouteSource, RowId }

import scala.collection.mutable

class EdgeSubImpl(sub: StreamUserSubscription, initial: Seq[IdentifiedEdgeUpdate], map: Map[RowId, EdgeKeyUpdateTranslator]) extends EdgeSubscription with LazyLogging {

  private var initialIssued = false

  def updates: Source[Seq[IdentifiedEdgeUpdate]] = {
    new Source[Seq[IdentifiedEdgeUpdate]] {
      def bind(handler: Handler[Seq[IdentifiedEdgeUpdate]]): Unit = {
        sub.events.bind { rowUpdates =>
          if (!initialIssued) {
            handler.handle(initial)
            initialIssued = true
          }
          val edgeUpdates = rowUpdates.flatMap { up =>
            map.get(up.row).map(_.handle(up.update)).getOrElse(Seq())
          }
          handler.handle(edgeUpdates)
        }
      }
    }
  }

  def close(): Unit = {
    sub.close()
  }
}

class EdgeSubscriptionProvider(peer: StreamPeer) extends EdgeSubscriptionClient {

  def subscribe(params: SubscriptionParams): EdgeSubscription = {

    val transMap = mutable.Map.empty[RowId, EdgeKeyUpdateTranslator]
    val initial = Vector.newBuilder[IdentifiedEdgeUpdate]

    params.endpointPrefixSet.foreach { path =>
      val keyTranslator = new EdgeKeyUpdateTranslator(new ManifestRowToEndpointSetCodec(path.toString, path))
      val row = PeerRouteSource.peerRouteRow(peer.session)
      initial += IdEndpointPrefixUpdate(path, Pending)
      transMap += (row -> keyTranslator)
    }

    params.endpointDescriptors.foreach { id =>
      val keyTranslator = new EdgeKeyUpdateTranslator(new EndpointDescSubCodec(id.toString, id))
      val row = EdgeCodecCommon.endpointIdToEndpointDescriptorRow(id)
      initial += IdEndpointUpdate(id, Pending)
      transMap += (row -> keyTranslator)
    }

    params.dataKeys.foreach { id =>
      val keyTranslator = new EdgeKeyUpdateTranslator(new DynamicDataKeyCodec(id.toString, up => IdDataKeyUpdate(id, up)))
      val row = EdgeCodecCommon.dataKeyRowId(id)
      initial += IdDataKeyUpdate(id, Pending)
      transMap += (row -> keyTranslator)
    }

    params.outputKeys.foreach { id =>
      val keyTranslator = new EdgeKeyUpdateTranslator(new AppendOutputKeySubCodec(id.toString, id, AppendOutputKeyCodec))
      val row = EdgeCodecCommon.outputKeyRowId(id)
      initial += IdOutputKeyUpdate(id, Pending)
      transMap += (row -> keyTranslator)
    }

    params.dynamicDataKeys.foreach { id =>
      val keyTranslator = new EdgeKeyUpdateTranslator(new DynamicDataKeyCodec(id.toString, up => IdDynamicDataKeyUpdate(id, up)))
      val row = EdgeCodecCommon.dynamicDataKeyRow(id)
      initial += IdDynamicDataKeyUpdate(id, Pending)
      transMap += (row -> keyTranslator)
    }

    val transMapResult = transMap.toMap

    val streamSub = peer.subscribe(transMapResult.keySet)

    new EdgeSubImpl(streamSub, initial.result(), transMap.toMap)
  }

}

class EdgeKeyUpdateTranslator(codec: EdgeSubCodec) {

  def handle(update: ValueUpdate): Seq[IdentifiedEdgeUpdate] = {
    update match {
      case vs: ValueSync =>
        codec.updateFor(vs.initial, vs.metadata)
      case vd: ValueDelta =>
        codec.updateFor(vd.update, None)
      case ValueAbsent =>
        Seq(codec.simpleToUpdate(ResolvedAbsent))
      case ValueUnresolved =>
        Seq(codec.simpleToUpdate(DataUnresolved))
    }
  }
}
