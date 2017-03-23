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
package io.greenbus.edge.api.stream.index

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api.{ EndpointDescriptor, EndpointId }
import io.greenbus.edge.api.stream.EdgeCodecCommon
import io.greenbus.edge.colset.subscribe._
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.gateway.GatewayRouteSource
import io.greenbus.edge.thread.CallMarshaller

/*
sealed trait ValueUpdate
case object ValueAbsent extends ValueUpdate
case object ValueUnresolved extends ValueUpdate
case object ValueDisconnected extends ValueUpdate
sealed trait DataValueUpdate extends ValueUpdate
case class Appended(session: PeerSessionId, values: Seq[AppendSetValue]) extends DataValueUpdate
case class SetUpdated(session: PeerSessionId, sequence: SequencedTypeValue, value: Set[TypeValue], removed: Set[TypeValue], added: Set[TypeValue]) extends DataValueUpdate
case class KeyedSetUpdated(session: PeerSessionId, sequence: SequencedTypeValue, value: Map[TypeValue, TypeValue], removed: Set[TypeValue], added: Set[(TypeValue, TypeValue)], modified: Set[(TypeValue, TypeValue)]) extends DataValueUpdate

 */
object Indexer {
  case class EndpointEntry(id: EndpointId, desc: Option[EndpointDescriptor])
}
class Indexer(eventThread: CallMarshaller) extends LazyLogging {
  import Indexer._

  private val subMgr = new DynamicSubscriptionManager(eventThread)

  private val peerManifestKey: SubscriptionKey = PeerBasedSubKey(sess => PeerRouteSource.peerRouteRow(sess))

  private var observedRoutes = Set.empty[TypeValue]
  private var endpointRouteSet = Map.empty[TypeValue, EndpointId]
  //private var endpointSet = Set.empty[EndpointId]

  private var descSubscriptionSet = Map.empty[RowId, EndpointEntry]

  private val descDb = new DescriptorDb

  subMgr.update(Set(peerManifestKey))
  subMgr.source.bind(handleEvents)

  def connected(sess: PeerSessionId, peer: PeerLinkProxyChannel): Unit = {
    subMgr.connected(sess, peer)
  }

  private def handleEvents(events: Seq[KeyedUpdate]): Unit = {
    events.foreach { update =>
      if (update.key == peerManifestKey) {
        update.value match {
          case up: KeyedSetUpdated =>
            onRouteManifestUpdate(up.value)
          case _ => logger.warn(s"Manifest data update was unexpected type: " + update)
        }
      } else {

        update.key match {
          case RowSubKey(row) => {
            descSubscriptionSet.get(row).foreach { entry =>
              update.value match {
                case up: Appended => {
                  val descOpt: Option[EndpointDescriptor] = up.values.lastOption.flatMap { v =>
                    EdgeCodecCommon.readEndpointDescriptor(v.value) match {
                      case Left(err) =>
                        logger.warn(s"Could not read endpoint descriptor: " + err); None
                      case Right(desc) => Some(desc)
                    }
                  }

                  (entry.desc, descOpt) match {
                    case (None, None) =>
                    case (Some(l), None) => descDb.removed(entry.id)
                    case (None, Some(r)) => descDb.observed(entry.id, r)
                    case (Some(l), Some(r)) =>
                      if (l != r) {
                        descDb.observed(entry.id, r)
                      }
                  }

                  descSubscriptionSet += (row -> entry.copy(desc = descOpt))
                }
                case ValueAbsent => descDb.removed(entry.id)
                case ValueUnresolved => descDb.removed(entry.id)
                case ValueDisconnected => descDb.removed(entry.id)
                case _ => logger.warn(s"Manifest data update was unexpected type: " + update)
              }
            }
          }
          case _ => logger.warn(s"Manifest sub key was unexpected type: " + update)
        }

      }
    }
  }

  private def onRouteManifestUpdate(manifest: Map[TypeValue, TypeValue]): Unit = {
    val added = manifest.keySet -- observedRoutes
    val removed = observedRoutes -- manifest.keySet

    val addedEndpointPairs = added.flatMap { route =>
      EdgeCodecCommon.routeToEndpointId(route).toOption.map { endId =>
        (route, endId)
      }
    }

    //val addedEndpointIds = addedEndpointPairs.map(_._2)
    endpointRouteSet ++= addedEndpointPairs

    //val removedEndpoints = removed.flatMap(endpointRouteSet.get)
    endpointRouteSet --= removed

    observedRoutes = manifest.keySet

    if (added.nonEmpty || removed.nonEmpty) {
      onEndpointSetUpdate()
    }
  }

  private def onEndpointSetUpdate(): Unit = {

    val descriptorRowMap: Map[RowId, EndpointId] = endpointRouteSet.map {
      case (route, id) =>
        val tr = EdgeCodecCommon.endpointIdToEndpointDescriptorTableRow(id)
        (tr.toRowId(route), id)
    }.toMap

    val descriptorRowSet = descriptorRowMap.keySet

    /*val removedRows = descSubscriptionSet.keySet -- descriptorRowSet
    removedRows.foreach(row => )*/

    val (alive, removed) = descSubscriptionSet.partition(tup => descriptorRowSet.contains(tup._1))

    removed.foreach {
      case (_, entry) => descDb.removed(entry.id)
    }

    descSubscriptionSet = alive

    descriptorRowMap.foreach {
      case (row, endId) =>
        if (!descSubscriptionSet.contains(row)) {
          descSubscriptionSet += (row -> EndpointEntry(endId, None))
        }
    }

    val keys: Set[SubscriptionKey] = descriptorRowSet.map(row => RowSubKey(row)).toSet
    val manifestKeySet: Set[SubscriptionKey] = Set(peerManifestKey)
    subMgr.update(manifestKeySet ++ keys)
  }

  private def onDisconnected(): Unit = {

  }
}

class DescriptorDb {
  def observed(id: EndpointId, desc: EndpointDescriptor): Unit = {

  }
  def removed(id: EndpointId): Unit = {

  }
}

class IndexProducer(routeSource: GatewayRouteSource) {

  val handle = routeSource.route(SymbolVal("indexer"))

  //handle.

}

