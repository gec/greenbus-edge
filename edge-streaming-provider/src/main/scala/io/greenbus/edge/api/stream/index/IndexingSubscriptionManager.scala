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
import io.greenbus.edge.api.stream.{ EdgeCodecCommon, EdgeTables }
import io.greenbus.edge.colset.subscribe._
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.gateway._
import io.greenbus.edge.thread.CallMarshaller

object IndexingSubscriptionManager {
  case class EndpointEntry(id: EndpointId, desc: Option[EndpointDescriptor])
}
class IndexingSubscriptionManager(eventThread: CallMarshaller, observer: DescriptorObserver) extends LazyLogging {
  import IndexingSubscriptionManager._

  private val subMgr = new DynamicSubscriptionManager(eventThread)

  private val peerManifestKey: SubscriptionKey = PeerBasedSubKey(sess => PeerRouteSource.peerRouteRow(sess))

  private var observedRoutes = Set.empty[TypeValue]
  private var endpointRouteSet = Map.empty[TypeValue, EndpointId]

  private var descSubscriptionSet = Map.empty[RowId, EndpointEntry]

  subMgr.update(Set(peerManifestKey))
  subMgr.source.bind(handleEvents)

  def connected(sess: PeerSessionId, peer: PeerLinkProxyChannel): Unit = {
    subMgr.connected(sess, peer)
  }

  private def onDisconnected(): Unit = {

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
                    case (Some(l), None) => observer.removed(entry.id)
                    case (None, Some(r)) => observer.observed(entry.id, r)
                    case (Some(l), Some(r)) =>
                      if (l != r) {
                        observer.observed(entry.id, r)
                      }
                  }

                  descSubscriptionSet += (row -> entry.copy(desc = descOpt))
                }
                case ValueAbsent => observer.removed(entry.id)
                case ValueUnresolved => observer.removed(entry.id)
                case ValueDisconnected => observer.removed(entry.id)
                case _ => logger.warn(s"Descriptor data update was unexpected type: " + update)
              }
            }
          }
          case _ => logger.warn(s"Descriptor sub key was unexpected type: " + update)
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
    logger.debug(s"Updating endpoint subscription set")

    val descriptorRowMap: Map[RowId, EndpointId] = endpointRouteSet.map {
      case (route, id) =>
        val tr = EdgeCodecCommon.endpointIdToEndpointDescriptorTableRow(id)
        (tr.toRowId(route), id)
    }.toMap

    val descriptorRowSet = descriptorRowMap.keySet

    val (alive, removed) = descSubscriptionSet.partition(tup => descriptorRowSet.contains(tup._1))

    removed.foreach {
      case (_, entry) => observer.removed(entry.id)
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
}

class DescriptorObserver extends DescriptorCache {

  private var descMap = Map.empty[EndpointId, EndpointDescriptor]

  def descriptors: Map[EndpointId, EndpointDescriptor] = {
    descMap
  }

  def observed(id: EndpointId, desc: EndpointDescriptor): Unit = {
    descMap += (id -> desc)
  }

  def removed(id: EndpointId): Unit = {
    descMap -= id
  }
}

class DataKeyIndexSource(cache: DescriptorCache, indexer: DataKeyIndexDb[TypeValue]) extends DynamicTableSource with LazyLogging {

  private var rowMap = Map.empty[TypeValue, SetSink]

  def added(row: TypeValue): BindableRowMgr = {

    val sink = rowMap.getOrElse(row, {
      val built = new SetSink
      rowMap += (row -> built)
      built
    })

    EdgeCodecCommon.readIndexSpecifier(row) match {
      case Left(err) => {
        logger.warn(s"Endpoint index row could not be parsed: $row: $err")
        sink.update(Set())
      }
      case Right(spec) => {
        val orig = indexer.addSubscription(spec, row)
        sink.update(orig.map(endPath => EdgeCodecCommon.writeEndpointPath(endPath)))
      }
    }

    sink
  }

  def removed(row: TypeValue): Unit = {
    indexer.removeTarget(row)
    rowMap -= row
  }
}

class IndexProducer(eventThread: CallMarshaller, routeSource: GatewayRouteSource) {

  private val observer = new DescriptorObserver

  private val dataKeyIndexDb = new DataKeyIndexDb[TypeValue](observer)
  private val dataKeyIndexSource = new DataKeyIndexSource(observer, dataKeyIndexDb)

  private val indexer = new IndexingSubscriptionManager(eventThread, observer)

  private var sourceOpt = Option.empty[RouteSourceHandle]

  def connected(sess: PeerSessionId, peer: PeerLinkProxyChannel): Unit = {
    if (sourceOpt.isEmpty) {
      register(sess, peer)
    }
    indexer.connected(sess, peer)
  }

  private def register(sess: PeerSessionId, peer: PeerLinkProxyChannel): Unit = {
    val handle = routeSource.route(TypeValueConversions.toTypeValue(sess))
    handle.dynamicTable(EdgeTables.dataKeyIndexTable, dataKeyIndexSource)
    sourceOpt = Some(handle)
  }
}

