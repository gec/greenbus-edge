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
import io.greenbus.edge.channel2.{ CloseObservable, QueuedDistributor, Source }
import io.greenbus.edge.collection.BiMultiMap

object PeerManifest {

  def eitherSomeIsRightOrNone[L, R](vOpt: Option[Either[L, R]]): Either[L, Option[R]] = {
    vOpt.map(_.map(r => Some(r))).getOrElse(Right(Option.empty[R]))
  }

  def parseIndexableTypeValue(tv: TypeValue): Either[String, IndexableTypeValue] = {
    tv match {
      case v: IndexableTypeValue => Right(v)
      case _ => Left(s"Unrecognized indexable type value: $tv")
    }
  }

  def parseIndexSpecifier(tv: TypeValue): Either[String, IndexSpecifier] = {
    tv match {
      case TupleVal(seq) =>
        if (seq.size >= 2) {
          val key = seq(0)
          seq(1) match {
            case v: OptionVal =>
              eitherSomeIsRightOrNone(v.element.map(parseIndexableTypeValue))
                .map(vOpt => IndexSpecifier(key, vOpt))
            case _ => Left(s"Unrecognized index type: $tv")
          }
        } else {
          Left(s"Unrecognized index type: $tv")
        }
      case _ => Left(s"Unrecognized index type: $tv")
    }
  }
}
case class PeerManifest(routingKeySet: Set[TypeValue], indexSet: Set[IndexSpecifier])

class SourceLinksManifest[Link] {

  private var routingMap = BiMultiMap.empty[TypeValue, Link]
  private var indexMap = BiMultiMap.empty[IndexSpecifier, Link]

  def routingKeys: Map[TypeValue, Set[Link]] = routingMap.keyToVal
  def indexes: Map[IndexSpecifier, Set[Link]] = indexMap.keyToVal

  def handleUpdate(link: Link, manifest: PeerManifest): Unit = {
    routingMap = routingMap.reverseAdd(link, manifest.routingKeySet)
    indexMap = indexMap.reverseAdd(link, manifest.indexSet)
  }

  def linkRemoved(link: Link): Unit = {
    routingMap = routingMap.removeValue(link)
    indexMap = indexMap.removeValue(link)
  }
}

class LocalManifest[Publisher] {

}

/*

case class ManifestUpdate(routingSet: Option[SetChanges[TypeValue]], indexSet: Option[SetChanges[IndexSpecifier]])

case class PeerSourceEvents(manifestUpdate: Option[ManifestUpdate], sessionNotifications: Seq[StreamEvent])


trait RemotePeerSourceLink extends CloseObservable {
  def link(): Unit
  def remoteManifest: Source[ManifestUpdate]
}

// TODO: could eliminate direct notifications, which would make peer manifest visible to ui, but need that "rowKey" to not be an endpoint somehow??
class RemotePeerSubscriptionLinkMgr(remoteId: PeerSessionId, subscriptionMgr: RemoteSubscribable) extends /*RemotePeerSourceLink with*/ LazyLogging {

  private val endpointTableRow =  DirectTableRowId(SourcedEndpointPeer.endpointTable, TypeValueConversions.toTypeValue(remoteId))
  private val endpointIndexesTableRow =  DirectTableRowId(SourcedEndpointPeer.endpointIndexTable, TypeValueConversions.toTypeValue(remoteId))
  private val endpointKeyIndexesTableRow =  DirectTableRowId(SourcedEndpointPeer.keyIndexTable, TypeValueConversions.toTypeValue(remoteId))

  //private val endTableSetOpt = Option.empty[TypedSimpleSeqModifiedSetDb]
  private val endTableSet = new UntypedSimpleSeqModifiedSetDb
  private val endIndexesSet = new UntypedSimpleSeqModifiedSetDb
  private val endKeysIndexesSet = new UntypedSimpleSeqModifiedSetDb

  private val distributor = new QueuedDistributor[PeerSourceEvents]
  def events: Source[PeerSourceEvents] = distributor

  def link(): Unit = {
    val endpoints = DirectSetSubscription(endpointTableRow, None)
    val endIndexes = DirectSetSubscription(endpointIndexesTableRow, None)
    val endKeyIndexes = DirectSetSubscription(endpointKeyIndexesTableRow, None)
    val params = SubscriptionParams(Seq(endpoints, endIndexes, endKeyIndexes))

    subscriptionMgr.updateSubscription(params)
    subscriptionMgr.source.bind(handle)
  }

  //def events: Source[PeerSourceEvents]

  //def remoteManifest: Source[ManifestUpdate] = ???

  protected def handle(notifications: SubscriptionNotifications): Unit = {
    val startEndTableSet = endTableSet.current
    val startEndIndexesSet = endIndexesSet.current
    val startKeysIndexesSet = endKeysIndexesSet.current

    notifications.localNotifications.foreach { batch =>
      batch.sets.foreach { set =>
        set.tableRowId match {
          case `endpointTableRow` => endTableSet.observe(set.update)
          case `endpointIndexesTableRow` => endIndexesSet.observe(set.update)
          case `endpointKeyIndexesTableRow` => endKeysIndexesSet.observe(set.update)
          case _ => logger.warn("Unrecognized local set subscription: " + set.tableRowId)
        }
      }
      batch.keyedSets.foreach { set =>
        logger.warn("Unrecognized local keyed set subscription: " + set.tableRowId)
      }
      batch.appendSets.foreach { set =>
        logger.warn("Unrecognized append set subscription: " + set.tableRowId)
      }
    }

    val updatedEndTableSet = endTableSet.current
    val updatedEndIndexesSet = endIndexesSet.current
    val updatedKeysIndexesSet = endKeysIndexesSet.current

    val routingOpt = SetChanges.calcOpt(startEndTableSet, updatedEndTableSet)

    val rowIndexOpt = SetChanges.calcOpt(
      startKeysIndexesSet.flatMap(parseIndexSpecifier),
      updatedKeysIndexesSet.flatMap(parseIndexSpecifier))


    val manifestUpdateOpt = if (routingOpt.nonEmpty || rowIndexOpt.nonEmpty) {
      Some(ManifestUpdate(routingOpt, rowIndexOpt))
    } else {
      None
    }

    ???
    //distributor.push(PeerSourceEvents(manifestUpdateOpt, notifications.sessionNotifications))
  }

  private def parseIndexSpecifier(tv: TypeValue): Option[IndexSpecifier] = {
    PeerManifest.parseIndexSpecifier(tv) match {
      case Right(indexSpecifier) => Some(indexSpecifier)
      case Left(err) => logger.warn(s"$remoteId did not recognize index set value: $tv, error: $err"); None
    }
  }
}*/

/*

trait RemoteSubscribable extends Closeable with CloseObservable {
  def updateSubscription(params: SubscriptionParams)
  def source: Source[SubscriptionNotifications]
}


case class RemotePeerChannels(
                               subscriptionControl: SenderChannel[SubscriptionParams, Boolean],
                               subscriptionReceive: ReceiverChannel[SubscriptionNotifications, Boolean]/*,
                               outputRequests: SenderChannel[OutputRequest, Boolean],
                               outputResponses: ReceiverChannel[OutputResponse, Boolean]*/)

// TODO: "two-(multi-) way channel that combines closeability/closeobservableness and abstracts individual amqp links from actual two-way inter-process comms like websocket
class RemoteSubscribableImpl(eventThread: SchedulableCallMarshaller, channels: RemotePeerChannels) extends RemoteSubscribable {
  def updateSubscription(params: SubscriptionParams): Unit = ???

  def source: Source[SubscriptionNotifications] = ???

  def onClose(): Unit = ???

  def close(): Unit = ???
}

object SetChanges {
  def calc[A](start: Set[A], end: Set[A]): SetChanges[A] = {
    SetChanges(end, end -- start, start -- end)
  }
  def calcOpt[A](start: Set[A], end: Set[A]): Option[SetChanges[A]] = {
    val adds = end -- start
    val removes = start -- end
    if (adds.nonEmpty || removes.nonEmpty) {
      Some(SetChanges(end, end -- start, start -- end))
    } else {
      None
    }
  }
}
case class SetChanges[A](snapshot: Set[A], adds: Set[A], removes: Set[A])
*/
