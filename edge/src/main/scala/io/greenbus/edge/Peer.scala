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
package io.greenbus.edge

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.channel.Responder

import scala.concurrent.Promise

trait SourceId
case class ClientSessionSourceId(sessionId: SessionId) extends SourceId

class EndpointDbMgr {
  private var endpoints = Map.empty[EndpointId, EndpointDb]

  def lookup(path: Path): Seq[(EndpointId, EndpointDb)] = {
    endpoints.toVector
  }

  def map: Map[EndpointId, EndpointDb] = endpoints

  def get(id: EndpointId): Option[EndpointDb] = endpoints.get(id)
  def add(id: EndpointId, db: EndpointDb): Unit = {
    endpoints += (id -> db)
  }
}

case class IndexSpecifier(key: Path, valueOpt: Option[IndexableValue])
sealed trait IndexSubscription
case class EndpointIndexSubscription(specifier: IndexSpecifier, target: ClientSubscriberProxy) extends IndexSubscription
case class DataIndexSubscription(specifier: IndexSpecifier, target: ClientSubscriberProxy) extends IndexSubscription
case class OutputIndexSubscription(specifier: IndexSpecifier, target: ClientSubscriberProxy) extends IndexSubscription

case class IndexUpdate[A](added: Set[A], removed: Set[A], targets: Set[ClientSubscriberProxy])

trait TypedIndexDb[A] {
  protected var activeSets = Map.empty[IndexSpecifier, Set[A]]
  private var queryToTarget = Map.empty[IndexSpecifier, Set[ClientSubscriberProxy]]
  private var targetToQuery = Map.empty[ClientSubscriberProxy, Set[IndexSpecifier]]

  def active: Set[IndexSpecifier] = activeSets.keySet

  protected def buildIndex(specifier: IndexSpecifier): Set[A]

  protected def observe(desc: EndpointDescriptor): IndexUpdate[A]

  def addSubscription(specifier: IndexSpecifier, target: ClientSubscriberProxy): Unit = {
    queryToTarget.get(specifier) match {
      case None =>
        queryToTarget += (specifier -> Set(target))
      case Some(targSet) =>
        queryToTarget += (specifier -> (targSet ++ Set(target)))
    }

    targetToQuery.get(target) match {
      case None =>
        targetToQuery += (target -> Set(specifier))
      case Some(specSet) =>
        targetToQuery += (target -> (specSet ++ Set(specifier)))
    }

    activeSets.get(specifier) match {
      case None =>
        val built = buildIndex(specifier)
        activeSets += (specifier -> built)
        built
      case Some(set) => set
    }
  }

  def removeTarget(target: ClientSubscriberProxy): Unit = {
    targetToQuery.get(target).foreach { set =>
      set.foreach { spec =>
        queryToTarget.get(spec).foreach { targSet =>
          val removed = targSet - target
          if (removed.isEmpty) {
            queryToTarget -= spec
            activeSets -= spec
          } else {
            queryToTarget += (spec -> removed)
          }
        }
      }

      targetToQuery -= target
    }
  }
}

class EndpointIndexDb(endpointDbMgr: EndpointDbMgr) extends TypedIndexDb[EndpointId] {

  protected def buildIndex(specifier: IndexSpecifier): Set[EndpointId] = {
    val key = specifier.key
    val vOpt = specifier.valueOpt
    endpointDbMgr.map.flatMap {
      case (endId, db) => db.currentInfo().flatMap { descRec =>
        descRec.descriptor.indexes.get(key).flatMap { v =>
          if (vOpt.isEmpty || vOpt.contains(v)) {
            Some(endId)
          } else {
            None
          }
        }
      }
    }.toSet
  }

  protected def observe(desc: EndpointDescriptor): IndexUpdate[EndpointId] = {

  }
}

class IndexSetDb(endpointDbMgr: EndpointDbMgr) {

  private var activeEndpointIndexes = Map.empty[IndexSpecifier, Set[EndpointId]]
  private var endpointTargets = Map.empty[IndexSpecifier, Set[ClientSubscriberProxy]]
  private var targetToEndpoints = Map.empty[ClientSubscriberProxy, Set[IndexSpecifier]]

  def add(subscription: IndexSubscription): Set[EndpointId] = {
    subscription match {
      case sub: EndpointIndexSubscription => {
        //val key = sub.specifier.key
        //val vOpt = sub.specifier.valueOpt

        def buildIndex(specifier: IndexSpecifier, target: ClientSubscriberProxy): Set[EndpointId] = {
          ???
        }

        def addTarget(specifier: IndexSpecifier, target: ClientSubscriberProxy): Unit = {

        }

        val spec = sub.specifier
        val target = sub.target
        activeEndpointIndexes.get(spec) match {
          case None => buildIndex(spec, target)
          case Some(set) =>
            addTarget(spec, target)
            set
        }

        /*endpointDbMgr.map.flatMap {
          case (endId, db) => db.currentInfo().map { descRec =>
            descRec.descriptor.indexes.get(sub.specifier.key).map { v =>
              if (vOpt.isEmpty || vOpt.contains(v)) {
                endId
              } else {
                None
              }
            }
          }
        }
        */
      }
    }

  }
  def remove(subscription: IndexSubscription): Unit = {

  }
  def removeTarget(target: ClientSubscriberProxy): Unit ={

  }

}

/*
  // TODO: how does control move through peers?
 */
object Peer {
  type SubscriptionTarget = ClientSubscriberProxy
}

class Peer(selfMarshaller: CallMarshaller, dataDef: DataStreamSource) extends LazyLogging {
  import Peer._

  /*
  manifest db
  endpoint db
  subscription db
  output source db?
   */
  private val subscriptionDb = new ScanningSubscriptionDb[SubscriptionTarget]
  private val endpoints = new EndpointDbMgr
  private val clientPublishers: ClientPublisherProxyDb = new ClientPublisherProxyDbImpl
  private var aliveSubscribers = Set.empty[ClientSubscriberProxy]
  private val outputCorrelator = new Correlator[(ClientOutputProxy, Long)]
  private var aliveOutputClients = Set.empty[ClientOutputProxy]

  private def activeSessionForEndpoint(id: EndpointId): Option[SessionId] = {
    endpoints.get(id).flatMap(_.activeSession())
  }

  private def handlePublisherOutputResponse(correlation: Long, result: OutputResult): Unit = {
    logger.debug(s"handlePublisherOutputResponse: $correlation - $result")
    outputCorrelator.pop(correlation) match {
      case None => // log?
      case Some((outputProxy, clientCorrelation)) =>
        logger.debug(s"handlePublisherOutputResponse: $outputProxy, for $clientCorrelation")
        outputProxy.respond(clientCorrelation, result)
    }
  }

  def onClientOutputOpened(outputProxy: ClientOutputProxy): Unit = {
    outputProxy.requests.bindFunc(handleClientOutputValueRequest(outputProxy, _, _))
    outputProxy.onClose.bind(() => onClientOutputClosed(outputProxy))
    aliveOutputClients += outputProxy
  }

  def onClientOutputClosed(outputProxy: ClientOutputProxy): Unit = {
    aliveOutputClients -= outputProxy
  }

  def handleClientOutputValueRequest(outputProxy: ClientOutputProxy, message: ClientOutputRequestMessage, promise: Promise[Boolean]): Unit = {
    logger.debug("handleClientOutputValueRequest" + message)
    message.requests.foreach { req =>
      val key = req.key
      val requestSessionOpt = req.value.sessionOpt
      val activeSessionOpt = activeSessionForEndpoint(key.endpoint)

      if (requestSessionOpt.isEmpty || requestSessionOpt == activeSessionOpt) {

        val publisherOpt = activeSessionOpt.flatMap(sess => clientPublishers.get(sess, key.endpoint))

        publisherOpt match {
          case None => outputProxy.respond(req.correlation, OutputFailure("Publisher not found"))
          case Some(publisherProxy) => {

            val peerCorrelation = outputCorrelator.add((outputProxy, req.correlation))
            val outputValue = PublisherOutputParams(req.value.sequenceOpt, req.value.compareValueOpt, req.value.outputValueOpt)
            publisherProxy.outputIssued(key.key, outputValue, handlePublisherOutputResponse(peerCorrelation, _))
          }
        }
      } else {
        outputProxy.respond(req.correlation, OutputFailure("Session not active"))
      }

    }
    promise.success(true)
  }

  private def currentSubscriptionSnapshot(params: ClientSubscriptionParams): ClientSubscriptionNotification = {

    val setNotifications = params.endpointSetPrefixes.map { prefix =>
      val entries = endpoints.lookup(prefix).flatMap {
        case (id, db) => db.currentInfo().map(info => EndpointSetEntry(id, info.descriptor.indexes))
      }
      EndpointSetNotification(prefix, Some(EndpointSetSnapshot(entries)), Seq(), Seq(), Seq())
    }

    val records = params.infoSubscriptions.flatMap { id =>
      endpoints.get(id).flatMap { db =>
        db.currentInfo()
      }
    }

    val infoNotifications = records.map(rec => EndpointDescriptorNotification(rec.endpointId, rec.descriptor, rec.sequence))

    val dataB = Vector.newBuilder[EndpointDataNotification]
    params.dataSubscriptions.groupBy(_.endpoint).foreach {
      case (id, dataKeys) =>
        endpoints.get(id).foreach { db =>
          val updateSetOpt = db.currentData(dataKeys.map(_.key))
          updateSetOpt.foreach { updateSet =>
            updateSet.foreach {
              case ((key, state)) =>
                dataB += EndpointDataNotification(EndpointPath(id, key), state)
            }
          }
        }
    }
    val dataNotifications = dataB.result()

    val outputB = Vector.newBuilder[EndpointOutputStatusNotification]
    params.outputSubscriptions.groupBy(_.endpoint).foreach {
      case (id, keys) =>
        endpoints.get(id).foreach { db =>
          val updateSetOpt = db.currentOutputStatuses(keys.map(_.key))
          updateSetOpt.foreach { updateSet =>
            updateSet.foreach {
              case ((key, state)) =>
                outputB += EndpointOutputStatusNotification(EndpointPath(id, key), state)
            }
          }
        }
    }
    val outputNotifications = outputB.result()

    ClientSubscriptionNotification(setNotifications, infoNotifications, dataNotifications, outputNotifications)
  }

  private def handleBatchPublish(sourceId: SourceId, sessionId: SessionId, endpointId: EndpointId, batch: EndpointPublishMessage): Unit = {
    val processResultOpt = endpoints.get(endpointId).map { db => db.processBatch(sourceId, sessionId, batch) }
    processResultOpt.foreach { result =>

      // TODO: transaction system where during transaction sub db puts stuff into buckets?
      var notifyMap = Set.empty[SubscriptionTarget]
      var infoMap = Map.empty[SubscriptionTarget, EndpointDescriptorNotification]
      var dataMap = Map.empty[SubscriptionTarget, Seq[EndpointDataNotification]]
      var outputMap = Map.empty[SubscriptionTarget, Seq[EndpointOutputStatusNotification]]
      var setMap = Map.empty[SubscriptionTarget, Seq[EndpointSetNotification]]

      result.setUpdate.foreach { entry =>
        subscriptionDb.queryForEndpointSetPrefixMatches(Path(Seq())).foreach {
          case (prefix, subscribers) =>
            val notification = if (result.added) {
              EndpointSetNotification(prefix, None, Seq(entry), Seq(), Seq())
            } else {
              EndpointSetNotification(prefix, None, Seq(), Seq(entry), Seq())
            }

            subscribers.foreach { target =>
              setMap.get(target) match {
                case None => setMap += (target -> Seq(notification))
                case Some(existing) => setMap += (target -> (existing :+ notification))
              }
              notifyMap += target
            }
        }
      }

      result.infoUpdateOpt.foreach {
        case (seq, info) => {
          val subscribers = subscriptionDb.queryEndpointInfoSubscriptions(endpointId)
          subscribers.foreach { target =>
            infoMap += (target -> EndpointDescriptorNotification(endpointId, info, seq))
            notifyMap += target
          }
        }
      }

      result.valueUpdates.foreach {
        case (key, update) =>
          val endKey = EndpointPath(endpointId, key)
          val subscribers = subscriptionDb.queryDataSubscriptions(endKey)
          val notification = EndpointDataNotification(endKey, update)
          subscribers.foreach { target =>
            dataMap.get(target) match {
              case None => dataMap += (target -> Seq(notification))
              case Some(existing) => dataMap += (target -> (existing :+ notification))
            }
            notifyMap += target
          }
      }

      result.outputUpdates.foreach {
        case (key, update) =>
          val endKey = EndpointPath(endpointId, key)
          val subscribers = subscriptionDb.queryOutputSubscriptions(endKey)
          val notification = EndpointOutputStatusNotification(endKey, update)
          subscribers.foreach { target =>
            outputMap.get(target) match {
              case None => outputMap += (target -> Seq(notification))
              case Some(existing) => outputMap += (target -> (existing :+ notification))
            }
            notifyMap += target
          }
      }

      notifyMap.foreach { target =>
        val setOpt = setMap.get(target)
        val infoOpt = infoMap.get(target)
        val dataOpt = dataMap.get(target)
        val outputOpt = outputMap.get(target)
        val setSeq = setOpt.getOrElse(Seq())
        val infoSeq = infoOpt.map(Seq(_)).getOrElse(Seq())
        val dataSeq = dataOpt.getOrElse(Seq())
        val outputSeq = outputOpt.getOrElse(Seq())
        val message = ClientSubscriptionNotification(setSeq, infoSeq, dataSeq, outputSeq)
        target.notify(message)
      }
    }
  }

  def onPeerInputChannelOpened() = ???
  def onPeerInputChannelClosed() = ???

  def onClientPublish(id: EndpointPublisherId, batch: EndpointPublishMessage, promise: Promise[Boolean]): Unit = {
    logger.debug("got client publish: " + batch)
    handleBatchPublish(id.sourceId, id.sessionId, id.endpointId, batch)
    promise.success(true)
  }

  def onClientPublisherOpened(id: EndpointPublisherId, proxy: ClientPublisherProxy): Unit = {

    def doOpen(): Unit = {
      logger.info(s"Publisher opened for ${id.endpointId}")
      proxy.onClose.bind(() => onClientPublisherClosed(id, proxy))

      proxy.dataUpdates.bind(new Responder[EndpointPublishMessage, Boolean] {
        def handle(obj: EndpointPublishMessage, promise: Promise[Boolean]): Unit = {
          onClientPublish(id, obj, promise)
        }
      })

      clientPublishers.add(id.sessionId, id.endpointId, proxy)
    }

    def doReject(): Unit = {
      proxy.close()
    }

    logger.info(s"Publisher open request for $id")

    import id._
    endpoints.get(id.endpointId) match {
      case None => {
        val db = new EndpointDb(endpointId, dataDef)
        if (db.allowPublisher(sourceId, sessionId)) {
          endpoints.add(endpointId, db)
          doOpen()
        } else {
          logger.warn(s"Endpoint publisher was disallowed for $id")
          doReject()
        }
      }
      case Some(db) => {
        if (db.allowPublisher(sourceId, sessionId)) {
          doOpen()
        } else {
          logger.warn(s"Endpoint publisher was disallowed for $id")
          doReject()
        }
      }
    }

  }

  def onClientPublisherClosed(id: EndpointPublisherId, proxy: ClientPublisherProxy): Unit = {
    logger.info("Publisher closed: " + id)
    proxy.close()
    clientPublishers.remove(proxy)
    endpoints.get(id.endpointId).foreach(_.sourceRemoved(id.sourceId))
  }

  def onPeerOutputChannelOpened() = ???
  def onPeerOutputChannelClosed() = ???

  def onClientSubscriptionOpened(proxy: ClientSubscriberProxy): Unit = {
    aliveSubscribers += proxy

    proxy.params.bindFunc(onSubscriptionParamsUpdate(proxy, _, _))

    proxy.onClose.bind(() => onClientSubscriptionClosed(proxy))
  }

  private def onSubscriptionParamsUpdate(proxy: ClientSubscriberProxy, message: ClientSubscriptionParamsMessage, promise: Promise[Boolean]): Unit = {
    logger.info(s"Subscribing to ${message.params}")
    if (aliveSubscribers.contains(proxy)) {

      subscriptionDb.remove(proxy)
      subscriptionDb.add(message.params, proxy)

      val current = currentSubscriptionSnapshot(message.params)
      proxy.notify(current)

      promise.success(true)
    }
  }

  private def onClientSubscriptionClosed(proxy: ClientSubscriberProxy): Unit = {
    aliveSubscribers -= proxy
    subscriptionDb.remove(proxy)
  }

  def channelSetHandler: EdgeServerChannelSetHandler = new EdgeServerChannelSetHandler {
    import EdgeChannels._
    def handle(channelSet: EdgeChannelSet): Unit = {
      channelSet match {
        case set: PublisherChannelSet =>
          onClientPublisherOpened(set.id, new ClientPublisherProxyImpl(selfMarshaller, set))
        case set: ClientSubscriberChannelSet =>
          onClientSubscriptionOpened(new ClientSubscriberProxyImpl(selfMarshaller, set))
        case set: ClientOutputIssuerSet =>
          onClientOutputOpened(new ClientOutputProxyImpl(selfMarshaller, set))
        case _ =>
      }

    }
  }

}
