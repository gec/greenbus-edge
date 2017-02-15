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

import java.util.concurrent.TimeoutException

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.channel.Responder

import scala.concurrent.Promise
import scala.util.{ Failure, Try }

trait SourceId
case class ClientSessionSourceId(sessionId: SessionId) extends SourceId

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
  private var endpoints = Map.empty[EndpointId, EndpointDb]
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

    ClientSubscriptionNotification(Seq(), infoNotifications, dataNotifications, outputNotifications)
  }

  private def handleBatchPublish(sourceId: SourceId, sessionId: SessionId, endpointId: EndpointId, batch: EndpointPublishMessage): Unit = {
    val processResultOpt = endpoints.get(endpointId).map { db => db.processBatch(sourceId, sessionId, batch) }
    processResultOpt.foreach { result =>

      // TODO: transaction system where during transaction sub db puts stuff into buckets?
      var notifyMap = Set.empty[SubscriptionTarget]
      var infoMap = Map.empty[SubscriptionTarget, EndpointDescriptorNotification]
      var dataMap = Map.empty[SubscriptionTarget, Seq[EndpointDataNotification]]
      var outputMap = Map.empty[SubscriptionTarget, Seq[EndpointOutputStatusNotification]]

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
        val infoOpt = infoMap.get(target)
        val dataOpt = dataMap.get(target)
        val outputOpt = outputMap.get(target)
        val infoSeq = infoOpt.map(Seq(_)).getOrElse(Seq())
        val dataSeq = dataOpt.getOrElse(Seq())
        val outputSeq = outputOpt.getOrElse(Seq())
        val message = ClientSubscriptionNotification(Seq(), infoSeq, dataSeq, outputSeq)
        target.notify(message)
      }
    }
  }

  def onPeerInputChannelOpened() = ???
  def onPeerInputChannelClosed() = ???

  def onClientPublish(id: EndpointPublisherId, batch: EndpointPublishMessage, promise: Promise[Boolean]): Unit = {
    logger.info("got client publish: " + batch)
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
          endpoints += (endpointId -> db)
          doOpen()
        } else {
          logger.warn(s"Endpoint publisher for $id was disallowed")
          doReject()
        }
      }
      case Some(db) => {
        if (db.allowPublisher(sourceId, sessionId)) {
          doOpen()
        } else {
          logger.warn(s"Endpoint publisher for $id was disallowed")
          doReject()
        }
      }
    }

  }

  def onClientPublisherClosed(id: EndpointPublisherId, proxy: ClientPublisherProxy): Unit = {
    logger.info("Publisher closed: " + id)
    proxy.close()
    clientPublishers.remove(proxy)
  }

  def onClientPublisherClosed(sourceId: SourceId, sessionId: SessionId, endpointId: EndpointId): Unit = {
    logger.info(s"Client publisher closed for $sourceId, $sessionId, $endpointId")
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

/*
a node has:
- a set of user links, some of which are data sources, some are user subscriptions
- "source endpoints" it is the gateway for
- a set of links to peer nodes
- a gossip manifest of endpoints accessible from other nodes (and itself)
- a map of endpoints to active sessions providing their source
- a set of endpoint key subscriptions maintained on behalf of users or peers
- a set of endpoint data subscriptions maintained on behalf of users or peers

channel:
- local recv (sub)
  - endpoint key sub list
  - endpoint data sub list
  - unsync'ed endpoint list
- remote recv (pub)
  - sub list
  - remote endpoint syncs

on established
--> gossip set
<--


sync sessions or endpoints?
sessions... endpoints might have  other session sources that are fine
a zombie session may keep "sending" valid sequences, this is fine and actually backfill...

peer's view of:
------------------
link to session
  - lifetime: life of link or until remote peer has no record of session
record of session
  - lifetime:
data session streams
  - lifetime: until all links have no further record of session
  - lifetime: or until all subs disappear

backfill updates, marked not-up-to-date?

publish message:
- endpoint update session-id
  - data updates with individual seqs

db:
==================
- set: input channels
  - input channel
    - active sessions
      - sync state
    - endpoint, session map

- set: endpoints
  - endpoint
    - active primary session
      - data series
    - unsynced sessions (shown up, can't be ordered, awaiting sync)
      - data series
    - latent sessions (replaced as active, still alive in some peers)
      - data series

- set: output channel
  - output channel
    - subs

value streams have a resume point that varies based on stream type?
 */
