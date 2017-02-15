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
package io.greenbus.edge.client

import java.util.UUID

import io.greenbus.edge._

import scala.concurrent.{ Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global

trait EdgeConnection extends EdgeSubscriptionClient {
  def connectPublisher(endpointId: EndpointId, sessionId: SessionId, publisher: EndpointPublisher): Future[EndpointPublisherConnection]
  def openSubscription(params: ClientSubscriptionParams): Future[EdgeSubscription]
  def openOutputClient(): Future[EdgeOutputClient]
}

class EdgeConnectionImpl(eventThread: CallMarshaller, client: EdgeChannelClient) extends EdgeConnection {

  def connectPublisher(endpointId: EndpointId, sessionId: SessionId, publisher: EndpointPublisher): Future[EndpointPublisherConnection] = {

    val sourceId = ClientSessionSourceId(sessionId)
    val pubId = EndpointPublisherId(sourceId, sessionId, endpointId)

    import EdgeChannels._
    val pubFut = client.openSender[EndpointPublishMessage, EndpointPublishDesc](EndpointPublishDesc(pubId))
    val outFut = client.openReceiver[PublisherOutputRequestMessage, PublisherOutputRequestDesc](PublisherOutputRequestDesc(pubId))
    val outRespFut = client.openSender[PublisherOutputResponseMessage, PublisherOutputResponseDesc](PublisherOutputResponseDesc(pubId))

    val resultFut = pubFut.zip(outFut).zip(outRespFut).map {
      case ((data, output), outResp) =>
        val channels = EndpointPublisherClientChannels(data, output, outResp)
        new EndpointPublisherConnectionImpl(eventThread, endpointId, publisher, channels)
    }

    resultFut.onFailure {
      case ex =>
        pubFut.onSuccess { case ch => ch.close() }
        outFut.onSuccess { case ch => ch.close() }
        outRespFut.onSuccess { case ch => ch.close() }
    }

    resultFut
  }

  def openSubscription(params: ClientSubscriptionParams): Future[EdgeSubscription] = {

    val correlator = UUID.randomUUID().toString

    import EdgeChannels._
    val paramFut = client.openSender[ClientSubscriptionParamsMessage, ClientSubscriptionRequestDesc](ClientSubscriptionRequestDesc(correlator))
    val notifyFut = client.openReceiver[ClientSubscriptionNotificationMessage, ClientSubscriptionNotificationDesc](ClientSubscriptionNotificationDesc(correlator))

    val resultFut = paramFut.zip(notifyFut).flatMap {
      case (param, notify) =>
        val channels = SubscriberChannels(param, notify)
        channels.params.sender.send(ClientSubscriptionParamsMessage(params)).map { success =>
          if (success) {
            new EdgeSubscriptionImpl(eventThread, channels)
          } else {
            throw new IllegalArgumentException("Subscription rejected")
          }

        }
    }

    resultFut.onFailure {
      case ex =>
        paramFut.onSuccess { case ch => ch.close() }
        notifyFut.onSuccess { case ch => ch.close() }
    }

    resultFut
  }

  def openOutputClient(): Future[EdgeOutputClient] = {
    val id = UUID.randomUUID()

    import EdgeChannels._
    val issueFut = client.openSender[ClientOutputRequestMessage, ClientOutputRequestDesc](ClientOutputRequestDesc(id))
    val responseFut = client.openReceiver[ClientOutputResponseMessage, ClientOutputResponseDesc](ClientOutputResponseDesc(id))

    val resultFut = issueFut.zip(responseFut).map {
      case (issue, response) =>
        val channels = OutputChannels(issue, response)
        new EdgeOutputClientImpl(eventThread, channels)
    }

    resultFut.onFailure {
      case ex =>
        issueFut.onSuccess { case ch => ch.close() }
        responseFut.onSuccess { case ch => ch.close() }
    }

    resultFut
  }
}
