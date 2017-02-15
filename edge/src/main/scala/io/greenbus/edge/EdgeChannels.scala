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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.channel._

import scala.concurrent.Future

trait ChannelDescriptor[Message] {
  type Sender = TransferChannelSender[Message, Boolean]
  type Receiver = TransferChannelReceiver[Message, Boolean]
}

case class EndpointPublisherId(sourceId: SourceId, sessionId: SessionId, endpointId: EndpointId)

trait ServerConnectionAuthContext
object EmptyServerConnectionAuthContext extends ServerConnectionAuthContext

trait ClientChannelAuthContext
object EmptyClientAuthContext extends ClientChannelAuthContext

case class ServerChannelAuthContext(connectionAuthContext: ServerConnectionAuthContext, clientChannelAuthContext: ClientChannelAuthContext)

sealed trait EdgeChannelSet

object EdgeChannels {

  case class ClientSubscriptionRequestDesc(correlator: String) extends ChannelDescriptor[ClientSubscriptionParamsMessage]
  case class ClientSubscriptionNotificationDesc(correlator: String) extends ChannelDescriptor[ClientSubscriptionNotificationMessage]

  case class ClientOutputRequestDesc(issuerId: UUID) extends ChannelDescriptor[ClientOutputRequestMessage]
  case class ClientOutputResponseDesc(issuerId: UUID) extends ChannelDescriptor[ClientOutputResponseMessage]

  case class EndpointPublishDesc(key: EndpointPublisherId) extends ChannelDescriptor[EndpointPublishMessage]
  case class PublisherOutputRequestDesc(key: EndpointPublisherId) extends ChannelDescriptor[PublisherOutputRequestMessage]
  case class PublisherOutputResponseDesc(key: EndpointPublisherId) extends ChannelDescriptor[PublisherOutputResponseMessage]

  case class ClientSubscriberChannelSet(
    paramsChannel: TransferChannelReceiver[ClientSubscriptionParamsMessage, Boolean],
    notificationChannel: TransferChannelSender[ClientSubscriptionNotificationMessage, Boolean]) extends EdgeChannelSet

  case class ClientOutputIssuerSet(
    issuerId: UUID,
    issueChannel: TransferChannelReceiver[ClientOutputRequestMessage, Boolean],
    responseChannel: TransferChannelSender[ClientOutputResponseMessage, Boolean]) extends EdgeChannelSet

  case class PublisherChannelSet(
    id: EndpointPublisherId,
    publishChannel: TransferChannelReceiver[EndpointPublishMessage, Boolean],
    outputIssueChannel: TransferChannelSender[PublisherOutputRequestMessage, Boolean],
    outputResponseChannel: TransferChannelReceiver[PublisherOutputResponseMessage, Boolean]) extends EdgeChannelSet

  class SubscriberAggregator(protected val complete: ClientSubscriberChannelSet => Unit)
      extends ChannelAggregator[ClientSubscriberChannelSet] {

    val requestChannel = receiver[ClientSubscriptionParamsMessage, Boolean]
    val notifyChannel = sender[ClientSubscriptionNotificationMessage, Boolean]

    protected def result(): ClientSubscriberChannelSet = {
      ClientSubscriberChannelSet(requestChannel.get, notifyChannel.get)
    }
  }

  class OutputIssueAggregator(issuerId: UUID, protected val complete: ClientOutputIssuerSet => Unit)
      extends ChannelAggregator[ClientOutputIssuerSet] {

    val issueChannel = receiver[ClientOutputRequestMessage, Boolean]
    val responseChannel = sender[ClientOutputResponseMessage, Boolean]

    protected def result(): ClientOutputIssuerSet = {
      ClientOutputIssuerSet(issuerId, issueChannel.get, responseChannel.get)
    }
  }

  class PublishAggregator(
    id: EndpointPublisherId,
    protected val complete: PublisherChannelSet => Unit)
      extends ChannelAggregator[PublisherChannelSet] {

    val pubChannel = receiver[EndpointPublishMessage, Boolean]
    val controlIssueChannel = sender[PublisherOutputRequestMessage, Boolean]
    val controlResponseChannel = receiver[PublisherOutputResponseMessage, Boolean]

    protected def result(): PublisherChannelSet = {
      PublisherChannelSet(id, pubChannel.get, controlIssueChannel.get, controlResponseChannel.get)
    }
  }
}

import EdgeChannels._
trait EdgeChannelClient {
  def openSender[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[TransferChannelSender[Message, Boolean]]
  def openReceiver[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[TransferChannelReceiver[Message, Boolean]]
}

class PublisherAggregator

trait EdgeServerChannelSetHandler {
  def handle(set: EdgeChannelSet)
}

trait EdgeServerChannelHandler {
  def handleReceiver[Message](desc: ChannelDescriptor[Message], channel: TransferChannelReceiver[Message, Boolean]): Unit
  def handleSender[Message](desc: ChannelDescriptor[Message], channel: TransferChannelSender[Message, Boolean]): Unit
}

class EdgeServerChannelHandlerImpl(handler: EdgeServerChannelSetHandler) extends EdgeServerChannelHandler with LazyLogging {

  private val publishTable = new BuilderTableImpl[EndpointPublisherId, PublisherChannelSet, PublishAggregator](key => new PublishAggregator(key, handler.handle))
  private val subscribeTable = new BuilderTableImpl[String, ClientSubscriberChannelSet, SubscriberAggregator](key => new SubscriberAggregator(handler.handle))
  private val outputIssueTable = new BuilderTableImpl[UUID, ClientOutputIssuerSet, OutputIssueAggregator](key => new OutputIssueAggregator(key, handler.handle))

  def handleReceiver[Message](desc: ChannelDescriptor[Message], channel: TransferChannelReceiver[Message, Boolean]): Unit = {
    logger.debug(s"Peer handling receiver channel $desc")
    desc match {
      case d: EndpointPublishDesc => publishTable.handleForKey(d.key, channel, b => b.pubChannel.put(channel))
      case d: PublisherOutputResponseDesc => publishTable.handleForKey(d.key, channel, b => b.controlResponseChannel.put(channel))
      case d: ClientSubscriptionRequestDesc => subscribeTable.handleForKey(d.correlator, channel, b => b.requestChannel.put(channel))
      case d: ClientOutputRequestDesc => outputIssueTable.handleForKey(d.issuerId, channel, b => b.issueChannel.put(channel))
      case _ =>
        throw new IllegalArgumentException("Unrecognized receiver descriptor in peer: " + desc)
    }
  }
  def handleSender[Message](desc: ChannelDescriptor[Message], channel: TransferChannelSender[Message, Boolean]): Unit = {
    logger.debug(s"Peer handling sender channel $desc")
    desc match {
      case d: PublisherOutputRequestDesc => publishTable.handleForKey(d.key, channel, b => b.controlIssueChannel.put(channel))
      case d: ClientSubscriptionNotificationDesc => subscribeTable.handleForKey(d.correlator, channel, b => b.notifyChannel.put(channel))
      case d: ClientOutputResponseDesc => outputIssueTable.handleForKey(d.issuerId, channel, b => b.responseChannel.put(channel))
      case _ =>
        throw new IllegalArgumentException("Unrecognized sender descriptor in peer: " + desc)
    }
  }

}
