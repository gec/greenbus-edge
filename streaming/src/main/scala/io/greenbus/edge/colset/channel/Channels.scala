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
package io.greenbus.edge.colset.channel

import io.greenbus.edge.channel._
import io.greenbus.edge.colset._
import io.greenbus.edge.flow.{ ReceiverChannel, SenderChannel }

object Channels {

  case class PeerLinkGroupKey(linkSession: PeerSessionId)

  sealed trait PeerLinkDesc {
    val linkSession: PeerSessionId
    def key: PeerLinkGroupKey = PeerLinkGroupKey(linkSession)
  }
  case class PeerSubscriptionSetSenderDesc(linkSession: PeerSessionId) extends ChannelDescriptor[SubscriptionSetUpdate] with PeerLinkDesc
  case class PeerEventReceiverDesc(linkSession: PeerSessionId) extends ChannelDescriptor[EventBatch] with PeerLinkDesc
  case class PeerServiceRequestsDesc(linkSession: PeerSessionId) extends ChannelDescriptor[ServiceRequestBatch] with PeerLinkDesc
  case class PeerServiceResponsesDesc(linkSession: PeerSessionId) extends ChannelDescriptor[ServiceResponseBatch] with PeerLinkDesc

  case class SubscriberGroupKey(correlation: String)

  sealed trait SubscriberDesc {
    val correlation: String
    def key: SubscriberGroupKey = SubscriberGroupKey(correlation)
  }
  case class SubSubscriptionSetDesc(correlation: String) extends ChannelDescriptor[SubscriptionSetUpdate] with SubscriberDesc
  case class SubEventReceiverDesc(correlation: String) extends ChannelDescriptor[EventBatch] with SubscriberDesc
  case class SubServiceRequestsDesc(correlation: String) extends ChannelDescriptor[ServiceRequestBatch] with SubscriberDesc
  case class SubServiceResponsesDesc(correlation: String) extends ChannelDescriptor[ServiceResponseBatch] with SubscriberDesc

  case class GatewayClientGroupKey(correlation: String)

  sealed trait GatewayClientDesc {
    val correlation: String
    def key: GatewayClientGroupKey = GatewayClientGroupKey(correlation)
  }
  case class GateSubscriptionSetSenderDesc(correlation: String) extends ChannelDescriptor[SubscriptionSetUpdate] with GatewayClientDesc
  case class GateEventReceiverDesc(correlation: String) extends ChannelDescriptor[GatewayClientEvents] with GatewayClientDesc
  case class GateServiceRequestsDesc(correlation: String) extends ChannelDescriptor[ServiceRequestBatch] with GatewayClientDesc
  case class GateServiceResponsesDesc(correlation: String) extends ChannelDescriptor[ServiceResponseBatch] with GatewayClientDesc

}

import io.greenbus.edge.colset.channel.Channels._
class PeerLinkAggregator(sessionId: PeerSessionId, handler: (PeerSessionId, PeerLinkProxyChannel) => Unit) extends CloseableAggregator {

  val subChannel = bucket[SenderChannel[SubscriptionSetUpdate, Boolean]]
  val eventChannel = bucket[ReceiverChannel[EventBatch, Boolean]]
  val serviceRequestsChannel = bucket[SenderChannel[ServiceRequestBatch, Boolean]]
  val serviceResponsesChannel = bucket[ReceiverChannel[ServiceResponseBatch, Boolean]]

  protected def onComplete(): Unit = {

  }
}
class SubscriberAggregator(correlation: String, handler: SubscriberProxyChannel => Unit) extends CloseableAggregator {

  val subChannel = bucket[ReceiverChannel[SubscriptionSetUpdate, Boolean]]
  val eventChannel = bucket[SenderChannel[EventBatch, Boolean]]
  val serviceRequestsChannel = bucket[ReceiverChannel[ServiceRequestBatch, Boolean]]
  val serviceResponsesChannel = bucket[SenderChannel[ServiceResponseBatch, Boolean]]

  protected def onComplete(): Unit = {
    val proxy = new SubscriberChannelProxyImpl(subChannel.get, eventChannel.get, serviceRequestsChannel.get, serviceResponsesChannel.get)
    handler(proxy)
  }
}
class GatewayAggregator(correlation: String, handler: GatewayClientProxyChannel => Unit) extends CloseableAggregator {

  val subChannel = bucket[SenderChannel[SubscriptionSetUpdate, Boolean]]
  val eventChannel = bucket[ReceiverChannel[GatewayClientEvents, Boolean]]
  val serviceRequestsChannel = bucket[SenderChannel[ServiceRequestBatch, Boolean]]
  val serviceResponsesChannel = bucket[ReceiverChannel[ServiceResponseBatch, Boolean]]

  protected def onComplete(): Unit = {
    handler(new GatewayClientProxyChannelImpl(subChannel.get, eventChannel.get, serviceRequestsChannel.get, serviceResponsesChannel.get))
  }
}

class ChannelHandler(handler: PeerChannelHandler) extends ChannelServerHandler {

  val peerLinkTable = AggregatorTable.forBuilder { key: PeerLinkGroupKey => new PeerLinkAggregator(key.linkSession, handler.peerOpened) }
  val subscriberTable = AggregatorTable.forBuilder { key: SubscriberGroupKey => new SubscriberAggregator(key.correlation, handler.subscriberOpened) }
  val gatewayTable = AggregatorTable.forBuilder { key: GatewayClientGroupKey => new GatewayAggregator(key.correlation, handler.gatewayClientOpened) }

  def handleReceiver[Message](desc: ChannelDescriptor[Message], channel: ReceiverChannel[Message, Boolean]): Unit = {
    desc match {
      case r: PeerEventReceiverDesc => peerLinkTable.handleForKey(r.key, channel) { agg => agg.eventChannel.put(channel) }
      case r: PeerServiceResponsesDesc => peerLinkTable.handleForKey(r.key, channel) { agg => agg.serviceResponsesChannel.put(channel) }
      case r: SubSubscriptionSetDesc => subscriberTable.handleForKey(r.key, channel) { agg => agg.subChannel.put(channel) }
      case r: SubServiceRequestsDesc => subscriberTable.handleForKey(r.key, channel) { agg => agg.serviceRequestsChannel.put(channel) }
      case r: GateEventReceiverDesc => gatewayTable.handleForKey(r.key, channel) { agg => agg.eventChannel.put(channel) }
      case r: GateServiceResponsesDesc => gatewayTable.handleForKey(r.key, channel) { agg => agg.serviceResponsesChannel.put(channel) }
    }
  }

  def handleSender[Message](desc: ChannelDescriptor[Message], channel: SenderChannel[Message, Boolean]): Unit = {
    desc match {
      case r: PeerSubscriptionSetSenderDesc => peerLinkTable.handleForKey(r.key, channel) { agg => agg.subChannel.put(channel) }
      case r: PeerServiceRequestsDesc => peerLinkTable.handleForKey(r.key, channel) { agg => agg.serviceRequestsChannel.put(channel) }
      case r: SubEventReceiverDesc => subscriberTable.handleForKey(r.key, channel) { agg => agg.eventChannel.put(channel) }
      case r: SubServiceResponsesDesc => subscriberTable.handleForKey(r.key, channel) { agg => agg.serviceResponsesChannel.put(channel) }
      case r: GateSubscriptionSetSenderDesc => gatewayTable.handleForKey(r.key, channel) { agg => agg.subChannel.put(channel) }
      case r: GateServiceRequestsDesc => gatewayTable.handleForKey(r.key, channel) { agg => agg.serviceRequestsChannel.put(channel) }
    }
  }
}
