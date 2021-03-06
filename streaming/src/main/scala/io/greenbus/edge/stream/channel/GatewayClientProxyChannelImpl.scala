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
package io.greenbus.edge.stream.channel

import io.greenbus.edge.flow._
import io.greenbus.edge.stream._

class GatewayClientProxyChannelImpl(
    subChannel: SenderChannel[SubscriptionSetUpdate, Boolean],
    eventChannel: ReceiverChannel[GatewayClientEvents, Boolean],
    serviceRequestsChannel: SenderChannel[ServiceRequestBatch, Boolean],
    serviceResponsesChannel: ReceiverChannel[ServiceResponseBatch, Boolean]) extends GatewayClientProxyChannel with CloseableChannelAggregate {

  private val channels = Seq(subChannel, eventChannel, serviceRequestsChannel, serviceResponsesChannel)
  protected val closeableHolder = new CloseableHolder(channels)

  private val subSink = ChannelHelpers.bindSink(subChannel, { set: Set[RowId] => SubscriptionSetUpdate(set) })

  private val eventDist = ChannelHelpers.wrapReceiver(eventChannel, { msg: GatewayClientEvents => GatewayEvents(msg.routesUpdate, msg.events) })

  private val requestSink = ChannelHelpers.bindSink(serviceRequestsChannel, { seq: Seq[ServiceRequest] => ServiceRequestBatch(seq) })

  private val responseDist = ChannelHelpers.wrapReceiver(serviceResponsesChannel, { msg: ServiceResponseBatch => msg.responses })

  def subscriptions: Sink[Set[RowId]] = subSink

  def events: Source[GatewayEvents] = eventDist

  def requests: Sink[Seq[ServiceRequest]] = requestSink

  def responses: Source[Seq[ServiceResponse]] = responseDist

}
