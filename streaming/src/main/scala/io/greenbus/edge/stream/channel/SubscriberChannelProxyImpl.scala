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

class SubscriberChannelProxyImpl(
    subChannel: ReceiverChannel[SubscriptionSetUpdate, Boolean],
    eventChannel: SenderChannel[EventBatch, Boolean],
    serviceRequestsChannel: ReceiverChannel[ServiceRequestBatch, Boolean],
    serviceResponsesChannel: SenderChannel[ServiceResponseBatch, Boolean]) extends SubscriberProxyChannel with CloseableChannelAggregate {

  private val channels = Seq(subChannel, eventChannel, serviceRequestsChannel, serviceResponsesChannel)
  protected val closeableHolder = new CloseableHolder(channels)

  private val subDist = ChannelHelpers.wrapReceiver(subChannel, { msg: SubscriptionSetUpdate => msg.rows })

  private val eventSink = ChannelHelpers.bindSink(eventChannel, { seq: Seq[StreamEvent] => EventBatch(seq) })

  private val requestDist = ChannelHelpers.wrapReceiver(serviceRequestsChannel, { msg: ServiceRequestBatch => msg.requests })

  private val responseSink = ChannelHelpers.bindSink(serviceResponsesChannel, { seq: Seq[ServiceResponse] => ServiceResponseBatch(seq) })

  def subscriptions: Source[Set[RowId]] = subDist

  def events: Sink[Seq[StreamEvent]] = eventSink

  def requests: Source[Seq[ServiceRequest]] = requestDist

  def responses: Sink[Seq[ServiceResponse]] = responseSink
}
