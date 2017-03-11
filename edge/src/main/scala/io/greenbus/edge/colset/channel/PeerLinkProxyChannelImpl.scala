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

import io.greenbus.edge.colset._
import io.greenbus.edge.flow._

import scala.util.Try

class ReceiverToSource[A](rcv: Receiver[A, Boolean]) extends Source[A] {
  private val dist = new QueuedDistributor[A]
  rcv.bind(new Responder[A, Boolean] {
    def handle(obj: A, respond: (Boolean) => Unit): Unit = {
      dist.push(obj)
      respond(true)
    }
  })

  def bind(handler: Handler[A]): Unit = dist.bind(handler)
}

class SenderToSink[A](snd: Sender[A, Boolean]) extends Sink[A] {
  def push(obj: A): Unit = {

    def handleResp(resp: Try[Boolean]): Unit = {}

    snd.send(obj, handleResp)
  }
}

class PeerLinkProxyChannelImpl(
    subChannel: SenderChannel[SubscriptionSetUpdate, Boolean],
    eventChannel: ReceiverChannel[EventBatch, Boolean],
    serviceRequestsChannel: SenderChannel[ServiceRequestBatch, Boolean],
    serviceResponsesChannel: ReceiverChannel[ServiceResponseBatch, Boolean]) extends PeerLinkProxyChannel {

  private val channels = Seq(subChannel, eventChannel, serviceRequestsChannel, serviceResponsesChannel)
  private val closeLatch = new SingleThreadedLatchSubscribable
  private val channelCloseSubs = channels.map(_.onClose.subscribe(() => onSubChannelClosed()))

  private def onSubResult(result: Try[Boolean]): Unit = {}
  private val subSink = Sink[Set[RowId]] { set => subChannel.send(SubscriptionSetUpdate(set), onSubResult) }

  private val eventDist = new QueuedDistributor[Seq[StreamEvent]]
  eventChannel.bind(new Responder[EventBatch, Boolean] {
    def handle(obj: EventBatch, respond: (Boolean) => Unit): Unit = {
      eventDist.push(obj.events)
      respond(true)
    }
  })

  private def onReqResult(result: Try[Boolean]): Unit = {}
  private val requestSink = Sink[Seq[ServiceRequest]] { reqs => serviceRequestsChannel.send(ServiceRequestBatch(reqs), onReqResult) }

  private val responseDist = new QueuedDistributor[Seq[ServiceResponse]]
  serviceResponsesChannel.bind(new Responder[ServiceResponseBatch, Boolean] {
    def handle(obj: ServiceResponseBatch, respond: (Boolean) => Unit): Unit = {
      responseDist.push(obj.responses)
      respond(true)
    }
  })

  def subscriptions: Sink[Set[RowId]] = subSink

  def events: Source[Seq[StreamEvent]] = eventDist

  def requests: Sink[Seq[ServiceRequest]] = requestSink

  def responses: Source[Seq[ServiceResponse]] = responseDist

  private def onSubChannelClosed(): Unit = {
    onClose
    channels.foreach(_.close())
    channelCloseSubs.foreach(_.close())
  }

  def close(): Unit = {
    closeLatch()
  }

  def onClose: LatchSubscribable = closeLatch
}
