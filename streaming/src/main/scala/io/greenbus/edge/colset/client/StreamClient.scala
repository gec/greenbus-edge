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
package io.greenbus.edge.colset.client

import java.util.UUID

import io.greenbus.edge.channel.ChannelClient
import io.greenbus.edge.colset._
import io.greenbus.edge.colset.channel.{ GatewayProxyChannelImpl, PeerLinkProxyChannelImpl }
import io.greenbus.edge.flow.{ CloseObservable, LatchSubscribable }

import scala.concurrent.{ ExecutionContext, Future }

trait StreamClient extends CloseObservable {
  def openGatewayChannel(): Future[GatewayProxyChannel]
  def openPeerLinkClient(): Future[(PeerSessionId, PeerLinkProxyChannel)]
}

class MultiChannelStreamClientImpl(client: ChannelClient)(implicit val ex: ExecutionContext) extends StreamClient {
  import io.greenbus.edge.colset.channel.Channels._

  def onClose: LatchSubscribable = client.onClose

  def openGatewayChannel(): Future[GatewayProxyChannel] = {
    val correlator = UUID.randomUUID().toString
    val subFut = client.openReceiver[SubscriptionSetUpdate, GateSubscriptionSetSenderDesc](GateSubscriptionSetSenderDesc(correlator))
    val eventFut = client.openSender[GatewayClientEvents, GateEventReceiverDesc](GateEventReceiverDesc(correlator))
    val reqFut = client.openReceiver[ServiceRequestBatch, GateServiceRequestsDesc](GateServiceRequestsDesc(correlator))
    val respFut = client.openSender[ServiceResponseBatch, GateServiceResponsesDesc](GateServiceResponsesDesc(correlator))

    val result = for {
      (sub, subDesc) <- subFut
      (event, eventDesc) <- eventFut
      (req, reqDesc) <- reqFut
      (resp, respDesc) <- respFut
    } yield {
      new GatewayProxyChannelImpl(sub, event, req, resp)
    }

    result.failed.foreach { _ => Seq(subFut, eventFut, reqFut, respFut).foreach(_.foreach(_._1.close())) }

    result
  }

  def openPeerLinkClient(): Future[(PeerSessionId, PeerLinkProxyChannel)] = {
    val correlator = UUID.randomUUID().toString
    val subFut = client.openSender[SubscriptionSetUpdate, SubSubscriptionSetDesc](SubSubscriptionSetDesc(correlator))
    val eventFut = client.openReceiver[EventBatch, SubEventReceiverDesc](SubEventReceiverDesc(correlator))
    val reqFut = client.openSender[ServiceRequestBatch, SubServiceRequestsDesc](SubServiceRequestsDesc(correlator))
    val respFut = client.openReceiver[ServiceResponseBatch, SubServiceResponsesDesc](SubServiceResponsesDesc(correlator))

    val result = for {
      (sub, subDesc) <- subFut
      (event, eventDesc) <- eventFut
      (req, reqDesc) <- reqFut
      (resp, respDesc) <- respFut
    } yield {
      val session = subDesc match {
        case d: PeerSubscriptionSetSenderDesc => d.linkSession
        case _ => throw new IllegalArgumentException("Unrecognized channel response: " + subDesc)
      }

      (session, new PeerLinkProxyChannelImpl(sub, event, req, resp))
    }

    result.failed.foreach { _ => Seq(subFut, eventFut, reqFut, respFut).foreach(_.foreach(_._1.close())) }

    result
  }
}