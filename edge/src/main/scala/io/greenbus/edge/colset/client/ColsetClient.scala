package io.greenbus.edge.colset.client

import java.util.UUID

import io.greenbus.edge.channel2.ChannelClient
import io.greenbus.edge.colset.channel.{GatewayClientProxyChannelImpl, GatewayProxyChannelImpl}
import io.greenbus.edge.colset._

import scala.concurrent.{ExecutionContext, Future}


trait ColsetClient {
  def openGatewayClient(): Future[GatewayProxyChannel]
  def openGatewayClientProxy(): Future[GatewayClientProxyChannel]

  def openPeerLinkClient(): Future[PeerLinkProxyChannel]

  def openSubscriberProxyClient(): Future[SubscriberProxyChannel]
}

class MultiChannelColsetClientImpl(client: ChannelClient)(implicit val ex: ExecutionContext) extends ColsetClient {
  import io.greenbus.edge.colset.channel.Channels._

  def openGatewayClient(): Future[GatewayProxyChannel] = {
    val correlator = UUID.randomUUID().toString
    val subFut = client.openReceiver[SubscriptionSetUpdate, GateSubscriptionSetSenderDesc](GateSubscriptionSetSenderDesc(correlator))
    val eventFut = client.openSender[GatewayClientEvents, GateEventReceiverDesc](GateEventReceiverDesc(correlator))
    val reqFut = client.openReceiver[ServiceRequestBatch, GateServiceRequestsDesc](GateServiceRequestsDesc(correlator))
    val respFut = client.openSender[ServiceResponseBatch, GateServiceResponsesDesc](GateServiceResponsesDesc(correlator))

    val result = for {
      sub <- subFut
      event <- eventFut
      req <- reqFut
      resp <- respFut
    } yield {
      new GatewayProxyChannelImpl(sub, event, req, resp)
    }

    result.failed.foreach { _ => Seq(subFut, eventFut, reqFut, respFut).foreach(_.foreach(_.close())) }

    result
  }

  def openGatewayClientProxy(): Future[GatewayClientProxyChannel] = ???

  def openPeerLinkClient(): Future[PeerLinkProxyChannel] = ???

  def openSubscriberProxyClient(): Future[SubscriberProxyChannel] = ???
}