package io.greenbus.edge.channel2

import io.greenbus.edge.flow.{ReceiverChannel, SenderChannel}

import scala.concurrent.Future

trait ChannelClient {
  def openSender[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[SenderChannel[Message, Boolean]]
  def openReceiver[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[ReceiverChannel[Message, Boolean]]
}
