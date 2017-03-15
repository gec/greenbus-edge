package io.greenbus.edge.amqp.impl2

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.CallMarshaller
import io.greenbus.edge.amqp.channel.impl.{ClientReceiverChannelImpl, ClientSenderChannelImpl}
import io.greenbus.edge.amqp.channel.{AmqpChannelDescriber, AmqpChannelInitiator}
import io.greenbus.edge.amqp.impl.{HandlerResource, ResourceContainer, ResourceRemoveObserver, SessionContext}
import io.greenbus.edge.channel2.{ChannelClient, ChannelDescriptor, ChannelSerializationProvider}
import io.greenbus.edge.flow.{ReceiverChannel, SenderChannel}
import org.apache.qpid.proton.engine.{Receiver, Sender, Session}

import scala.concurrent.{Future, Promise}

class AmqpChannelClient {

}


class ChannelClientImpl(
                         ioThread: CallMarshaller,
                         session: Session,
                         describer: AmqpChannelDescriber,
                         serialization: ChannelSerializationProvider,
                         promise: Promise[ChannelClient],
                         parent: ResourceRemoveObserver) extends ChannelClient with HandlerResource with LazyLogging {

  private val children = new ResourceContainer

  private val initiator = new AmqpChannelInitiator(describer, serialization)

  def handleParentClose(): Unit = {
    children.notifyOfClose()
  }

  def openSender[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[SenderChannel[Message, Boolean]] = {
    val promise = Promise[SenderChannel[Message, Boolean]]
    ioThread.marshal {
      initiator.sender(session, desc) match {
        case None => promise.failure(throw new IllegalArgumentException("Channel type unrecognized"))
        case Some((s, serialize)) =>
          val impl = new ClientSenderChannelImpl[Message](ioThread, s, serialize, promise, children)
          s.setContext(impl.handler)

          children.add(impl)

          s.open()
      }
    }
    promise.future
  }

  def openReceiver[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[ReceiverChannel[Message, Boolean]] = {
    val promise = Promise[ReceiverChannel[Message, Boolean]]
    ioThread.marshal {
      initiator.receiver(session, desc) match {
        case None => promise.failure(throw new IllegalArgumentException("Channel type unrecognized"))
        case Some((r, deserialize)) =>

          val impl = new ClientReceiverChannelImpl[Message](ioThread, r, deserialize, promise, children)
          r.setContext(impl.handler)

          children.add(impl)

          r.open()
          r.flow(1024) // TODO: configurable
      }
    }
    promise.future
  }

  private val self = this
  val handler = new SessionContext {

    def onSenderRemoteOpen(s: Sender): Unit = {
      // TODO: verify the protocol behavior here
      /*s.detach()
      s.close()*/
    }

    def onReceiverRemoteOpen(r: Receiver): Unit = {
      /*r.detach()
      r.close()*/
    }

    def onOpen(s: Session): Unit = {
      promise.success(self)
    }

    def onRemoteClose(s: Session): Unit = {
      children.notifyOfClose()
      parent.handleChildRemove(self)
    }
  }
}