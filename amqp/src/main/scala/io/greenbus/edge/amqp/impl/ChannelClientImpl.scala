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
package io.greenbus.edge.amqp.impl

import java.util.concurrent.TimeoutException

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.amqp.channel.impl.{ ClientReceiverChannelImpl, ClientSenderChannelImpl }
import io.greenbus.edge.amqp.channel.{ AmqpChannelDescriber, AmqpChannelInitiator, AmqpClientResponseParser }
import io.greenbus.edge.channel.{ ChannelClient, ChannelDescriptor, ChannelSerializationProvider }
import io.greenbus.edge.flow.{ Receiver => _, Sender => _, _ }
import io.greenbus.edge.thread.CallMarshaller
import org.apache.qpid.proton.engine._

import scala.concurrent.{ ExecutionContext, Future, Promise }

class ChannelClientImpl(
    ioThread: CallMarshaller,
    session: Session,
    describer: AmqpChannelDescriber,
    responseParser: AmqpClientResponseParser,
    serialization: ChannelSerializationProvider,
    promise: Promise[ChannelClient],
    parent: ResourceRemoveObserver) extends ChannelClient with HandlerResource with CloseObservable with LazyLogging {

  private val children = new ResourceContainer

  private val initiator = new AmqpChannelInitiator(describer, serialization)

  private val closeLatch = new RemoteSubscribedLatch(ioThread)

  private var opened = false

  def onClose: LatchSubscribable = closeLatch

  def handleParentClose(): Unit = {
    closeLatch()
    children.notifyOfClose()
  }

  def openSender[Message, Desc <: ChannelDescriptor[Message]](desc: Desc)(implicit ec: ExecutionContext): Future[(SenderChannel[Message, Boolean], ChannelDescriptor[Message])] = {
    val promise = Promise[(SenderChannel[Message, Boolean], ChannelDescriptor[Message])]
    ioThread.marshal {
      initiator.sender(session, desc) match {
        case None => promise.failure(throw new IllegalArgumentException("Channel type unrecognized"))
        case Some((s, serialize)) =>
          val channelProm = Promise[SenderChannel[Message, Boolean]]
          val impl = new ClientSenderChannelImpl[Message](ioThread, s, serialize, channelProm, children)
          s.setContext(impl.handler)

          children.add(impl)

          s.open()

          channelProm.future.map { ch =>
            responseParser.sender(desc, Option(s.getRemoteProperties)) match {
              case None =>
                ch.close()
                promise.failure(new IllegalArgumentException("Did not recognize response descriptor"))
              case Some(respDesc) => promise.success((ch, respDesc))
            }
          }
      }
    }
    promise.future
  }

  def openReceiver[Message, Desc <: ChannelDescriptor[Message]](desc: Desc)(implicit ec: ExecutionContext): Future[(ReceiverChannel[Message, Boolean], ChannelDescriptor[Message])] = {
    val promise = Promise[(ReceiverChannel[Message, Boolean], ChannelDescriptor[Message])]
    ioThread.marshal {
      initiator.receiver(session, desc) match {
        case None => promise.failure(throw new IllegalArgumentException("Channel type unrecognized"))
        case Some((r, deserialize)) =>

          val channelProm = Promise[ReceiverChannel[Message, Boolean]]
          val impl = new ClientReceiverChannelImpl[Message](ioThread, r, deserialize, channelProm, children)
          r.setContext(impl.handler)

          children.add(impl)

          r.open()
          r.flow(1024) // TODO: configurable

          channelProm.future.map { ch =>
            responseParser.receiver(desc, Option(r.getRemoteProperties)) match {
              case None =>
                ch.close()
                promise.failure(new IllegalArgumentException("Did not recognize response descriptor to: " + desc))
              case Some(respDesc) => promise.success((ch, respDesc))
            }
          }
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
      opened = true
      promise.success(self)
    }

    def onRemoteClose(s: Session): Unit = {
      if (!opened) {
        promise.failure(new TimeoutException("Transport closed"))
      }
      closeLatch()
      children.notifyOfClose()
      parent.handleChildRemove(self)
    }
  }
}

class ClientConnectionProtonHandler(id: String /*, sslOpt: Option[AmqpSslClientConfig], saslOpt: Option[SaslPlainCredentials]*/ ) extends BaseHandler with LazyLogging {
  add(new ContextualProtonHandler(id))

  /*override def onConnectionBound(e: Event): Unit = {
    val conn = e.getConnection

    saslOpt.foreach { creds =>
      val sasl = conn.getTransport.sasl()
      sasl.client()
      //sasl.setRemoteHostname("127.0.0.1")
      sasl.plain(creds.username, creds.password)
    }

    sslOpt.foreach { ssl =>
      val domain = Proton.sslDomain()
      domain.init(SslDomain.Mode.CLIENT)
      domain.setPeerAuthentication(SslDomain.VerifyMode.VERIFY_PEER)

      ssl.peerConfigOpt.foreach { cfg =>
        domain.setCredentials(cfg.publicKey, cfg.privateKey, cfg.passwordOpt.orNull)
      }
      ssl.caOpt.foreach(ca => domain.setTrustedCaDb(ca))

      conn.getTransport.ssl(domain)
    }
  }*/

  override def onConnectionRemoteOpen(e: Event): Unit = {
    e.getConnection.getContext match {
      case ctx: ConnectionContext => ctx.onOpen(e.getConnection)
      case _ => logger.warn(s"$id unknown context in connection remote open")
    }
  }

}
