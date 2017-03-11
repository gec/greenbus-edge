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

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.TimeoutException

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.amqp.{ ChannelSessionSource, EdgeAmqpChannelInitiator, EdgeChannelInitiatorImpl }
import io.greenbus.edge.channel.{ Receiver => _, Sender => _, _ }
import io.greenbus.edge.proto.provider.EdgeProtobufProvider
import io.greenbus.edge.{ CallMarshaller, ChannelDescriptor, EdgeChannelClient, channel }
import org.apache.qpid.proton.engine.{ Receiver, _ }

import scala.concurrent.{ Future, Promise }
import scala.util.{ Success, Try }

class DeliverySequencer {
  private var deliverySequence: Long = 0

  def next(): Array[Byte] = {
    val bb = ByteBuffer.allocate(java.lang.Long.BYTES)
    bb.putLong(deliverySequence)
    deliverySequence += 1
    bb.array()
  }
}

class SentTransferDeliveryContext(promise: Promise[Boolean]) extends SenderDeliveryContext with LazyLogging {
  def onDelivery(s: Sender, delivery: Delivery): Unit = {
    logger.trace("Got delivery update: " + delivery)
    if (!delivery.isSettled && delivery.remotelySettled()) {
      delivery.settle()
      promise.success(true)
    }
  }
}

class SentTransferDeliveryContextTry(handler: Try[Boolean] => Unit) extends SenderDeliveryContext with LazyLogging {
  def onDelivery(s: Sender, delivery: Delivery): Unit = {
    logger.trace("Got delivery update: " + delivery)
    if (!delivery.isSettled && delivery.remotelySettled()) {
      delivery.settle()
      handler(Success(true))
    }
  }
}

class ChannelClientImpl(
    ioThread: CallMarshaller,
    session: Session,
    promise: Promise[EdgeChannelClient],
    parent: ResourceRemoveObserver) extends EdgeChannelClient with HandlerResource with LazyLogging {
  private val children = new ResourceContainer

  private val channelMapping: EdgeAmqpChannelInitiator = new EdgeChannelInitiatorImpl(new EdgeProtobufProvider)

  def handleParentClose(): Unit = {
    children.notifyOfClose()
  }

  def openSender[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[TransferChannelSender[Message, Boolean]] = {
    val promise = Promise[TransferChannelSender[Message, Boolean]]
    ioThread.marshal {
      channelMapping.sender(session, desc) match {
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

  def openReceiver[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[TransferChannelReceiver[Message, Boolean]] = {
    val promise = Promise[TransferChannelReceiver[Message, Boolean]]
    ioThread.marshal {

      channelMapping.receiver(session, desc) match {
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

class ChannelSessionSourceImpl(ioThread: CallMarshaller, c: Connection, promise: Promise[ChannelSessionSource]) extends ChannelSessionSource {
  private val children = new ResourceContainer

  def open(): Future[EdgeChannelClient] = {
    val promise = Promise[EdgeChannelClient]
    ioThread.marshal {
      val sess = c.session()
      val impl = new ChannelClientImpl(ioThread, sess, promise, children)
      children.add(impl)
      sess.setContext(impl.handler)
      sess.open()
    }
    promise.future
  }

  private val self = this
  val handler = new ConnectionContext {
    def onSessionRemoteOpen(s: Session): Unit = {
      // TODO: verify the protocol behavior here
      //s.close()
    }

    def onOpen(c: Connection): Unit = {
      promise.success(self)
    }

    def onRemoteClose(c: Connection): Unit = {
      children.notifyOfClose()
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