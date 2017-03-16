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
package io.greenbus.edge.amqp.channel.impl

import java.util.concurrent.TimeoutException

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.amqp.impl._
import io.greenbus.edge.{CallMarshaller, flow}
import io.greenbus.edge.flow._
import org.apache.qpid.proton.engine.{Delivery, Link, Receiver, Sender}

import scala.concurrent.Promise
import scala.util.{Failure, Try}

class ServerSenderChannelImpl[A](
  ioThread: CallMarshaller,
  s: Sender,
  serializer: A => (Array[Byte], Int),
  parent: ResourceRemoveObserver)
    extends BaseSenderChannelImpl(ioThread, s, serializer, startOpened = true, parent) {

  protected def handleOpenTransition(): Unit = {}
  protected def handleClosedBeforeOpen(): Unit = {}
}

class ClientSenderChannelImpl[A](
  ioThread: CallMarshaller,
  s: Sender,
  serializer: A => (Array[Byte], Int),
  promise: Promise[SenderChannel[A, Boolean]],
  parent: ResourceRemoveObserver)
    extends BaseSenderChannelImpl(ioThread, s, serializer, startOpened = false, parent) {

  protected def handleOpenTransition(): Unit = {
    promise.success(this)
  }

  protected def handleClosedBeforeOpen(): Unit = {
    promise.failure(new TimeoutException())
  }
}

abstract class BaseSenderChannelImpl[A](
  ioThread: CallMarshaller,
  s: Sender,
  serializer: A => (Array[Byte], Int),
  startOpened: Boolean,
  parent: ResourceRemoveObserver)
    extends CloseableChannel(ioThread, startOpened, parent, s) with SenderChannel[A, Boolean] with HandlerResource {

  private val deliverySequencer = new DeliverySequencer()

  private val sndImpl: flow.Sender[A, Boolean] = (obj: A, handleResponse: Try[Boolean] => Unit) => {
    try {
      val (data, length) = serializer(obj)
      ioThread.marshal {
        val delivery = s.delivery(deliverySequencer.next())

        // TODO: keep track of outstanding deliveries to cancel? or do it through the sender's iterator?
        delivery.setContext(new SentTransferDeliveryContextTry(handleResponse))

        s.send(data, 0, length)

        s.advance()
      }
    } catch {
      case ex: Throwable =>
        handleResponse(Failure(ex))
    }
  }

  def send(obj: A, handleResponse: (Try[Boolean]) => Unit): Unit = {
    sndImpl.send(obj, handleResponse)
  }

  val handler: SenderContext = new SenderContext {
    def onDelivery(s: Sender, d: Delivery): Unit = {}

    def linkFlow(s: Sender): Unit = {}

    def onOpen(l: Link): Unit = {
      handleSelfOpen()
    }

    def onRemoteClose(l: Link): Unit = {
      handleSelfClose()
    }
  }

}

class ServerReceiverChannelImpl[A](
  ioThread: CallMarshaller,
  s: Receiver,
  deserializer: Array[Byte] => Option[A],
  parent: ResourceRemoveObserver)
    extends BaseReceiverChannelImpl(ioThread, s, deserializer, startOpened = true, parent) {

  protected def handleOpenTransition(): Unit = {}
  protected def handleClosedBeforeOpen(): Unit = {}
}

class ClientReceiverChannelImpl[A](
  ioThread: CallMarshaller,
  s: Receiver,
  deserializer: Array[Byte] => Option[A],
  promise: Promise[ReceiverChannel[A, Boolean]],
  parent: ResourceRemoveObserver)
    extends BaseReceiverChannelImpl(ioThread, s, deserializer, startOpened = false, parent) {

  protected def handleOpenTransition(): Unit = {
    promise.success(this)
  }

  protected def handleClosedBeforeOpen(): Unit = {
    promise.failure(new TimeoutException())
  }
}

abstract class BaseReceiverChannelImpl[A](
  ioThread: CallMarshaller,
  s: Receiver,
  deserializer: Array[Byte] => Option[A],
  startOpened: Boolean,
  parent: ResourceRemoveObserver)
    extends CloseableChannel(ioThread, startOpened, parent, s) with ReceiverChannel[A, Boolean] with LazyLogging {

  private val rcvImpl = new QueueingReceiverImpl[A, Boolean](ioThread)

  def bind(responder: Responder[A, Boolean]): Unit = rcvImpl.bind(responder)

  private val self = this

  val handler: ReceiverContext = new ReceiverContext {
    def onDelivery(r: Receiver, d: Delivery): Unit = {

      if (d.isReadable && !d.isPartial) {
        val pending = d.pending()
        val buffer = new Array[Byte](pending)
        val read = r.recv(buffer, 0, buffer.length)
        r.advance()

        try {
          deserializer(buffer) match {
            case None => logger.warn("Deserialization failed in receiver channel")
            case Some(obj) => {

              def onResponse(resp: Boolean): Unit = {
                ioThread.marshal { d.settle() } // TODO: distinguish delivery state?
              }

              rcvImpl.handle(obj, onResponse)
            }
          }

        } catch {
          case ex: Throwable =>
            logger.warn("Delivery parsing failed: " + ex)
        }

        r.flow(pending)
      }
    }

    def onOpen(l: Link): Unit = {
      handleSelfOpen()
    }

    def onRemoteClose(l: Link): Unit = {
      handleSelfClose()
    }
  }
}

abstract class CloseableChannel(ioThread: CallMarshaller, startOpen: Boolean, parent: ResourceRemoveObserver, link: Link) extends Closeable with CloseObservable with HandlerResource {

  private var opened = startOpen
  private var userClosed = false
  protected val closeSource = new LocallyAppliedLatchSubscribable(ioThread)

  protected def handleOpenTransition(): Unit
  protected def handleClosedBeforeOpen(): Unit

  protected def handleSelfOpen(): Unit = {
    if (!opened && !userClosed) {
      opened = true
      handleOpenTransition()
    }
  }

  private def handleUserClose(): Unit = {
    ioThread.marshal {
      userClosed = true
      link.close()
      closeSource()
      parent.handleChildRemove(this)
    }
  }

  protected def handleSelfClose(): Unit = {
    closeSource()
    if (!opened && !userClosed) {
      handleClosedBeforeOpen()
    }
    parent.handleChildRemove(this)
  }

  def handleParentClose(): Unit = {
    closeSource()
    if (!opened && !userClosed) {
      handleClosedBeforeOpen()
    }
  }

  def close(): Unit = {
    handleUserClose()
  }

  def onClose: LatchSubscribable = closeSource
}
