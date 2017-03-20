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
package io.greenbus.edge.channel

import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ Future, Promise }

class LocallyAppliedLatchSource(self: CallMarshaller) extends LatchSource with LatchSink {
  private var latched = false
  private var handlerOpt = Option.empty[LatchHandler]

  def bind(handler: LatchHandler): Unit = {
    self.marshal {
      // TODO: figure out how to handle double binding or eliminate in favor of subscriptions
      handlerOpt = Some(handler)
      if (latched) handler.handle()
    }
  }

  def apply(): Unit = {
    if (!latched) {
      latched = true
      handlerOpt.foreach(_.handle())
    }
  }
}

class SingleThreadedLatchSource extends LatchSource with LatchSink {
  private var latched = false
  private var handlerOpt = Option.empty[LatchHandler]

  def bind(handler: LatchHandler): Unit = {
    handlerOpt = Some(handler)
    if (latched) handler.handle()
  }

  def apply(): Unit = {
    if (!latched) {
      latched = true
      handlerOpt.foreach(_.handle())
    }
  }
}

object QueueingReceiverImpl {
  sealed trait State[A, B]
  case class Unopened[A, B](queue: ArrayBuffer[(A, Promise[B])]) extends State[A, B]
  case class Opened[A, B](responder: Responder[A, B]) extends State[A, B]
}
class QueueingReceiverImpl[A, B](self: CallMarshaller) extends Receiver[A, B] with Responder[A, B] {
  import QueueingReceiverImpl._

  private var state: State[A, B] = Unopened(ArrayBuffer.empty[(A, Promise[B])])

  def bind(responder: Responder[A, B]): Unit = {
    self.marshal {
      state match {
        case Unopened(queue) =>
          state = Opened(responder)
          queue.foreach { case (obj, prom) => responder.handle(obj, prom) }
        case _ =>
          state = Opened(responder)
      }
    }
  }

  def handle(obj: A, prom: Promise[B]): Unit = {
    state match {
      case Unopened(queue) => queue.append((obj, prom))
      case Opened(responder) => responder.handle(obj, prom)
    }
  }
}

object ChannelPair {

  class SenderImpl[A, B](target: CallMarshaller, responder: Responder[A, B]) extends Sender[A, B] {
    def send(obj: A): Future[B] = {
      val prom = Promise[B]
      target.marshal {
        responder.handle(obj, prom)
      }
      prom.future
    }
  }

}

class ChannelPair[A, B](sendThread: CallMarshaller, recvThread: CallMarshaller) {
  import ChannelPair._

  private val rcv = new QueueingReceiverImpl[A, B](recvThread)
  private val snd = new SenderImpl[A, B](recvThread, rcv)

  private val senderCloseLatch = new LocallyAppliedLatchSource(sendThread)
  private val receiverCloseLatch = new LocallyAppliedLatchSource(recvThread)

  private def channelClose(): Unit = {
    sendThread.marshal(senderCloseLatch.apply())
    recvThread.marshal(receiverCloseLatch.apply())
  }

  private val out = new TransferChannelReceiver[A, B] {
    def receiver: Receiver[A, B] = rcv

    def onClose: LatchSource = receiverCloseLatch

    def close: LatchSink = new SimpleLatchSink(channelClose)
  }

  private val in = new TransferChannelSender[A, B] {
    def sender: Sender[A, B] = snd

    def onClose: LatchSource = senderCloseLatch

    def close: LatchSink = new SimpleLatchSink(channelClose)
  }

  def receiver: TransferChannelReceiver[A, B] = out
  def sender: TransferChannelSender[A, B] = in

}