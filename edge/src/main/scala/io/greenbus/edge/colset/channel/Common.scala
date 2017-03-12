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

import io.greenbus.edge.flow._

import scala.util.Try

trait CloseableChannelAggregate extends Closeable with CloseObservable {

  private val closeLatch = new SingleThreadedLatchSubscribable
  private val channelCloseSubs = closeables.map(_.onClose.subscribe(() => onSubChannelClosed()))

  protected def closeables: Seq[CloseableComponent]

  private def onSubChannelClosed(): Unit = {
    onClose
    closeables.foreach(_.close())
    channelCloseSubs.foreach(_.close())
  }

  def close(): Unit = {
    closeLatch()
  }

  def onClose: LatchSubscribable = closeLatch
}

object ChannelHelpers {

  def bindDistributor[Message, A](channel: ReceiverChannel[Message, Boolean], map: Message => A): QueuedDistributor[A] = {
    val dist = new QueuedDistributor[A]
    channel.bind(new Responder[Message, Boolean] {
      def handle(msg: Message, respond: (Boolean) => Unit): Unit = {
        dist.push(map(msg))
        respond(true)
      }
    })
    dist
  }

  def bindSink[Message, A](channel: SenderChannel[Message, Boolean], map: A => Message): Sink[A] = {
    def onResult(result: Try[Boolean]): Unit = {}
    Sink[A] { seq => channel.send(map(seq), onResult) }
  }

}

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