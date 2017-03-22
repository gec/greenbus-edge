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

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow._

import scala.util.Try

class CloseableHolder(closeables: Seq[CloseableComponent]) {
  val closeLatch = new SingleThreadedLatchSubscribable
  private val channelCloseSubs = closeables.map(_.onClose.subscribe(() => onSubChannelClosed()))

  private def onSubChannelClosed(): Unit = {
    closeLatch()
    closeables.foreach(_.close())
    channelCloseSubs.foreach(_.close())
  }
}
trait CloseableChannelAggregate extends Closeable with CloseObservable {

  protected val closeableHolder: CloseableHolder

  def close(): Unit = {
    closeableHolder.closeLatch()
  }

  def onClose: LatchSubscribable = closeableHolder.closeLatch
}

object ChannelHelpers extends LazyLogging {

  def wrapReceiver[Message, A](channel: ReceiverChannel[Message, Boolean], map: Message => A): Source[A] = {
    new Source[A] {
      def bind(handler: Handler[A]): Unit = {
        channel.bind(new Responder[Message, Boolean] {
          def handle(msg: Message, respond: (Boolean) => Unit): Unit = {
            handler.handle(map(msg))
            respond(true)
          }
        })
      }
    }
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