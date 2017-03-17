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
package io.greenbus.edge.flow

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.CallMarshaller

import scala.collection.mutable.ArrayBuffer

object QueuedDistributor {
  sealed trait State[A]
  case class Unbound[A](queue: ArrayBuffer[A]) extends State[A]
  case class Opened[A](handler: Handler[A]) extends State[A]
}
class QueuedDistributor[A] extends Source[A] with Sink[A] {
  import QueuedDistributor._

  private var state: State[A] = Unbound(ArrayBuffer.empty[A])

  def bind(handler: Handler[A]): Unit = {
    state match {
      case Unbound(queue) =>
        state = Opened(handler)
        queue.foreach(handler.handle)
      case Opened(_) =>
        throw new IllegalStateException("Queued distributor bound twice")
    }
  }

  def push(obj: A): Unit = {
    state match {
      case Unbound(queue) => queue += obj
      case Opened(handler) => handler.handle(obj)
    }
  }
}

object RemoteBoundQueuedDistributor {
  sealed trait State[A]
  case class Unbound[A](queue: ArrayBuffer[A]) extends State[A]
  case class Opened[A](handler: Handler[A]) extends State[A]
}
class RemoteBoundQueuedDistributor[A](eventThread: CallMarshaller) extends Source[A] with Sink[A] with LazyLogging {
  import QueuedDistributor._

  private var state: State[A] = Unbound(ArrayBuffer.empty[A])

  def bind(handler: Handler[A]): Unit = {
    eventThread.marshal {
      state match {
        case Unbound(queue) =>
          state = Opened(handler)
          queue.foreach(handler.handle)
        case Opened(_) =>
          logger.error("Queued distributor bound twice")
      }
    }
  }

  def push(obj: A): Unit = {
    state match {
      case Unbound(queue) => queue += obj
      case Opened(handler) => handler.handle(obj)
    }
  }
}

object QueueingReceiverImpl {
  sealed trait State[A, B]
  case class Unopened[A, B](queue: ArrayBuffer[(A, B => Unit)]) extends State[A, B]
  case class Opened[A, B](responder: Responder[A, B]) extends State[A, B]
}
class QueueingReceiverImpl[A, B](self: CallMarshaller) extends Receiver[A, B] with Responder[A, B] {
  import QueueingReceiverImpl._

  private var state: State[A, B] = Unopened(ArrayBuffer.empty[(A, B => Unit)])

  def bind(responder: Responder[A, B]): Unit = {
    assert(responder != this)
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

  def handle(obj: A, respond: B => Unit): Unit = {
    state match {
      case Unopened(queue) => queue.append((obj, respond))
      case Opened(responder) => responder.handle(obj, respond)
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

class SingleThreadedLatchSubscribable extends LatchSubscribable with LatchSink {
  private var latched = false
  private var handlers = Set.empty[LatchHandler]

  def subscribe(handler: LatchHandler): Closeable = {
    handlers += handler
    if (latched) handler.handle()
    new Closeable {
      def close(): Unit = {
        handlers -= handler
      }
    }
  }

  def apply(): Unit = {
    if (!latched) {
      latched = true
      handlers.foreach(_.handle())
    }
  }
}

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

class LocallyAppliedLatchSubscribable(self: CallMarshaller) extends LatchSubscribable with LatchSink {
  private var latched = false
  private var handlers = Set.empty[LatchHandler]

  def subscribe(handler: LatchHandler): Closeable = {
    self.marshal {
      handlers += handler
      if (latched) handler.handle()
    }
    new Closeable {
      def close(): Unit = {
        self.marshal {
          handlers -= handler
        }
      }
    }
  }

  def apply(): Unit = {
    if (!latched) {
      latched = true
      handlers.foreach(_.handle())
    }
  }
}