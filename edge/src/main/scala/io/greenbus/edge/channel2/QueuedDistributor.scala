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
package io.greenbus.edge.channel2

import scala.collection.mutable.ArrayBuffer

/*

object SinkOwnedSourceJoin {
  sealed trait State[A]
  case class Unbound[A](queue: ArrayBuffer[A]) extends State[A]
  case class Opened[A](handler: Handler[A]) extends State[A]
}
class SinkOwnedSourceJoin[A](bindMarshal: CallMarshaller) extends Source[A] with Sink[A] {
  import SinkOwnedSourceJoin._

  private var state: State[A] = Unbound(ArrayBuffer.empty[A])

  def bind(handler: Handler[A]): Unit = {
    bindMarshal.marshal {
      state match {
        case Unbound(queue) =>
          state = Opened(handler)
          queue.foreach(handler.handle)
        case Opened(_) =>
          // TODO: warning? exception?
          state = Opened(handler)
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
 */
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
