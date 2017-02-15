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

import io.greenbus.edge.CallMarshaller

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ Future, Promise }

class SimpleSink[A](f: A => Unit) extends Sink[A] {
  def push(obj: A): Unit = f(obj)
}

class SimpleLatchSink(f: () => Unit) extends LatchSink {
  def apply(): Unit = f()
}

object NullLatchSource extends LatchSource {
  def bind(handler: LatchHandler): Unit = {}
}

object NullLatchSink extends LatchSink {
  def apply(): Unit = {}
}

class DeferredLatchHandler(sink: LatchSink) extends LatchHandler {
  def handle(): Unit = sink()
}

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

// Distributes to multiple subscribers, late-joiners get the snapshot then subsequent updates
class SnapshotAndUpdateDistributor[A](marshaller: CallMarshaller, getSnapshot: () => A) {
  private var subscribers = Set.empty[Sink[A]]

  protected def subscriberAdded(sink: Sink[A]): Unit = {
    marshaller.marshal {
      subscribers += sink
      sink.push(getSnapshot())
    }
  }

  protected def subscriberRemoved(sink: Sink[A]): Unit = {
    marshaller.marshal {
      subscribers -= sink
    }
  }

  def subscribe(): EventChannelReceiver[A] = {
    val subscriber = new SinkOwnedSourceJoin[A](marshaller)
    subscriberAdded(subscriber)
    new EventChannelReceiver[A] {
      def source: Source[A] = subscriber

      def onClose: LatchSource = NullLatchSource

      def close: LatchSink = new SimpleLatchSink(() => subscriberRemoved(subscriber))
    }
  }

  def update(update: A): Unit = {
    subscribers.foreach(_.push(update))
  }
}

