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

import scala.concurrent.{ Future, Promise }

trait Handler[A] {
  def handle(obj: A): Unit
}
trait Responder[A, B] {
  def handle(obj: A, promise: Promise[B])
}

trait LatchHandler {
  def handle(): Unit
}

trait Sink[A] {
  def push(obj: A): Unit
}

trait Source[A] {
  def bind(handler: Handler[A]): Unit
  def bind(f: A => Unit): Unit = {
    bind(new Handler[A] {
      def handle(obj: A): Unit = f(obj)
    })
  }
}

trait Sender[A, B] {
  def send(obj: A): Future[B]
}
trait Receiver[A, B] {
  def bind(responder: Responder[A, B]): Unit
  def bindFunc(f: (A, Promise[B]) => Unit): Unit = {
    bind(new Responder[A, B] {
      def handle(obj: A, promise: Promise[B]): Unit = {
        f(obj, promise)
      }
    })
  }
}

trait LatchSink {
  def apply(): Unit
}

trait LatchSource {
  def bind(handler: LatchHandler)
  def bind(f: () => Unit): Unit = {
    bind(new LatchHandler {
      def handle(): Unit = f()
    })
  }
}

trait Notifier {
  def apply()
}
trait Observer {
  def handle(): Unit
}

trait CloseObservable {
  def onClose: LatchSource
}

trait Closeable extends CloseObservable {
  def close: LatchSink
}

trait EventChannelReceiver[A] extends Closeable {
  def source: Source[A]
}

trait EventChannelSender[A] extends Closeable {
  def sink: Sink[A]

}

trait TransferChannelSender[A, B] extends Closeable {
  def sender: Sender[A, B]
}

trait TransferChannelReceiver[A, B] extends Closeable {
  def receiver: Receiver[A, B]
}

