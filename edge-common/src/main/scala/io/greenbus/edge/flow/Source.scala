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

import scala.util.Try

trait Handler[A] {
  def handle(obj: A): Unit
}
trait Source[A] {
  def bind(handler: Handler[A]): Unit
}

object Sink {
  def apply[A](f: A => Unit) = {
    new Sink[A] {
      def push(obj: A): Unit = f(obj)
    }
  }
}
trait Sink[A] {
  def push(obj: A): Unit
}

object LatchSink {
  def apply(f: () => Unit): LatchSink = {
    () => f()
  }
}
trait LatchSink {
  def apply(): Unit
}

trait LatchHandler {
  def handle(): Unit
}

trait LatchSource {
  def bind(handler: LatchHandler): Unit
}

trait LatchSubscribable {
  def subscribe(handler: LatchHandler): Closeable
}

trait Closeable {
  def close(): Unit
}

trait CloseObservable {
  def onClose: LatchSubscribable
}

trait CloseableComponent extends Closeable with CloseObservable

trait Responder[A, B] {
  def handle(obj: A, respond: B => Unit)
}

trait Sender[A, B] {
  def send(obj: A, handleResponse: Try[B] => Unit): Unit
}

trait Receiver[A, B] {
  def bind(responder: Responder[A, B]): Unit
}

trait SenderChannel[A, B] extends Sender[A, B] with CloseableComponent

trait ReceiverChannel[A, B] extends Receiver[A, B] with CloseableComponent
