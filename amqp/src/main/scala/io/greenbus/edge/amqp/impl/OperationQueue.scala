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

import java.io.{ PrintWriter, StringWriter }
import java.util.concurrent.ConcurrentLinkedQueue

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.thread.CallMarshaller

import scala.concurrent.{ Future, Promise }

class OperationQueue extends CallMarshaller with LazyLogging {
  private val requestQueue = new ConcurrentLinkedQueue[() => Unit]()
  private var elOpt = Option.empty[EventLoop]

  def setNotifier(el: EventLoop): Unit = {
    elOpt = Some(el)
  }

  private def enqueue(f: () => Unit): Unit = {
    requestQueue.add(f)
    elOpt.foreach(_.wakeup())
  }

  def marshal(f: => Unit): Unit = {
    //logger.debug(s"!!! AMQP MARSHAL: " + Thread.currentThread().getStackTrace.toVector(2))
    enqueue(() => {
      try f catch {
        case ex: Throwable =>
          logger.warn(s"Thrown exception in marshaller: " + ex)
          lazy val stack = {
            val sw = new StringWriter()
            val pw = new PrintWriter(sw)
            ex.printStackTrace(pw)
            sw.toString
          }
          logger.debug(stack)
      }
    })
  }

  def marshalResult[A](f: => A): Future[A] = {
    val prom = Promise[A]
    enqueue(() => {
      try {
        prom.success(f)
      } catch {
        case ex: Throwable => prom.failure(ex)
      }
    })
    prom.future
  }

  def handle(): Unit = {
    while (!requestQueue.isEmpty) {
      val f = requestQueue.poll()
      f()
    }
  }
}