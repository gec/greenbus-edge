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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.qpid.proton.reactor.Reactor

trait EventLoop {
  def wakeup(): Unit
}

class ThreadReactorPump(r: Reactor, handler: () => Unit, id: String) extends EventLoop {

  private val runnable = new Runnable {
    override def run(): Unit = threadRun()
  }
  private val thread = new Thread(runnable, id)
  private val userStopped = new AtomicBoolean(false)

  private def threadRun(): Unit = {
    r.setTimeout(300000)
    r.start()
    var continuing = true
    while (!userStopped.get() && continuing) {
      handler()
      continuing = r.process()
    }
    r.stop()
  }

  def wakeup(): Unit = {
    r.wakeup()
  }

  def open(): Unit = {
    thread.start()
  }

  def close(): Unit = {
    userStopped.set(true)
    r.wakeup()
    thread.join()
  }
}