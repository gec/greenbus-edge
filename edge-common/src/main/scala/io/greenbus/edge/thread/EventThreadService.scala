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
package io.greenbus.edge.thread

import java.util.concurrent.{ Executors, ThreadFactory, TimeUnit }

trait EventThreadService extends SchedulableCallMarshaller {

  def close(): Unit
}

object EventThreadService {

  private class ExecutorEventThread(id: String) extends EventThreadService {

    private val s = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      def newThread(runnable: Runnable): Thread = {
        new Thread(runnable, id)
      }
    })

    def marshal(f: => Unit): Unit = {
      s.execute(new Runnable {
        def run(): Unit = f
      })
    }

    def delayed(durationMs: Long, f: => Unit): Unit = {
      s.schedule(new Runnable {
        def run(): Unit = f
      }, durationMs, TimeUnit.MILLISECONDS)
    }

    def close(): Unit = {
      s.shutdown()
    }
  }

  def build(id: String): EventThreadService = {
    new ExecutorEventThread(id)
  }
}
