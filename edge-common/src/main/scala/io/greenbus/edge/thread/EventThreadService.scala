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

import java.io.{ ByteArrayOutputStream, PrintStream }
import java.util.concurrent.{ Executors, ThreadFactory, TimeUnit }

import com.typesafe.scalalogging.LazyLogging

trait EventThreadService extends SchedulableCallMarshaller {

  def close(): Unit
}

object EventThreadService {

  private class ExecutorEventThread(id: String) extends EventThreadService with LazyLogging {

    private val s = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      def newThread(runnable: Runnable): Thread = {
        new Thread(runnable, id)
      }
    })

    def marshal(f: => Unit): Unit = {
      logger.debug(s"!!! MARSHAL: " + Thread.currentThread().getStackTrace.toVector(2))
      if (!s.isShutdown) {
        s.execute(new Runnable {
          def run(): Unit = execute { f }
        })
      }
    }

    def delayed(durationMs: Long, f: => Unit): Unit = {
      s.schedule(new Runnable {
        def run(): Unit = execute { f }
      }, durationMs, TimeUnit.MILLISECONDS)
    }

    private def execute(f: => Unit): Unit = {
      try { f } catch {
        case ex: Throwable =>
          logger.error(s"Exception thrown in event thread handler: " + ex)
          val bos = new ByteArrayOutputStream()
          val ps = new PrintStream(bos)
          ex.printStackTrace(ps)
          ps.flush()
          logger.debug(new String(bos.toByteArray, "UTF-8"))
      }
    }

    def close(): Unit = {
      s.shutdown()
    }
  }

  def build(id: String): EventThreadService = {
    new ExecutorEventThread(id)
  }
}
