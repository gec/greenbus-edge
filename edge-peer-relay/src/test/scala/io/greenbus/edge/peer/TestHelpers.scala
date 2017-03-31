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
package io.greenbus.edge.peer

import scala.concurrent.{ Future, Promise, promise }

object TestHelpers {

  class TypedEventQueue[A] {

    private var queue = Seq.empty[A]
    private var chk = Option.empty[(Promise[Seq[A]], Seq[A] => Boolean)]

    private val mutex = new Object

    def received(obj: A): Unit = {
      mutex.synchronized {
        queue = queue ++ Vector(obj)
        chk.foreach {
          case (prom, check) =>
            if (check(queue)) {
              prom.success(queue)
              chk = None
            }
        }
      }
    }

    def listen(check: Seq[A] => Boolean): Future[Seq[A]] = {
      mutex.synchronized {
        val prom = Promise[Seq[A]]

        if (check(queue)) {
          prom.success(queue)
        } else {
          chk = Some((prom, check))
        }
        prom.future
      }
    }
  }
}
