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

import io.greenbus.edge.api.stream.{ ServiceClient, ServiceClientChannel }
import io.greenbus.edge.api.{ OutputRequest, OutputResult }
import io.greenbus.edge.thread.SchedulableCallMarshaller

import scala.collection.mutable
import scala.util.Try

object QueuingServiceChannel {

  case class Entry(obj: OutputRequest, handleResponse: (Try[OutputResult]) => Unit)

  sealed trait State
  case class Disconnected(queue: mutable.ArrayBuffer[Entry]) extends State
  case class Connected(channel: ServiceClientChannel) extends State
}
class QueuingServiceChannel(eventThread: SchedulableCallMarshaller) extends ServiceClient {
  import QueuingServiceChannel._

  private var state: State = Disconnected(mutable.ArrayBuffer.empty[Entry])

  def connected(channel: ServiceClientChannel): Unit = {
    eventThread.marshal {
      state match {
        case Disconnected(queue) => {
          queue.foreach(e => channel.send(e.obj, e.handleResponse))
        }
        case Connected(_) =>
      }
      state = Connected(channel)
      channel.onClose.subscribe(() => eventThread.marshal { onDisconnect() })
    }
  }

  private def onDisconnect(): Unit = {
    state = Disconnected(mutable.ArrayBuffer.empty[Entry])
  }

  def send(obj: OutputRequest, handleResponse: (Try[OutputResult]) => Unit): Unit = {
    eventThread.marshal {
      state match {
        case Disconnected(queue) => queue += Entry(obj, handleResponse)
        case Connected(ch) => ch.send(obj, handleResponse)
      }
    }
  }
}

