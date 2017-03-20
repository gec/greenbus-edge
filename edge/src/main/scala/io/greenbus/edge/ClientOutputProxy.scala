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
package io.greenbus.edge

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.channel.{ Closeable, DeferredLatchHandler, Receiver }
import io.greenbus.edge.thread.CallMarshaller

trait ClientOutputProxy extends Closeable {
  def respond(correlation: Long, response: OutputResult): Unit
  def requests: Receiver[ClientOutputRequestMessage, Boolean]
}

import EdgeChannels._

class ClientOutputProxyImpl(eventThread: CallMarshaller, channels: ClientOutputIssuerSet) extends ClientOutputProxy with CloseablePeerComponent with LazyLogging {

  channels.issueChannel.onClose.bind(new DeferredLatchHandler(closeLatch))
  channels.responseChannel.onClose.bind(new DeferredLatchHandler(closeLatch))

  def requests: Receiver[ClientOutputRequestMessage, Boolean] = channels.issueChannel.receiver

  def respond(correlation: Long, response: OutputResult): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val message = ClientOutputResponseMessage(Seq(correlation -> response))
    val fut = channels.responseChannel.sender.send(message)
    fut.onFailure {
      case ex =>
        logger.warn(s"Failure sending output response: $ex")
        eventThread.marshal {
          this.close()
        }
    }
  }

  protected def handleClose(): Unit = {
    channels.issueChannel.close()
    channels.responseChannel.close()
  }
}
