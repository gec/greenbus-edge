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
package io.greenbus.edge.client

import java.util.concurrent.TimeoutException

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge._
import io.greenbus.edge.channel._

import scala.concurrent.{ Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global

trait EdgeOutputClient extends Closeable {

  def issueOutput(key: EndpointPath, request: ClientOutputParams): Future[OutputResult]
}

case class OutputChannels(
    issue: TransferChannelSender[ClientOutputRequestMessage, Boolean],
    response: TransferChannelReceiver[ClientOutputResponseMessage, Boolean]) {
  def closeAll(): Unit = {
    issue.close()
    response.close()
  }
}

class EdgeOutputClientImpl(eventThread: CallMarshaller, channels: OutputChannels) extends EdgeOutputClient with LazyLogging {

  private val correlator = new Correlator[Promise[OutputResult]]

  private val closeSink = new SimpleLatchSink(handleUserClose)
  protected val closeSource = new LocallyAppliedLatchSource(eventThread)

  channels.response.receiver.bindFunc(handleIssueResponse)

  channels.issue.onClose.bind(() => handleChannelClose())
  channels.response.onClose.bind(() => handleChannelClose())

  def issueOutput(key: EndpointPath, request: ClientOutputParams): Future[OutputResult] = {
    val prom = Promise[OutputResult]
    eventThread.marshal {
      val sequence = correlator.add(prom)
      val req = ClientOutputRequest(key, request, sequence)
      val message = ClientOutputRequestMessage(Seq(req))
      val fut = channels.issue.sender.send(message)
      fut.onFailure {
        case ex =>
          logger.warn(s"Channel send failure for output client, closing")
          eventThread.marshal {
            handleChannelClose()
            // TODO: fail all promises in correlator?
            prom.failure(new TimeoutException())
          }
      }
    }
    prom.future
  }

  private def handleIssueResponse(message: ClientOutputResponseMessage, promise: Promise[Boolean]): Unit = {
    message.results.foreach {
      case (correlation, result) =>
        correlator.pop(correlation).foreach(_.success(result))
    }
  }

  protected def handleUserClose(): Unit = {
    eventThread.marshal {
      handleChannelClose()
    }
  }

  private def handleChannelClose(): Unit = {
    channels.closeAll()
    closeSource()
  }

  def close: LatchSink = closeSink

  def onClose: LatchSource = closeSource
}
