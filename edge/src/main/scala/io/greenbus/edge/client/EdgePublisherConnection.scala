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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge._
import io.greenbus.edge.channel._
import io.greenbus.edge.thread.CallMarshaller

import scala.concurrent.{ Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global

case class EndpointPublisherClientChannels(
    data: TransferChannelSender[EndpointPublishMessage, Boolean],
    output: TransferChannelReceiver[PublisherOutputRequestMessage, Boolean],
    outputResponse: TransferChannelSender[PublisherOutputResponseMessage, Boolean]) {
  def closeAll(): Unit = {
    data.close()
    output.close()
    outputResponse.close()
  }
}

trait EndpointPublisherConnection extends Closeable

class SimpleLatchSource extends LatchSource {
  def bind(handler: LatchHandler): Unit = {

  }
}

class EndpointPublisherConnectionImpl(
    eventThread: CallMarshaller,
    id: EndpointId,
    publisher: EndpointPublisher,
    channels: EndpointPublisherClientChannels) extends EndpointPublisherConnection with LazyLogging {

  private val closeSink = new SimpleLatchSink(handleUserClose)
  protected val closeSource = new LocallyAppliedLatchSource(eventThread)

  channels.data.onClose.bind(new DeferredLatchHandler(closeSink))
  channels.output.onClose.bind(new DeferredLatchHandler(closeSink))
  channels.outputResponse.onClose.bind(new DeferredLatchHandler(closeSink))

  private val updateSub = publisher.subscribeToUpdates()
  updateSub.source.bind(update => sendAndReportFailure(update, channels.data))
  updateSub.onClose.bind(() => handleChannelClose())

  updateSub.onClose.bind(() => handleChannelClose())
  channels.output.receiver.bindFunc(handleOutput)

  private def handleOutputResult(result: OutputResult, correlationId: Long): Unit = {
    val resp = PublisherOutputResponseMessage(Seq((correlationId, result)))
    sendAndReportFailure(resp, channels.outputResponse)
  }

  protected def handleOutput(message: PublisherOutputRequestMessage, promise: Promise[Boolean]): Unit = {
    publisher.handleOutput(message, handleOutputResult)
    promise.success(true)
  }

  protected def sendAndReportFailure[A](obj: A, channel: TransferChannelSender[A, Boolean]): Unit = {
    // TODO: channels need to do callbacks that get adapted to futures
    val fut = channel.sender.send(obj)
    fut.onFailure {
      case ex =>
        logger.warn(s"Channel send failure for $id, closing: " + ex)
        eventThread.marshal {
          handleChannelClose()
        }
    }
  }

  private def closeSubscriptions(): Unit = {
    updateSub.close()
  }

  private def handleChannelClose(): Unit = {
    channels.closeAll()
    closeSubscriptions()
    closeSource()
  }

  protected def handleUserClose(): Unit = {
    eventThread.marshal {
      handleChannelClose()
    }
  }

  def onClose: LatchSource = closeSource
  def close: LatchSink = closeSink
}