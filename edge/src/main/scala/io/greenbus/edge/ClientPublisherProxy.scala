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

import EdgeChannels._
import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.channel._

import scala.concurrent.Promise
import scala.concurrent.ExecutionContext.Implicits.global

//TODO: guarantee that we can unsubscribe and see nothing further
trait ClientPublisherProxy extends Closeable {

  def dataUpdates: Receiver[EndpointPublishMessage, Boolean]
  //def output: Sender[EndpointOutputMessage, Boolean]
  //def outputResponses: Receiver[EndpointOutputResponseMessage, Boolean]

  def outputIssued(key: Path, output: PublisherOutputParams, responseCallback: OutputResult => Unit): Unit

}

class ClientPublisherProxyImpl(eventThread: CallMarshaller, channels: PublisherChannelSet) extends ClientPublisherProxy with CloseablePeerComponent with LazyLogging {

  private val outputCorrelator = new Correlator[OutputResult => Unit]()

  channels.publishChannel.onClose.bind(new DeferredLatchHandler(closeLatch))
  channels.outputIssueChannel.onClose.bind(new DeferredLatchHandler(closeLatch))
  channels.outputResponseChannel.onClose.bind(new DeferredLatchHandler(closeLatch))

  def dataUpdates: Receiver[EndpointPublishMessage, Boolean] = channels.publishChannel.receiver
  private def outputSender: Sender[PublisherOutputRequestMessage, Boolean] = channels.outputIssueChannel.sender
  private def outputResponses: Receiver[PublisherOutputResponseMessage, Boolean] = channels.outputResponseChannel.receiver

  outputResponses.bindFunc(onOutputResponse)

  def outputIssued(key: Path, output: PublisherOutputParams, responseCallback: OutputResult => Unit): Unit = {

    val correlationId = outputCorrelator.add(responseCallback)
    val request = PublisherOutputRequest(key, output, correlationId)
    val outputMessage = PublisherOutputRequestMessage(Seq(request))

    val fut = outputSender.send(outputMessage)
    fut.onFailure {
      case ex =>
        logger.warn(s"Failure sending output: $ex")
        eventThread.marshal {
          this.close()
        }
    }
  }

  private def onOutputResponse(message: PublisherOutputResponseMessage, promise: Promise[Boolean]): Unit = {
    promise.success(true)
    message.results.foreach {
      case (correlator, result) => outputCorrelator.pop(correlator).foreach(f => f(result))
    }
  }

  protected def handleClose(): Unit = {
    channels.publishChannel.close()
    channels.outputIssueChannel.close()
    channels.outputResponseChannel.close()
  }
}

// TODO: priority queue based dropping old ones
class Correlator[A] {

  private var sequence: Long = 0
  private var correlationMap = Map.empty[Long, A]

  def add(obj: A): Long = {
    val peerCorrelation = sequence
    sequence += 1

    correlationMap += (peerCorrelation -> obj)

    peerCorrelation
  }

  def pop(correlator: Long): Option[A] = {
    val result = correlationMap.get(correlator)
    correlationMap -= correlator
    result
  }
}

trait CloseablePeerComponent extends Closeable {
  private val closeSink = new SimpleLatchSink(doClose)
  private val closeLatchSource = new SingleThreadedLatchSource

  private def doClose(): Unit = {
    handleClose()
    closeLatchSource()
  }

  protected def closeLatch: LatchSink = closeSink

  protected def handleClose(): Unit

  def close: LatchSink = closeSink
  def onClose: LatchSource = closeLatchSource
}