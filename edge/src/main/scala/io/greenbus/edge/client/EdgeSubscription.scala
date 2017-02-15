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

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge._
import io.greenbus.edge.channel._

import scala.concurrent.{ Future, Promise }

trait EdgeSubscriptionClient {
  def openSubscription(params: ClientSubscriptionParams): Future[EdgeSubscription]
}

trait EdgeSubscription extends Closeable {

  def updateParams(params: ClientSubscriptionParams): Unit
  def notifications: Source[ClientSubscriptionNotification]
}

case class SubscriberChannels(
    params: TransferChannelSender[ClientSubscriptionParamsMessage, Boolean],
    notifications: TransferChannelReceiver[ClientSubscriptionNotificationMessage, Boolean]) {
  def closeAll(): Unit = {
    params.close()
    notifications.close()
  }
}

class EdgeSubscriptionImpl(
    eventThread: CallMarshaller,
    channels: SubscriberChannels) extends EdgeSubscription with LazyLogging {

  private val closeSink = new SimpleLatchSink(handleUserClose)
  protected val closeSource = new LocallyAppliedLatchSource(eventThread)

  private val sinkSource = new SinkOwnedSourceJoin[ClientSubscriptionNotification](eventThread)

  channels.notifications.receiver.bind(new Responder[ClientSubscriptionNotificationMessage, Boolean] {
    def handle(obj: ClientSubscriptionNotificationMessage, promise: Promise[Boolean]): Unit = {
      sinkSource.push(obj.notification)
      promise.success(true)
    }
  })

  def notifications: Source[ClientSubscriptionNotification] = sinkSource

  def updateParams(params: ClientSubscriptionParams): Unit = {
    eventThread.marshal {
      sendAndReportFailure(ClientSubscriptionParamsMessage(params), channels.params)
    }
  }

  protected def sendAndReportFailure[A](obj: A, channel: TransferChannelSender[A, Boolean]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // TODO: channels need to do callbacks that get adapted to futures
    val fut = channel.sender.send(obj)
    fut.onFailure {
      case ex =>
        logger.warn(s"Channel send failure for subscription request, closing")
        eventThread.marshal {
          handleChannelClose()
        }
    }
  }

  private def handleChannelClose(): Unit = {
    channels.closeAll()
    closeSource()
  }

  protected def handleUserClose(): Unit = {
    eventThread.marshal {
      handleChannelClose()
    }
  }

  def close: LatchSink = closeSink

  def onClose: LatchSource = closeSource
}