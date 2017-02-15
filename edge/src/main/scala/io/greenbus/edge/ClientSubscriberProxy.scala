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
import io.greenbus.edge.channel.{ Closeable, DeferredLatchHandler, Receiver, Sender }

trait ClientSubscriberProxy extends Closeable {
  def params: Receiver[ClientSubscriptionParamsMessage, Boolean]
  def notify(notification: ClientSubscriptionNotification): Unit
}

import EdgeChannels._

// NOTE: in the future this will probably have auth
class ClientSubscriberProxyImpl(eventThread: CallMarshaller, channels: ClientSubscriberChannelSet) extends ClientSubscriberProxy with CloseablePeerComponent with LazyLogging {

  channels.paramsChannel.onClose.bind(new DeferredLatchHandler(closeLatch))
  channels.notificationChannel.onClose.bind(new DeferredLatchHandler(closeLatch))

  def params: Receiver[ClientSubscriptionParamsMessage, Boolean] = channels.paramsChannel.receiver

  def notify(notification: ClientSubscriptionNotification): Unit = {
    // TODO: CallMarshaller *is* an execution context?
    import scala.concurrent.ExecutionContext.Implicits.global

    val message = ClientSubscriptionNotificationMessage(notification)
    val fut = channels.notificationChannel.sender.send(message)
    fut.onFailure {
      case ex =>
        logger.warn(s"Failure sending subscription notification: $ex")
        eventThread.marshal {
          this.close()
        }
    }
  }

  protected def handleClose(): Unit = {
    channels.paramsChannel.close()
    channels.notificationChannel.close()
  }
}