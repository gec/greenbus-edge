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
package io.greenbus.edge.amqp
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.EdgeChannels.ClientSubscriptionNotificationDesc
import io.greenbus.edge._
import io.greenbus.edge.amqp.impl.AmqpIoImpl
import io.greenbus.edge.channel.{ TransferChannelReceiver, TransferChannelSender }
import io.greenbus.edge.proto.provider.EdgeProtobufProvider

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

object ProtoTest extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("Starting")
    val listener = runServer()

    runClient()

    System.in.read()
  }

  def runClient(): Unit = {
    val service = new AmqpIoImpl(Some("client"))

    val connFut = service.connect("127.0.0.1", 50001, 10000)

    val conn = Await.result(connFut, 5000.milliseconds)

    val sessionFut = conn.open()

    val session = Await.result(sessionFut, 5000.milliseconds)

    val correlation = UUID.randomUUID().toString

    import io.greenbus.edge.EdgeChannels._
    val reqDesc = ClientSubscriptionRequestDesc(correlation)
    val senderFut = session.openSender[ClientSubscriptionParamsMessage, ClientSubscriptionRequestDesc](reqDesc)
    val notDesc = ClientSubscriptionNotificationDesc(correlation)
    val receiverFut = session.openReceiver[ClientSubscriptionNotificationMessage, ClientSubscriptionNotificationDesc](notDesc)

    val sender = Await.result(senderFut, 5000.milliseconds)
    val receiver = Await.result(receiverFut, 5000.milliseconds)

    receiver.receiver.bindFunc { (msg, prom) =>
      logger.info("CLIENT GOT: " + msg)
      prom.success(true)
    }

    logger.info("sender opened")

    val id = NamedEndpointId("my-test-endpoint")

    val params = ClientSubscriptionParams(
      infoSubscriptions = Seq(id),
      dataSubscriptions = Seq(EndpointPath(id, Path("key01")), EndpointPath(id, Path("key02"))),
      outputSubscriptions = Seq(EndpointPath(id, Path("outKey01")), EndpointPath(id, Path("outKey02"))))

    val reqFut = sender.sender.send(ClientSubscriptionParamsMessage(params))

    val req = Await.result(reqFut, 5000.milliseconds)

    logger.info("Request succeeded")
  }

  def runServer(): Future[AmqpListener] = {
    val service = new AmqpIoImpl(Some("server"))

    val serverHandler = new EdgeServerChannelHandler {
      def handleReceiver[Message](desc: ChannelDescriptor[Message], channel: TransferChannelReceiver[Message, Boolean]): Unit = {
        println("Got receiver: " + desc)

        channel.receiver.bindFunc { (msg, prom) =>
          logger.info("SERVER GOT: " + msg)
          prom.success(true)
        }

      }

      def handleSender[Message](desc: ChannelDescriptor[Message], channel: TransferChannelSender[Message, Boolean]): Unit = {
        println("Got sender: " + desc)
        desc match {
          case d: ClientSubscriptionNotificationDesc =>
            channel.sender.send(
              ClientSubscriptionNotificationMessage(ClientSubscriptionNotification(
                Seq(), ClientIndexNotification(), Seq(), Seq(), Seq())))
        }
      }
    }

    val server = new EdgeAmqpChannelHandlerImpl(service.eventLoop, new EdgeProtobufProvider, serverHandler)

    service.listen("127.0.0.1", 50001, server)
  }
}
