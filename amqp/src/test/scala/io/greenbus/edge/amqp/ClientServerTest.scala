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

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.ClientSubscriptionParamsMessage
import io.greenbus.edge.amqp.impl.{ AmqpIoImpl, HandlerResource, ResourceRemoveObserver }
import org.apache.qpid.proton.engine.{ Receiver, Sender }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

object ClientServerTest extends LazyLogging {

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

    import io.greenbus.edge.EdgeChannels._
    val desc = ClientSubscriptionRequestDesc("correlator")
    val senderFut = session.openSender[ClientSubscriptionParamsMessage, ClientSubscriptionRequestDesc](desc)

    val sender = Await.result(senderFut, 5000.milliseconds)

    logger.info("sender opened")

  }

  def runServer(): Future[AmqpListener] = {
    val service = new AmqpIoImpl(Some("server"))

    val handler = new AmqpChannelServer {
      def handleReceiver(r: Receiver, parent: ResourceRemoveObserver): Option[HandlerResource] = {
        println("receiver: " + r)
        None
      }

      def handleSender(s: Sender, parent: ResourceRemoveObserver): Option[HandlerResource] = {
        println("sender: " + s)
        None
      }
    }

    service.listen("127.0.0.1", 50001, handler)
  }
}
