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
package io.greenbus.edge.ws

import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.greenbus.edge.peer.AmqpEdgeService

import scala.concurrent.ExecutionContext.Implicits.global

object EdgeWebSocketProxy {

  val globalSocketMgr = new AtomicReference[SocketMgr](null)

  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load()
    val slf4jConfig = ConfigFactory.parseString("""akka { loggers = ["akka.event.slf4j.Slf4jLogger"] }""")
    val akkaConfig = slf4jConfig.withFallback(rootConfig)
    val system = ActorSystem("brokerTest", akkaConfig)

    val services = AmqpEdgeService.build("127.0.0.1", 50001, 10000)
    services.start()

    val linkMgr = system.actorOf(PeerLinkMgr.props(services.consumer))

    val mgr = new GuiSocketMgr(linkMgr)
    globalSocketMgr.set(mgr)

    val server = new EdgeGuiServer(8080)
    server.run()
  }
}

