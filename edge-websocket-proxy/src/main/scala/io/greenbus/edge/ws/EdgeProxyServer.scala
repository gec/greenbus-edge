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

import com.typesafe.scalalogging.LazyLogging
import org.eclipse.jetty.server.{ Server, ServerConnector }
import org.eclipse.jetty.servlet.{ ServletContextHandler, ServletHolder }
import org.eclipse.jetty.websocket.api.{ Session, WebSocketAdapter }
import org.eclipse.jetty.websocket.servlet.{ WebSocketServlet, WebSocketServletFactory }

class EdgeGuiServer(port: Int) {

  def run(): Unit = {

    val server = new Server
    val connector = new ServerConnector(server)
    connector.setPort(port)
    server.addConnector(connector)

    val ctx = new ServletContextHandler(ServletContextHandler.SESSIONS)
    ctx.setContextPath("/")
    server.setHandler(ctx)

    val holder = new ServletHolder("edgegui", classOf[EdgeServlet])
    new ServletHolder()
    holder.setInitParameter("dirAllowed", "true")
    holder.setInitParameter("pathInfoOnly", "true")
    ctx.addServlet(holder, "/socket/*")

    try {
      server.start()
      server.dump(System.err)
      server.join()
    } catch {
      case ex: Throwable => ex.printStackTrace()
    }
  }
}

class EdgeServlet extends WebSocketServlet with LazyLogging {
  def configure(webSocketServletFactory: WebSocketServletFactory): Unit = {
    logger.info("Got servlet configure " + this)
    webSocketServletFactory.getPolicy.setMaxBinaryMessageSize(5000000)
    webSocketServletFactory.getPolicy.setMaxTextMessageSize(5000000)
    webSocketServletFactory.register(classOf[EdgeSocket])
  }
}

class SessionSocketImpl(session: Session) extends Socket with LazyLogging {
  def send(text: String): Unit = {
    try {
      session.getRemote.sendString(text)
    } catch {
      case ex: Throwable => logger.warn("Problem writing to socket: " + ex)
    }
  }
}

class EdgeSocket extends WebSocketAdapter with LazyLogging {
  private val mgr = EdgeWebSocketProxy.globalSocketMgr.get()
  private var socketOpt = Option.empty[Socket]

  override def onWebSocketBinary(payload: Array[Byte], offset: Int, len: Int): Unit = {
    logger.info("Got web socket binary")
    super.onWebSocketBinary(payload, offset, len)
  }

  override def onWebSocketConnect(sess: Session): Unit = {
    val sock = new SessionSocketImpl(sess)
    socketOpt = Some(sock)
    mgr.connected(sock)
    super.onWebSocketConnect(sess)

  }

  override def onWebSocketError(cause: Throwable): Unit = {
    logger.info("Got web socket error: " + cause)
    super.onWebSocketError(cause)
  }

  override def onWebSocketText(message: String): Unit = {
    logger.info("Got web socket text: " + message)
    socketOpt.foreach { sock =>
      mgr.handle(message, sock)
    }
    super.onWebSocketText(message)
  }

  override def onWebSocketClose(statusCode: Int, reason: String): Unit = {
    logger.info("Got web socket close")
    socketOpt.foreach(mgr.disconnected)
    super.onWebSocketClose(statusCode, reason)
  }
}

