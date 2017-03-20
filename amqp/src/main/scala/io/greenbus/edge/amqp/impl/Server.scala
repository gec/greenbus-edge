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
package io.greenbus.edge.amqp.impl

import java.util.concurrent.TimeoutException

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.channel
import io.greenbus.edge.amqp.{ AmqpChannelServer, AmqpListener }
import io.greenbus.edge.channel.{ Receiver => _, Sender => _, _ }
import io.greenbus.edge.thread.CallMarshaller
import org.apache.qpid.proton.engine._
import org.apache.qpid.proton.reactor.Acceptor

import scala.concurrent.{ Future, Promise }

class ListenerImpl(ioThread: CallMarshaller, acceptor: Acceptor) extends AmqpListener {
  def close(): Unit = {
    ioThread.marshal {
      acceptor.close()
    }
  }
}

class ServerSessionImpl(handler: AmqpChannelServer, parent: ResourceRemoveObserver) extends SessionContext with HandlerResource with LazyLogging {
  private val children = new ResourceContainer

  def handleParentClose(): Unit = {
    children.notifyOfClose()
  }

  def onOpen(s: Session): Unit = {}

  def onRemoteClose(s: Session): Unit = {
    children.notifyOfClose()
    parent.handleChildRemove(this)
  }

  def onSenderRemoteOpen(s: Sender): Unit = {
    //logger.debug(s"server sender open: $s")
    try {
      handler.handleSender(s, parent) match {
        case None => s.close()
        case Some(resource) => {
          children.add(resource)
          s.open()
        }
      }
    } catch {
      case ex: Throwable =>
        logger.error("Error handing sender: " + ex)
        s.close()
    }
  }

  def onReceiverRemoteOpen(r: Receiver): Unit = {
    //logger.debug(s"server receiver open: $r")
    try {
      handler.handleReceiver(r, parent) match {
        case None => r.close()
        case Some(resource) => {
          children.add(resource)
          r.flow(1024) // TODO: move this?
          r.open()
        }
      }
    } catch {
      case ex: Throwable =>
        logger.error("Error handing receiver: " + ex)
        r.close()
    }
  }
}

class ServerConnectionImpl(handler: AmqpChannelServer) extends ConnectionContext with LazyLogging {
  private val children = new ResourceContainer

  def onSessionRemoteOpen(s: Session): Unit = {
    logger.debug(s"Session remote open: $s")
    val impl = new ServerSessionImpl(handler, children)
    s.setContext(impl)
    children.add(impl)
    s.open()
  }

  def onOpen(c: Connection): Unit = {}

  def onRemoteClose(c: Connection): Unit = {
    logger.debug(s"Connection closed: ${c.getRemoteHostname}")
    children.notifyOfClose()
  }
}

class ListenerContext(id: String, ioThread: CallMarshaller, handler: AmqpChannelServer) extends LazyLogging {

  def handleConnectionOpen(conn: Connection /*, credentials: Option[SaslPlainCredentials]*/ ): Unit = {

    val remoteHostname = Option(conn.getRemoteHostname).getOrElse("[unknown]")
    logger.info(s"Connection from hostname $remoteHostname")

    // TODO: auth here?

    val impl = new ServerConnectionImpl(handler)
    conn.setContext(impl)
    conn.open()
  }
}

class ListenerProtonHandler(id: String, ctx: ListenerContext /*, sslOpt: Option[AmqpSslServerConfig], saslEnabled: Boolean*/ ) extends BaseHandler with LazyLogging {
  add(new ContextualProtonHandler(id))

  /*override def onConnectionInit(e: Event): Unit = {

    if (saslEnabled) {
      val sasl = e.getTransport.sasl()
      sasl.server()
      sasl.setMechanisms("PLAIN")
      sasl.allowSkip(false)
      //sasl.done(Sasl.SaslOutcome.PN_SASL_AUTH)
    }
    r.getGlobalHandler.add(new UnhandledLogger)

    sslOpt.foreach { ssl =>
      val domain = Proton.sslDomain()
      domain.init(SslDomain.Mode.SERVER)

      domain.setCredentials(ssl.peerConfig.publicKey, ssl.peerConfig.privateKey, ssl.peerConfig.passwordOpt.orNull)
      ssl.caOpt.foreach { ca =>
        domain.setTrustedCaDb(ca)
        domain.setPeerAuthentication(SslDomain.VerifyMode.VERIFY_PEER)
      }

      e.getConnection.getTransport.ssl(domain)
    }
  }*/

  override def onConnectionRemoteOpen(e: Event): Unit = {
    /*val credentialsOpt = if (saslEnabled) {
      val sasl = e.getTransport.sasl()

      val pending = sasl.pending()
      if (pending > 0) {
        val arr = new Array[Byte](pending)
        sasl.recv(arr, 0, pending)
        SaslPlainParser.parse(arr)
      } else {
        None
      }
    } else {
      None
    }*/

    ctx.handleConnectionOpen(e.getConnection /*, credentialsOpt*/ )
  }
}
