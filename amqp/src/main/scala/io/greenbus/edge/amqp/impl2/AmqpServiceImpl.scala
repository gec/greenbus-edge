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
package io.greenbus.edge.amqp.impl2

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.amqp.{ AmqpChannelServer, AmqpListener }
import io.greenbus.edge.amqp.channel.{ AmqpChannelDescriber, AmqpClientResponseParser }
import io.greenbus.edge.amqp.impl.{ ResourceContainer, _ }
import io.greenbus.edge.channel2.{ ChannelClient, ChannelSerializationProvider }
import io.greenbus.edge.thread.CallMarshaller
import org.apache.qpid.proton.Proton
import org.apache.qpid.proton.engine.{ BaseHandler, Connection, Event, Session }

import scala.concurrent.{ Future, Promise }

trait AmqpChannelClientSource {
  def open(describer: AmqpChannelDescriber, responseParser: AmqpClientResponseParser, serialization: ChannelSerializationProvider): Future[ChannelClient]
}

class AmqpChannelClientSourceImpl(ioThread: CallMarshaller, c: Connection, promise: Promise[AmqpChannelClientSource]) extends AmqpChannelClientSource {
  private val children = new ResourceContainer

  def open(describer: AmqpChannelDescriber, responseParser: AmqpClientResponseParser, serialization: ChannelSerializationProvider): Future[ChannelClient] = {
    val promise = Promise[ChannelClient]
    ioThread.marshal {
      val sess = c.session()
      val impl = new ChannelClientImpl(ioThread, sess, describer, responseParser, serialization, promise, children)
      children.add(impl)
      sess.setContext(impl.handler)
      sess.open()
    }
    promise.future
  }

  private val self = this
  val handler = new ConnectionContext {
    def onSessionRemoteOpen(s: Session): Unit = {
      // TODO: verify the protocol behavior here
      //s.close()
    }

    def onOpen(c: Connection): Unit = {
      promise.success(self)
    }

    def onRemoteClose(c: Connection): Unit = {
      children.notifyOfClose()
    }
  }
}

object AmqpService {
  def build(threadId: Option[String] = None): AmqpService = {
    new AmqpServiceImpl(threadId)
  }
}
trait AmqpService {
  def close(): Unit
  def eventLoop: CallMarshaller
  def connect(host: String, port: Int, timeoutMs: Long): Future[AmqpChannelClientSource]
  def listen(host: String, port: Int, handler: AmqpChannelServer): Future[AmqpListener]
}

class AmqpServiceImpl(idOpt: Option[String] = None) extends AmqpService {
  private val opQueue = new OperationQueue
  private val baseHandler = new ReactorHandler
  private val r = Proton.reactor(baseHandler)
  r.getGlobalHandler.add(new UnhandledLogger(idOpt.getOrElse("AMQP")))

  private val threadId = idOpt.map(id => s"AMQP reactor - $id").getOrElse("AMQP reactor")
  private val threadPump = new ThreadReactorPump(r, opQueue.handle, threadId)
  opQueue.setNotifier(threadPump)

  threadPump.open()

  def close(): Unit = {
    threadPump.close()
  }

  def eventLoop: CallMarshaller = opQueue

  def connect(host: String, port: Int, timeoutMs: Long): Future[AmqpChannelClientSource] = {
    val hostname = s"$host:$port"

    val protonHandler = new ClientConnectionProtonHandler(hostname)
    val promise = Promise[AmqpChannelClientSource]

    opQueue.marshal {
      val conn = r.connectionToHost(host, port, protonHandler)
      val impl = new AmqpChannelClientSourceImpl(opQueue, conn, promise)

      r.schedule(timeoutMs.toInt, protonHandler)
      conn.setContext(impl.handler)

      conn.open()
    }

    promise.future
  }

  def listen(host: String, port: Int, /*sslOpt: Option[AmqpSslServerConfig], saslEnabled: Boolean,*/ handler: AmqpChannelServer): Future[AmqpListener] = {
    val hostname = s"$host:$port"
    val ctx = new ListenerContext(hostname, opQueue, handler)
    val protonHandler = new ListenerProtonHandler(hostname, ctx /*, sslOpt, saslEnabled*/ )

    val prom = Promise[AmqpListener]

    opQueue.marshal {
      val acceptor = r.acceptor(host, port, protonHandler)
      prom.success(new ListenerImpl(opQueue, acceptor))
    }

    prom.future
  }

  private class ReactorHandler extends BaseHandler {
    override def onReactorInit(e: Event): Unit = {
      val task = e.getReactor.schedule(300000, this)
      task.attachments().set("key", classOf[String], "loop")
    }

    override def onTimerTask(e: Event): Unit = {
      if (e.getTask.attachments().get("key", classOf[String]) == "loop") {
        val task = e.getReactor.schedule(300000, this)
        task.attachments().set("key", classOf[String], "loop")
      }
    }
  }
}

class UnhandledLogger(id: String) extends BaseHandler with LazyLogging {
  override def onUnhandled(event: Event): Unit = {
    logger.trace(s"$id UNHANDLED: " + event)
  }
}

