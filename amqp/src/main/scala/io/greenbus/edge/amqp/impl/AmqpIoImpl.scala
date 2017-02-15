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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.{ CallMarshaller, ChannelDescriptor, EdgeChannelClient, EdgeServerChannelHandler }
import io.greenbus.edge.amqp.{ AmqpChannelServer, ChannelSessionSource }
import io.greenbus.edge.channel.{ Sender => _, _ }
import org.apache.qpid.proton.Proton
import org.apache.qpid.proton.engine.{ Receiver, _ }
import org.apache.qpid.proton.reactor.Acceptor

import scala.concurrent.{ Future, Promise }

class AmqpIoImpl(idOpt: Option[String] = None) {
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

  def connect(host: String, port: Int, timeoutMs: Long): Future[ChannelSessionSource] = {
    val hostname = s"$host:$port"

    val protonHandler = new ClientConnectionProtonHandler(hostname)
    val promise = Promise[ChannelSessionSource]

    opQueue.marshal {
      val conn = r.connectionToHost(host, port, protonHandler)
      val impl = new ChannelSessionSourceImpl(opQueue, conn, promise)

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

trait HandlerResource {
  def handleParentClose(): Unit
}
trait ResourceRemoveObserver {
  def handleChildRemove(resource: HandlerResource)
}

class ResourceContainer extends ResourceRemoveObserver {
  private var children = Set.empty[HandlerResource]
  def add(res: HandlerResource): Unit = {
    children += res
  }

  def handleChildRemove(resource: HandlerResource): Unit = {
    children -= resource
  }

  def notifyOfClose(): Unit = {
    children.foreach(_.handleParentClose())
  }
}
