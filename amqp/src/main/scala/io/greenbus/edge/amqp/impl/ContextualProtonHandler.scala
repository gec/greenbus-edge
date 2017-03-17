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

import com.typesafe.scalalogging.LazyLogging
import org.apache.qpid.proton.engine.{ BaseHandler, Event, Receiver, Sender }

class ContextualProtonHandler(id: String) extends BaseHandler with LazyLogging {

  override def onTransportError(e: Event): Unit = {
    logger.debug("Transport error: " + Option(e.getTransport.getCondition).map(_.getDescription).getOrElse("[unknown]"))
  }

  override def onTransportClosed(e: Event): Unit = {
    e.getConnection.getContext match {
      case ctx: ConnectionContext => ctx.onRemoteClose(e.getConnection)
      case _ => logger.warn(s"$id unknown context in transport closed")
    }
  }

  override def onConnectionRemoteClose(e: Event): Unit = {
    e.getConnection.getContext match {
      case ctx: ConnectionContext => ctx.onRemoteClose(e.getConnection)
      case _ => logger.warn(s"$id unknown context in connection remote close")
    }
  }

  override def onSessionRemoteOpen(e: Event): Unit = {
    e.getSession.getContext match {
      case ctx: SessionContext => ctx.onOpen(e.getSession)
      case _ =>
        e.getConnection.getContext match {
          case ctx: ConnectionContext => ctx.onSessionRemoteOpen(e.getSession)
          case _ => logger.warn(s"$id unknown context in session remote open")
        }
    }
  }

  override def onSessionRemoteClose(e: Event): Unit = {
    e.getSession.getContext match {
      case ctx: SessionContext => ctx.onRemoteClose(e.getSession)
      case _ => logger.warn(s"$id unknown context in session remote close")
    }
  }

  override def onLinkRemoteOpen(e: Event): Unit = {
    e.getLink.getContext match {
      case ctx: LinkContext => ctx.onOpen(e.getLink)
      case _ =>
        e.getSession.getContext match {
          case ctx: SessionContext =>
            e.getLink match {
              case s: Sender => ctx.onSenderRemoteOpen(s)
              case r: Receiver => ctx.onReceiverRemoteOpen(r)
            }
          case _ => logger.warn(s"$id unknown context in link remote open")
        }
    }
  }

  override def onLinkRemoteClose(e: Event): Unit = {
    e.getLink.getContext match {
      case ctx: LinkContext => ctx.onRemoteClose(e.getLink)
      case _ => logger.warn(s"$id unknown context in link remote close")
    }
  }

  override def onLinkFlow(e: Event): Unit = {
    logger.trace("On link flow: " + e)
    e.getLink match {
      case s: Sender =>
        s.getContext match {
          case ctx: SenderContext => ctx.linkFlow(s)
          case o => logger.warn(s"$id onLinkFlow context unrecognized: " + o)
        }
      case _ => logger.warn(s"$id link in onLinkFlow was not a sender")
    }
  }

  override def onDelivery(e: Event): Unit = {
    logger.trace("On delivery: " + e)
    val d = e.getDelivery

    e.getLink match {
      case r: Receiver =>
        d.getContext match {
          case ctx: ReceiverDeliveryContext => ctx.onDelivery(r, d)
          case o => //logger.trace(s"$id receiver delivery context unrecognized: " + o)
        }
        r.getContext match {
          case ctx: ReceiverContext => ctx.onDelivery(r, d)
          case o => logger.warn(s"$id receiver context unrecognized: " + o)
        }
      case s: Sender =>
        d.getContext match {
          case ctx: SenderDeliveryContext => ctx.onDelivery(s, d)
          case o => logger.trace(s"$id sender delivery context unrecognized: " + o)
        }
        s.getContext match {
          case ctx: SenderContext => ctx.onDelivery(s, d)
          case o => logger.warn(s"$id sender context unrecognized: " + o)
        }
      case _ => logger.warn(s"$id link in onDelivery was not a receiver")
    }
  }

}