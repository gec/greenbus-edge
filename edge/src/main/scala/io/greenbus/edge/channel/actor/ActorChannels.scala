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
package io.greenbus.edge.channel.actor

import java.util.UUID

import akka.actor.ActorSystem
import io.greenbus.edge._
import io.greenbus.edge.channel._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ Future, Promise }

class ActorChannelEngine(system: ActorSystem, handlerFac: CallMarshaller => EdgeServerChannelHandler) {
  private val serverActor = system.actorOf(CallMarshallingActor.props)
  private val serverMarshaller = CallMarshallingActor.proxy(serverActor)
  private val handler = handlerFac(serverMarshaller)

  def client(): EdgeChannelClient = {
    clientAndThread()._1
  }
  def clientAndThread(): (EdgeChannelClient, CallMarshaller) = {
    val clientActor = system.actorOf(CallMarshallingActor.props)
    val marshaller = CallMarshallingActor.proxy(clientActor)

    val client = new ChannelSourceImpl(marshaller, serverMarshaller, handler, EmptyServerConnectionAuthContext)
    (client, marshaller)
  }
}

class ChannelSourceImpl(client: CallMarshaller, server: CallMarshaller, handler: EdgeServerChannelHandler, connAuth: ServerConnectionAuthContext) extends EdgeChannelClient {

  def openSender[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[TransferChannelSender[Message, Boolean]] = {
    val pair = new ChannelPair[Message, Boolean](client, server)
    server.marshal {
      handler.handleReceiver(desc, pair.receiver)
    }
    Future.successful(pair.sender)
  }

  def openReceiver[Message, Desc <: ChannelDescriptor[Message]](desc: Desc): Future[TransferChannelReceiver[Message, Boolean]] = {
    val pair = new ChannelPair[Message, Boolean](client, server)
    server.marshal {
      handler.handleSender(desc, pair.sender)
    }
    Future.successful(pair.receiver)
  }
}

