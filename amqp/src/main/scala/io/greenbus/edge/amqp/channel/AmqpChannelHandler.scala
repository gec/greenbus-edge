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
package io.greenbus.edge.amqp.channel

import java.io.ByteArrayInputStream
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.CallMarshaller
import io.greenbus.edge.amqp.AmqpChannelServer
import io.greenbus.edge.amqp.channel.impl.{ ServerReceiverChannelImpl, ServerSenderChannelImpl }
import io.greenbus.edge.amqp.impl.{ HandlerResource, ResourceRemoveObserver }
import io.greenbus.edge.channel2.{ ChannelDescriptor, ChannelSerializationProvider, ChannelServerHandler }
import org.apache.qpid.proton.amqp.{ Binary, Symbol => AmqpSymbol }
import org.apache.qpid.proton.engine.{ Receiver, Sender }

case class AmqpChannelParsed(desc: ChannelDescriptor[_], localProperties: java.util.Map[AmqpSymbol, AnyRef])

trait AmqpChannelParser {
  def sender(address: String, propertiesOpt: Option[java.util.Map[AmqpSymbol, AnyRef]]): Option[AmqpChannelParsed]
  def receiver(address: String, propertiesOpt: Option[java.util.Map[AmqpSymbol, AnyRef]]): Option[AmqpChannelParsed]
}

object AmqpChannelHandler {

  def binaryOpt(obj: AnyRef): Option[Binary] = {
    obj match {
      case v: Binary => Some(v)
      case _ => None
    }
  }
  def uuidOpt(obj: AnyRef): Option[UUID] = {
    obj match {
      case v: UUID => Some(v)
      case _ => None
    }
  }
  def stringOpt(obj: AnyRef): Option[String] = {
    obj match {
      case v: String => Some(v)
      case _ => None
    }
  }

}
class AmqpChannelHandler(ioThread: CallMarshaller, parser: AmqpChannelParser, provider: ChannelSerializationProvider, handler: ChannelServerHandler) extends AmqpChannelServer with LazyLogging {
  import AmqpChannelHandler._
  import io.greenbus.edge.amqp.AmqpSerialization._

  private def handleSenderDescriptor[A](s: Sender, desc: ChannelDescriptor[A], parent: ResourceRemoveObserver): HandlerResource = {
    val serializer = provider.serializerFor(desc)
    val impl = new ServerSenderChannelImpl[A](ioThread, s, buildFullSerializer(serializer), parent)
    s.setContext(impl.handler)
    handler.handleSender(desc, impl)
    impl
  }

  private def handleReceiverDescriptor[A](r: Receiver, desc: ChannelDescriptor[A], parent: ResourceRemoveObserver): HandlerResource = {
    val deserializer = provider.deserializerFor(desc)
    val impl = new ServerReceiverChannelImpl[A](ioThread, r, buildFullDeserializer(deserializer), parent)
    r.setContext(impl.handler)
    handler.handleReceiver(desc, impl)
    impl
  }

  def parseOpt[A](b: Binary, f: ByteArrayInputStream => A): Option[A] = {
    try {
      val in = new ByteArrayInputStream(b.getArray, b.getArrayOffset, b.getLength)
      Some(f(in))
    } catch {
      case ex: Throwable =>
        logger.warn(s"Error parsing binary property: " + ex)
        None
    }
  }

  private def parseProtoKv[A, Msg](obj: AnyRef, parse: ByteArrayInputStream => Msg, convert: Msg => Either[String, A]): Option[A] = {
    binaryOpt(obj).flatMap(b => parseOpt(b, parse)).map(convert).flatMap {
      case Left(err) =>
        logger.warn("Couldn't convert key value proto: " + err); None
      case Right(r) => Some(r)
    }
  }

  def handleSender(s: Sender, parent: ResourceRemoveObserver): Option[HandlerResource] = {

    val addrOpt = for {
      targ <- Option(s.getRemoteTarget)
      addr <- Option(targ.getAddress)
    } yield addr

    val propsOpt = Option(s.getRemoteProperties)

    logger.debug(s"Handling sender for address $addrOpt, properties ${propsOpt}")

    val resultOpt = addrOpt.flatMap { address =>
      parser.sender(address, propsOpt)
    }

    resultOpt.map { result =>
      s.setProperties(result.localProperties)
      handleSenderDescriptor(s, result.desc, parent)
    }
  }

  def handleReceiver(r: Receiver, parent: ResourceRemoveObserver): Option[HandlerResource] = {

    val addrOpt = for {
      targ <- Option(r.getRemoteTarget)
      addr <- Option(targ.getAddress)
    } yield addr

    val propsOpt = Option(r.getRemoteProperties)

    logger.debug(s"Handling receiver for address $addrOpt, properties ${propsOpt}")

    val resultOpt = addrOpt.flatMap { address =>
      parser.sender(address, propsOpt)
    }

    resultOpt.map { result =>
      r.setProperties(result.localProperties)
      handleReceiverDescriptor(r, result.desc, parent)
    }
  }
}