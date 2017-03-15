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

import java.util
import java.util.UUID

import io.greenbus.edge.channel2.{ ChannelDescriptor, ChannelSerializationProvider }
import org.apache.qpid.proton.amqp.messaging.{ Source, Target }
import org.apache.qpid.proton.engine.{ Link, Receiver, Sender, Session }

case class AmqpChannelDescription(prefix: String, address: String, props: Map[String, AnyRef])
trait AmqpChannelDescriber {
  def describe[A](desc: ChannelDescriptor[A]): Option[AmqpChannelDescription]
}

object AmqpChannelInitiator {

  def toSymbol(s: String): org.apache.qpid.proton.amqp.Symbol = {
    org.apache.qpid.proton.amqp.Symbol.getSymbol(s)
  }
}
class AmqpChannelInitiator(describer: AmqpChannelDescriber, provider: ChannelSerializationProvider) {
  import AmqpChannelInitiator._
  import io.greenbus.edge.amqp.AmqpSerialization._

  private def linkCommon(l: Link, address: String, props: Map[String, AnyRef]): Unit = {
    val source = new Source
    source.setAddress(address)
    l.setSource(source)

    val target = new Target
    target.setAddress(address)
    l.setTarget(target)

    val propMap = new util.HashMap[org.apache.qpid.proton.amqp.Symbol, AnyRef]()
    props.foreach { case (k, v) => propMap.put(toSymbol(k), v) }
    l.setProperties(propMap)
  }

  private def basicSender[A](sess: Session, prefix: String, desc: ChannelDescriptor[A], address: String, props: Map[String, AnyRef]): (Sender, A => (Array[Byte], Int)) = {
    val name = prefix + "-" + UUID.randomUUID().toString
    val s = sess.sender(name)
    linkCommon(s, address, props)
    val payloadSerializer = provider.serializerFor(desc)
    (s, buildFullSerializer(payloadSerializer))
  }
  private def basicReceiver[A](sess: Session, prefix: String, desc: ChannelDescriptor[A], address: String, props: Map[String, AnyRef]): (Receiver, Array[Byte] => Option[A]) = {
    val name = prefix + "-" + UUID.randomUUID().toString
    val r = sess.receiver(name)
    linkCommon(r, address, props)
    val payloadDeserializer = provider.deserializerFor(desc)
    (r, buildFullDeserializer(payloadDeserializer))
  }

  def sender[Message](sess: Session, desc: ChannelDescriptor[Message]): Option[(Sender, Message => (Array[Byte], Int))] = {
    describer.describe(desc).map { ch =>
      basicSender(sess, ch.prefix, desc, ch.address, ch.props)
    }
  }

  def receiver[Message](sess: Session, desc: ChannelDescriptor[Message]): Option[(Receiver, Array[Byte] => Option[Message])] = {
    describer.describe(desc).map { ch =>
      basicReceiver(sess, ch.prefix, desc, ch.address, ch.props)
    }
  }
}

