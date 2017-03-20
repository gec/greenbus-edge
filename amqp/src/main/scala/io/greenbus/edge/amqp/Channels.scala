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

import java.io.ByteArrayInputStream
import java.nio.BufferOverflowException
import java.util
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.amqp.impl._
import io.greenbus.edge._
import io.greenbus.edge.proto.convert.Conversions
import io.greenbus.edge.thread.CallMarshaller
import org.apache.qpid.proton.Proton
import org.apache.qpid.proton.amqp.Binary
import org.apache.qpid.proton.amqp.messaging.{ Data, Source, Target }
import org.apache.qpid.proton.engine.{ Link, Receiver, Sender, Session }
import org.apache.qpid.proton.message.Message

import scala.annotation.tailrec
import scala.concurrent.Future

trait ChannelSessionSource {
  def open(): Future[EdgeChannelClient]
}

trait AmqpEdgeClientService {
  def connect(host: String, port: Int, timeoutMs: Long): Future[EdgeChannelClient]
}

trait EdgeAmqpChannelInitiator {

  def sender[Message](sess: Session, desc: ChannelDescriptor[Message]): Option[(Sender, Message => (Array[Byte], Int))]

  def receiver[Message](sess: Session, desc: ChannelDescriptor[Message]): Option[(Receiver, Array[Byte] => Option[Message])]

}

trait AmqpChannelServer {

  def handleSender(s: Sender, parent: ResourceRemoveObserver): Option[HandlerResource]

  def handleReceiver(r: Receiver, parent: ResourceRemoveObserver): Option[HandlerResource]
}

object AmqpSerialization extends LazyLogging {

  @tailrec
  def encodeMessage(message: Message, hint: Int): (Array[Byte], Int) = {
    val data = new Array[Byte](hint)
    try {
      val amount = message.encode(data, 0, data.length)
      (data, amount)
    } catch {
      case ex: BufferOverflowException =>
        encodeMessage(message, hint * 2)
    }
  }

  def serializeMessage(payload: Array[Byte]): (Array[Byte], Int) = {
    val msg = Proton.message()
    msg.setBody(new Data(new Binary(payload)))

    encodeMessage(msg, payload.length + 40)
  }

  def deserializeMessage(message: Array[Byte]): Option[(Array[Byte], Int)] = {
    val msg = Proton.message()
    msg.decode(message, 0, message.length)

    Option(msg.getBody).flatMap {
      case d: Data =>
        val a = d.getValue.getArray
        val l = d.getValue.getLength
        Some((a, l))
      case _ => None
    }
  }

  def buildFullSerializer[A](payloadSerializer: A => Array[Byte]): A => (Array[Byte], Int) = {
    def serialize(obj: A): (Array[Byte], Int) = {
      val payload = payloadSerializer(obj)
      serializeMessage(payload)
    }

    serialize
  }

  // TODO: logger id system for warnings
  def buildFullDeserializer[A](payloadDeserializer: (Array[Byte], Int) => Either[String, A]): Array[Byte] => Option[A] = {
    def fullDeserialize(buffer: Array[Byte]): Option[A] = {
      val payloadOpt = deserializeMessage(buffer)
      payloadOpt.flatMap {
        case (array, length) =>
          payloadDeserializer(array, length) match {
            case Left(error) =>
              logger.warn(s"Could not deserialize message: $error")
              None
            case Right(obj) => Some(obj)
          }
      }
    }

    fullDeserialize
  }
}

object AmqpEdgeChannels {
  val subscriptionRequestAddress = "_gbedge/client-subscription-request"
  val subscriptionNotificationAddress = "_gbedge/client-subscription-notification"
  val subscriptionCorrelatorKey = "gbedge-subscription-correlator"

  val clientOutputRequestAddress = "_gbedge/client-output-request"
  val clientOutputResponseAddress = "_gbedge/client-output-response"
  val clientOutputIssuerIdKey = "gbedge-client-output-id"

  val endpointPublishAddress = "_gbedge/endpoint-publish"
  val publisherOutputRequestAddress = "_gbedge/publisher-output-request"
  val publisherOutputResponseAddress = "_gbedge/publisher-output-response"
  val publisherEndpointIdKey = "gbedge-endpoint-id"
  val publisherSessionIdKey = "gbedge-session-id"

  def toSymbol(s: String): org.apache.qpid.proton.amqp.Symbol = {
    org.apache.qpid.proton.amqp.Symbol.getSymbol(s)
  }

}

object EdgeChannelInitiatorImpl {

}
class EdgeChannelInitiatorImpl(provider: SerializationProvider) extends EdgeAmqpChannelInitiator with LazyLogging {
  import AmqpEdgeChannels._
  import io.greenbus.edge.EdgeChannels._
  import AmqpSerialization._

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

  private def publisherProps(id: EndpointPublisherId): Map[String, AnyRef] = {
    val endProto = new Binary(Conversions.toProto(id.endpointId).toByteArray)
    val sessProto = new Binary(Conversions.toProto(id.sessionId).toByteArray)
    Map(publisherEndpointIdKey -> endProto, publisherSessionIdKey -> sessProto)
  }

  def sender[Message](sess: Session, desc: ChannelDescriptor[Message]): Option[(Sender, Message => (Array[Byte], Int))] = {
    logger.info("Building sender for " + desc)
    desc match {
      case d: ClientSubscriptionRequestDesc => {
        Some(basicSender(sess, "ClientSubscriptionRequest", d, subscriptionRequestAddress, Map(subscriptionCorrelatorKey -> d.correlator)))
      }
      case d: ClientOutputRequestDesc => {
        Some(basicSender(sess, "ClientOutputRequest", d, clientOutputRequestAddress, Map(clientOutputIssuerIdKey -> d.issuerId)))
      }
      case d: EndpointPublishDesc => {
        Some(basicSender(sess, "EndpointPublish", d, endpointPublishAddress, publisherProps(d.key)))
      }
      case d: PublisherOutputResponseDesc => {
        Some(basicSender(sess, "PublisherOutputResponse", d, publisherOutputResponseAddress, publisherProps(d.key)))
      }
      case _ =>
        logger.warn("Sender descriptor unrecognized: " + desc)
        None
    }
  }

  def receiver[Message](sess: Session, desc: ChannelDescriptor[Message]): Option[(Receiver, Array[Byte] => Option[Message])] = {
    logger.info("Building receiver for " + desc)
    desc match {
      case d: ClientSubscriptionNotificationDesc => {
        Some(basicReceiver(sess, "ClientSubscriptionNotification", d, subscriptionNotificationAddress, Map(subscriptionCorrelatorKey -> d.correlator)))
      }
      case d: ClientOutputResponseDesc => {
        Some(basicReceiver(sess, "ClientOutputResponse", d, clientOutputResponseAddress, Map(clientOutputIssuerIdKey -> d.issuerId)))
      }
      case d: PublisherOutputRequestDesc => {
        Some(basicReceiver(sess, "PublisherOutputRequest", d, publisherOutputRequestAddress, publisherProps(d.key)))
      }
      case _ =>
        logger.warn("Receiver descriptor unrecognized: " + desc)
        None
    }
  }
}

object EdgeAmqpChannelHandlerImpl {

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
class EdgeAmqpChannelHandlerImpl(ioThread: CallMarshaller, provider: SerializationProvider, handler: EdgeServerChannelHandler) extends AmqpChannelServer with LazyLogging {
  import AmqpEdgeChannels._
  import EdgeAmqpChannelHandlerImpl._
  import io.greenbus.edge.EdgeChannels._
  import AmqpSerialization._

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

  private def publisherIdOpt(l: Link): Option[EndpointPublisherId] = {
    def eitherOpt[A](e: Either[String, A]): Option[A] = {
      e match {
        case Left(err) =>
          logger.warn("Couldn't convert key value proto: " + err); None
        case Right(r) => Some(r)
      }
    }

    for {
      props <- Option(l.getRemoteProperties)
      endIdBin <- Option(props.get(toSymbol(publisherEndpointIdKey))).flatMap(binaryOpt)
      endIdProto <- parseOpt(endIdBin, proto.EndpointId.parseFrom)
      endId <- eitherOpt(Conversions.fromProto(endIdProto))
      sessIdBin <- Option(props.get(toSymbol(publisherSessionIdKey))).flatMap(binaryOpt)
      sessIdProto <- parseOpt(sessIdBin, proto.SessionId.parseFrom)
      sessId <- eitherOpt(Conversions.fromProto(sessIdProto))
    } yield {
      EndpointPublisherId(ClientSessionSourceId(sessId), sessId, endId)
    }
  }

  def handleSender(s: Sender, parent: ResourceRemoveObserver): Option[HandlerResource] = {
    val addrOpt = for {
      targ <- Option(s.getRemoteTarget)
      addr <- Option(targ.getAddress)
    } yield addr

    logger.debug(s"Handling sender for address $addrOpt, properties ${s.getRemoteProperties}")

    val descOpt: Option[ChannelDescriptor[_]] = addrOpt.flatMap {
      case `subscriptionNotificationAddress` /*str if str == subscriptionNotificationAddress*/ => {
        for {
          props <- Option(s.getRemoteProperties)
          value <- Option(props.get(toSymbol(subscriptionCorrelatorKey)))
          correlator <- stringOpt(value)
        } yield {
          ClientSubscriptionNotificationDesc(correlator)
        }
      }
      case str if str == clientOutputResponseAddress => {
        for {
          props <- Option(s.getRemoteProperties)
          value <- Option(props.get(toSymbol(clientOutputIssuerIdKey)))
          clientId <- uuidOpt(value)
        } yield {
          ClientOutputResponseDesc(clientId)
        }
      }
      case str if str == publisherOutputRequestAddress => {
        publisherIdOpt(s).map { id => PublisherOutputRequestDesc(id) }
      }
      case other =>
        logger.warn(s"Sender address unrecognized: " + other)
        None
    }

    descOpt.map(desc => handleSenderDescriptor(s, desc, parent))
  }

  def handleReceiver(r: Receiver, parent: ResourceRemoveObserver): Option[HandlerResource] = {
    val addrOpt = for {
      targ <- Option(r.getRemoteTarget)
      addr <- Option(targ.getAddress)
    } yield addr

    logger.debug(s"Handling receiver for address $addrOpt, properties ${r.getRemoteProperties}")

    val descOpt: Option[ChannelDescriptor[_]] = addrOpt.flatMap {
      case str if str == subscriptionRequestAddress => {
        for {
          props <- Option(r.getRemoteProperties)
          value <- Option(props.get(toSymbol(subscriptionCorrelatorKey)))
          correlator <- stringOpt(value)
        } yield {
          ClientSubscriptionRequestDesc(correlator)
        }
      }
      case str if str == clientOutputRequestAddress => {
        for {
          props <- Option(r.getRemoteProperties)
          value <- Option(props.get(toSymbol(clientOutputIssuerIdKey)))
          clientId <- uuidOpt(value)
        } yield {
          ClientOutputRequestDesc(clientId)
        }
      }
      case str if str == endpointPublishAddress => {
        publisherIdOpt(r).map { id => EndpointPublishDesc(id) }
      }
      case str if str == publisherOutputResponseAddress => {
        publisherIdOpt(r).map { id => PublisherOutputResponseDesc(id) }
      }
      case other =>
        logger.warn(s"Receiver address unrecognized: " + other)
        None
    }

    descOpt.map(desc => handleReceiverDescriptor(r, desc, parent))
  }
}