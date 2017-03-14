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
package io.greenbus.edge.amqp.colset

import java.util
import java.util.UUID

import io.greenbus.edge.amqp.channel.{ AmqpChannelParsed, AmqpChannelParser }
import io.greenbus.edge.channel2.{ ChannelDescriptor, ChannelSerializationProvider }
import io.greenbus.edge.colset.PeerSessionId
import org.apache.qpid.proton.amqp.{ Binary, Symbol }
import org.apache.qpid.proton.amqp.messaging.{ Source, Target }
import org.apache.qpid.proton.engine.{ Link, Receiver, Sender, Session }

object ChannelIdentifiers {
  val prefix = "_gbe/"

  val peerLinkSubSetAddress = s"${prefix}peer-subscription-set"
  val peerLinkEventAddress = s"${prefix}peer-events"
  val peerLinkServiceRequestAddress = s"${prefix}peer-service-requests"
  val peerLinkServiceResponseAddress = s"${prefix}peer-service-responses"

  val subscriberSubSetAddress = s"${prefix}subscriber-subscription-set"
  val subscriberEventAddress = s"${prefix}subscriber-events"
  val subscriberServiceRequestAddress = s"${prefix}subscriber-service-requests"
  val subscriberServiceResponseAddress = s"${prefix}subscriber-service-responses"

  val gatewaySubSetAddress = s"${prefix}gateway-subscription-set"
  val gatewayEventAddress = s"${prefix}gateway-events"
  val gatewayServiceRequestAddress = s"${prefix}gateway-service-requests"
  val gatewayServiceResponseAddress = s"${prefix}gateway-service-responses"

  val peerPersistenceIdProp = "gbe-peer-persistence-id"
  val peerInstanceIdProp = "gbe-peer-instance-id"

  val subscriberCorrelationProp = "gbe-subscriber-correlation"
  val gatewayCorrelationProp = "gbe-subscriber-correlation"

  def toSymbol(s: String): org.apache.qpid.proton.amqp.Symbol = {
    org.apache.qpid.proton.amqp.Symbol.getSymbol(s)
  }

  def emptyProperties: util.Map[Symbol, AnyRef] = {
    new util.HashMap[Symbol, AnyRef]()
  }

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
  def longOpt(obj: AnyRef): Option[Long] = {
    obj match {
      case v: java.lang.Integer => Some(v.toLong)
      case v: java.lang.Long => Some(v.toLong)
      case _ => None
    }
  }
}

class ChannelParserImpl(serialization: ChannelSerializationProvider) extends AmqpChannelParser {
  import ChannelIdentifiers._
  import io.greenbus.edge.colset.channel.Channels._

  private def parsePeerLinkSession(props: util.Map[Symbol, AnyRef]): Option[PeerSessionId] = {
    for {
      persistenceId <- Option(props.get(toSymbol(peerPersistenceIdProp))).flatMap(uuidOpt)
      instanceId <- Option(props.get(toSymbol(peerInstanceIdProp))).flatMap(longOpt)
    } yield {
      PeerSessionId(persistenceId, instanceId)
    }
  }

  def peerLink[A <: ChannelDescriptor[_]](obj: PeerSessionId => A, propertiesOpt: Option[util.Map[Symbol, AnyRef]]): Option[AmqpChannelParsed] = {
    propertiesOpt.flatMap(parsePeerLinkSession)
      .map(sess => AmqpChannelParsed(obj(sess), emptyProperties))
  }

  def stringCorrelated[A <: ChannelDescriptor[_]](obj: String => A, propertiesOpt: Option[util.Map[Symbol, AnyRef]], key: String): Option[AmqpChannelParsed] = {
    propertiesOpt.flatMap(props => Option(props.get(toSymbol(key))))
      .flatMap(stringOpt)
      .map(corr => AmqpChannelParsed(obj(corr), emptyProperties))
  }

  def sender(address: String, propertiesOpt: Option[util.Map[Symbol, AnyRef]]): Option[AmqpChannelParsed] = {
    address match {
      case `peerLinkSubSetAddress` => peerLink(PeerSubscriptionSetSenderDesc, propertiesOpt)
      case `peerLinkServiceRequestAddress` => peerLink(PeerServiceRequestsDesc, propertiesOpt)
      case `subscriberEventAddress` => stringCorrelated(SubEventReceiverDesc, propertiesOpt, subscriberCorrelationProp)
      case `subscriberServiceResponseAddress` => stringCorrelated(SubServiceResponsesDesc, propertiesOpt, subscriberCorrelationProp)
      case `gatewaySubSetAddress` => stringCorrelated(GateSubscriptionSetSenderDesc, propertiesOpt, gatewayCorrelationProp)
      case `gatewayServiceRequestAddress` => stringCorrelated(GateServiceRequestsDesc, propertiesOpt, gatewayCorrelationProp)
    }
  }

  def receiver(address: String, propertiesOpt: Option[util.Map[Symbol, AnyRef]]): Option[AmqpChannelParsed] = {
    address match {
      case `peerLinkEventAddress` => peerLink(PeerEventReceiverDesc, propertiesOpt)
      case `peerLinkServiceResponseAddress` => peerLink(PeerServiceResponsesDesc, propertiesOpt)
      case `subscriberSubSetAddress` => stringCorrelated(SubSubscriptionSetDesc, propertiesOpt, subscriberCorrelationProp)
      case `subscriberServiceRequestAddress` => stringCorrelated(SubServiceRequestsDesc, propertiesOpt, subscriberCorrelationProp)
      case `gatewayEventAddress` => stringCorrelated(GateEventReceiverDesc, propertiesOpt, gatewayCorrelationProp)
      case `gatewayServiceResponseAddress` => stringCorrelated(GateServiceResponsesDesc, propertiesOpt, gatewayCorrelationProp)
    }
  }
}
