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
package io.greenbus.edge.stream.proto.provider

import java.io.ByteArrayInputStream

import com.google.protobuf.Message
import io.greenbus.edge.channel.{ ChannelDescriptor, ChannelSerializationProvider }
import io.greenbus.edge.stream.proto
import io.greenbus.edge.stream.proto.convert.ProtocolConversions

class ProtoSerializationProvider extends ChannelSerializationProvider {
  import io.greenbus.edge.stream.channel.Channels._

  private def serializer[A, B <: Message](f: A => B): A => Array[Byte] = {
    def ser(obj: A): Array[Byte] = {
      f(obj).toByteArray
    }
    ser
  }

  private def stream(array: Array[Byte], length: Int): ByteArrayInputStream = {
    new ByteArrayInputStream(array, 0, length)
  }

  private def deserializer[A, B](parser: ByteArrayInputStream => A, reader: A => Either[String, B]): (Array[Byte], Int) => Either[String, B] = {
    def deser(array: Array[Byte], length: Int): Either[String, B] = {
      reader(parser(stream(array, length)))
    }
    deser
  }

  def serializerFor[A](desc: ChannelDescriptor[A]): (A) => Array[Byte] = {
    desc match {
      case d: PeerSubscriptionSetSenderDesc => serializer(ProtocolConversions.subSetToProto)
      case d: PeerEventReceiverDesc => serializer(ProtocolConversions.eventBatchToProto)
      case d: PeerServiceRequestsDesc => serializer(ProtocolConversions.servReqBatchToProto)
      case d: PeerServiceResponsesDesc => serializer(ProtocolConversions.servRespBatchToProto)
      case d: SubSubscriptionSetDesc => serializer(ProtocolConversions.subSetToProto)
      case d: SubEventReceiverDesc => serializer(ProtocolConversions.eventBatchToProto)
      case d: SubServiceRequestsDesc => serializer(ProtocolConversions.servReqBatchToProto)
      case d: SubServiceResponsesDesc => serializer(ProtocolConversions.servRespBatchToProto)
      case d: GateSubscriptionSetSenderDesc => serializer(ProtocolConversions.subSetToProto)
      case d: GateEventReceiverDesc => serializer(ProtocolConversions.gatewayEventsToProto)
      case d: GateServiceRequestsDesc => serializer(ProtocolConversions.servReqBatchToProto)
      case d: GateServiceResponsesDesc => serializer(ProtocolConversions.servRespBatchToProto)
      case _ => throw new IllegalArgumentException(s"Channel descriptor not recognized: " + desc)
    }

  }

  def deserializerFor[A](desc: ChannelDescriptor[A]): (Array[Byte], Int) => Either[String, A] = {
    desc match {
      case d: PeerSubscriptionSetSenderDesc => deserializer(proto.SubscriptionSetUpdate.parseFrom, ProtocolConversions.subSetFromProto)
      case d: PeerEventReceiverDesc => deserializer(proto.EventBatch.parseFrom, ProtocolConversions.eventBatchFromProto)
      case d: PeerServiceRequestsDesc => deserializer(proto.ServiceRequestBatch.parseFrom, ProtocolConversions.servReqBatchFromProto)
      case d: PeerServiceResponsesDesc => deserializer(proto.ServiceResponseBatch.parseFrom, ProtocolConversions.servRespBatchFromProto)
      case d: SubSubscriptionSetDesc => deserializer(proto.SubscriptionSetUpdate.parseFrom, ProtocolConversions.subSetFromProto)
      case d: SubEventReceiverDesc => deserializer(proto.EventBatch.parseFrom, ProtocolConversions.eventBatchFromProto)
      case d: SubServiceRequestsDesc => deserializer(proto.ServiceRequestBatch.parseFrom, ProtocolConversions.servReqBatchFromProto)
      case d: SubServiceResponsesDesc => deserializer(proto.ServiceResponseBatch.parseFrom, ProtocolConversions.servRespBatchFromProto)
      case d: GateSubscriptionSetSenderDesc => deserializer(proto.SubscriptionSetUpdate.parseFrom, ProtocolConversions.subSetFromProto)
      case d: GateEventReceiverDesc => deserializer(proto.GatewayClientEvents.parseFrom, ProtocolConversions.gatewayEventsFromProto)
      case d: GateServiceRequestsDesc => deserializer(proto.ServiceRequestBatch.parseFrom, ProtocolConversions.servReqBatchFromProto)
      case d: GateServiceResponsesDesc => deserializer(proto.ServiceResponseBatch.parseFrom, ProtocolConversions.servRespBatchFromProto)
      case _ => throw new IllegalArgumentException(s"Channel descriptor not recognized: " + desc)
    }
  }
}
