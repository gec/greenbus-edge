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
package io.greenbus.edge.proto.provider

import java.io.ByteArrayInputStream

import com.google.protobuf.{ ByteString, Message }
import io.greenbus.edge.ChannelDescriptor
import io.greenbus.edge.SerializationProvider
import io.greenbus.edge.proto.ClientSubscriptionParams

object EdgeProtobufProvider {

  def serialize[A, B <: Message](obj: A, convert: A => B): Array[Byte] = {
    val msg = convert(obj)
    msg.toByteArray
  }

  def deserialize[A, B <: Message](
    array: Array[Byte],
    length: Int,
    parse: ByteArrayInputStream => B,
    convert: B => Either[String, A]): Either[String, A] = {

    val in = new ByteArrayInputStream(array, 0, length)
    val msg = parse(in)
    convert(msg)
  }

}
class EdgeProtobufProvider extends SerializationProvider {
  import io.greenbus.edge.EdgeChannels._
  import io.greenbus.edge.proto.convert.MessageConversions._
  import EdgeProtobufProvider._

  def serializerFor[A](desc: ChannelDescriptor[A]): (A) => Array[Byte] = {
    desc match {
      case d: ClientSubscriptionRequestDesc => clientSubscriptionParamsToBytes
      case d: ClientSubscriptionNotificationDesc => clientSubscriptionNotificationToBytes
      case d: ClientOutputRequestDesc => clientOutputRequestMessageToBytes
      case d: ClientOutputResponseDesc => clientOutputResponseMessageToBytes
      case d: EndpointPublishDesc => endpointPublishMessageToBytes
      case d: PublisherOutputRequestDesc => publisherOutputRequestMessageToBytes
      case d: PublisherOutputResponseDesc => publisherOutputResponseMessageToBytes
      case _ => throw new IllegalArgumentException(s"No serializer registered for $desc")
    }
  }

  def deserializerFor[A](desc: ChannelDescriptor[A]): (Array[Byte], Int) => Either[String, A] = {
    desc match {
      case d: ClientSubscriptionRequestDesc => clientSubscriptionParamsFromBytes
      case d: ClientSubscriptionNotificationDesc => clientSubscriptionNotificationFromBytes
      case d: ClientOutputRequestDesc => clientOutputRequestMessageFromBytes
      case d: ClientOutputResponseDesc => clientOutputResponseMessageFromBytes
      case d: EndpointPublishDesc => endpointPublishMessageFromBytes
      case d: PublisherOutputRequestDesc => publisherOutputRequestMessageFromBytes
      case d: PublisherOutputResponseDesc => publisherOutputResponseMessageFromBytes
      case _ => throw new IllegalArgumentException(s"No deserializer registered for $desc")
    }
  }
}
