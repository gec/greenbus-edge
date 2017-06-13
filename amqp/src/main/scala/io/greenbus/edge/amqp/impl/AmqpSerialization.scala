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

import java.nio.BufferOverflowException

import com.typesafe.scalalogging.LazyLogging
import org.apache.qpid.proton.Proton
import org.apache.qpid.proton.amqp.Binary
import org.apache.qpid.proton.amqp.messaging.Data
import org.apache.qpid.proton.message.Message

import scala.annotation.tailrec

trait AmqpListener {
  def close(): Unit
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