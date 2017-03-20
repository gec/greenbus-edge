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

import io.greenbus.edge.amqp.impl.AmqpIoImpl
import io.greenbus.edge.thread.CallMarshaller

import scala.concurrent.Future

trait AmqpListener {
  def close(): Unit
}

object AmqpService {
  def build(threadId: Option[String] = None): AmqpService = {
    new AmqpIoImpl(threadId)
  }
}
trait AmqpService {
  def close(): Unit
  def eventLoop: CallMarshaller
  def connect(host: String, port: Int, timeoutMs: Long): Future[ChannelSessionSource]
  def listen(host: String, port: Int, handler: AmqpChannelServer): Future[AmqpListener]
}
