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
package io.greenbus.edge.peer

import io.greenbus.edge.amqp.AmqpService
import io.greenbus.edge.thread.EventThreadService

import scala.concurrent.ExecutionContext

trait EdgeServices {

  def start(): Unit
  def shutdown(): Unit

  def consumer: ConsumerServices
  def producer: ProducerServices
}

object AmqpEdgeService {
  def build(host: String, port: Int, retryIntervalMs: Long)(implicit ec: ExecutionContext): EdgeServices = {
    new AmqpEdgeService(host, port, retryIntervalMs)
  }
}
class AmqpEdgeService(host: String, port: Int, retryIntervalMs: Long)(implicit ec: ExecutionContext) extends EdgeServices {

  private val service = AmqpService.build(Some("AMQP"))
  private val exe = EventThreadService.build("event")
  private val retrier = new RetryingConnector(exe, service, host, port, 10000)

  private val consumerServices = StreamConsumerManager.build(exe)
  retrier.bindPeerLinkObserver(consumerServices)

  private val producerServices = new ProducerManager(exe)
  retrier.bindGatewayObserver(producerServices)

  def start(): Unit = {
    retrier.start()
  }

  def shutdown(): Unit = {
    retrier.close()
    service.close()
    exe.close()
  }

  def consumer: ConsumerServices = {
    consumerServices
  }

  def producer: ProducerServices = {
    producerServices
  }
}
