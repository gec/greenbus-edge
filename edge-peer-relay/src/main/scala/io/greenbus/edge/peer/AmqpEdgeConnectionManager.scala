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
import io.greenbus.edge.api.{ ConsumerService, ProducerService }
import io.greenbus.edge.thread.{ EventThreadService, SchedulableCallMarshaller }

import scala.concurrent.ExecutionContext

object AmqpEdgeConnectionManager {
  def build(host: String, port: Int, retryIntervalMs: Long = 10000, connectTimeoutMs: Long = 10000, appendLimitDefault: Int = 10)(implicit ec: ExecutionContext): AmqpEdgeConnectionManager = {
    new AmqpEdgeConnectionManager(host, port, retryIntervalMs, connectTimeoutMs, appendLimitDefault)
  }
}
class AmqpEdgeConnectionManager(host: String, port: Int, retryIntervalMs: Long, connectTimeoutMs: Long, appendLimitDefault: Int)(implicit ec: ExecutionContext) {
  private val service = AmqpService.build()
  private val exe = EventThreadService.build("event")

  private val retrier = new RetryingConnector(exe, service, host, port, retryIntervalMs, connectTimeoutMs)

  def start(): Unit = {
    retrier.start()
  }

  def shutdown(): Unit = {
    retrier.close()
    service.close()
    exe.close()
  }

  def bindConsumerServices(): ConsumerService = {
    val services = new PeerConsumerServices(s"consumer-$host:$port", exe)
    retrier.bindPeerLinkObserver(services)
    services
  }

  def bindProducerServices(): ProducerService = {
    val services = new PeerProducerServices(s"producer-$host:$port", exe, appendLimitDefault)
    retrier.bindGatewayObserver(services)
    services
  }

  def eventThread: SchedulableCallMarshaller = exe
}
