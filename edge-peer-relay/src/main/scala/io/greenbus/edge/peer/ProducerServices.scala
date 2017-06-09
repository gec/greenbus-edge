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

import io.greenbus.edge.api.EndpointId
import io.greenbus.edge.api.stream.EndpointBuilder
import io.greenbus.edge.stream.GatewayProxyChannel

trait ProducerServices {
  def endpointBuilder(id: EndpointId): EndpointBuilder
}

trait GatewayLinkObserver {
  def connected(channel: GatewayProxyChannel): Unit
}

/*
class ProducerManager(eventThread: SchedulableCallMarshaller) extends ProducerServices with GatewayLinkObserver {

  private val gatewaySource = GatewayRouteSource.build(eventThread)

  private val provider = new StreamProducerBinder(gatewaySource)

  def connected(channel: GatewayProxyChannel): Unit = {
    gatewaySource.connect(channel)
  }

  def endpointBuilder(id: EndpointId): EndpointBuilder = {
    new EndpointProducerBuilderImpl(id, eventThread, provider)
  }
}*/
