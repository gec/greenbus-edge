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
package io.greenbus.edge.stream

import io.greenbus.edge.flow._

trait GatewayProxy extends ServiceConsumer {
  def subscriptions: Source[Set[RowId]]
  def events: Sender[GatewayEvents, Boolean]
}
trait GatewayProxyChannel extends GatewayProxy with CloseableComponent

trait GatewayClientProxy extends ServiceProvider {
  def subscriptions: Sink[Set[RowId]]
  def events: Source[GatewayEvents]
}

trait GatewayClientProxyChannel extends GatewayClientProxy with CloseableComponent

case class GatewayEvents(routesUpdate: Option[Set[TypeValue]], events: Seq[StreamEvent])
