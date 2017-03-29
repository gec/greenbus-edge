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
package io.greenbus.edge.api

import io.greenbus.edge.flow.Source

case class IndexSubscriptionParams(
  endpointPrefixes: Seq[Path] = Seq(),
  endpointIndexes: Seq[IndexSpecifier] = Seq(),
  dataKeyIndexes: Seq[IndexSpecifier] = Seq(),
  outputKeyIndexes: Seq[IndexSpecifier] = Seq())

case class DataKeySubscriptionParams(
  series: Seq[EndpointPath] = Seq(),
  keyValues: Seq[EndpointPath] = Seq(),
  topicEvent: Seq[EndpointPath] = Seq(),
  activeSet: Seq[EndpointPath] = Seq())

case class SubscriptionParams(
  descriptors: Seq[EndpointId] = Seq(),
  dataKeys: DataKeySubscriptionParams = DataKeySubscriptionParams(),
  outputKeys: Seq[EndpointPath] = Seq(),
  indexing: IndexSubscriptionParams = IndexSubscriptionParams())

trait EdgeSubscription {
  def updates: Source[Seq[IdentifiedEdgeUpdate]]
  def close(): Unit
}

trait EdgeSubscriptionClient {
  def subscribe(params: SubscriptionParams): EdgeSubscription
}