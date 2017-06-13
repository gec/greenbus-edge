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
package io.greenbus.edge.stream.gateway

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow.Sink
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.engine.CachingKeyStreamSubject
import io.greenbus.edge.stream.filter.StreamCache

trait DynamicTable {
  def subscribed(key: TypeValue): Unit
  def unsubscribed(key: TypeValue): Unit
}

case class AppendPublish(key: TableRow, values: Seq[TypeValue])
case class SetPublish(key: TableRow, value: Set[TypeValue])
case class MapPublish(key: TableRow, value: Map[TypeValue, TypeValue])
case class PublishBatch(
  appendUpdates: Seq[AppendPublish],
  mapUpdates: Seq[MapPublish],
  setUpdates: Seq[SetPublish])

case class RoutePublishConfig(
  appendKeys: Seq[(TableRow, SequenceCtx)],
  setKeys: Seq[(TableRow, SequenceCtx)],
  mapKeys: Seq[(TableRow, SequenceCtx)],
  dynamicTables: Seq[(String, DynamicTable)],
  handler: Sink[RouteServiceRequest])
