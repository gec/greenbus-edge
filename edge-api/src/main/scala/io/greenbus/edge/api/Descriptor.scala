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

trait DataKeyDescriptor {
  def indexes: Map[Path, IndexableValue]
  def metadata: Map[Path, Value]
}

case class LatestKeyValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor
case class TimeSeriesValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor
case class EventTopicValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor
case class ActiveSetValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor

// For reading from proto
case class UnrecognizedValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor

case class OutputKeyDescriptor(
  indexes: Map[Path, IndexableValue],
  metadata: Map[Path, Value])

case class EndpointDescriptor(
  indexes: Map[Path, IndexableValue],
  metadata: Map[Path, Value],
  dataKeySet: Map[Path, DataKeyDescriptor],
  outputKeySet: Map[Path, OutputKeyDescriptor])

