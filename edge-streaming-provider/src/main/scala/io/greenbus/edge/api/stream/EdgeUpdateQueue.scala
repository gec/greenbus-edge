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
package io.greenbus.edge.api.stream

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.api.stream.AppendDataKeyCodec.{ LatestKeyValueCodec, SeriesCodec, TopicEventCodec }
import io.greenbus.edge.api.stream.KeyedSetDataKeyCodec.ActiveSetCodec
import io.greenbus.edge.api.stream.SetCodec.EndpointIdSetCodec
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.subscribe._
import io.greenbus.edge.flow._
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

trait EdgeUpdateQueue {
  def enqueue(update: IdentifiedEdgeUpdate): Unit
  def flush(): Unit
}

class BatchedSink[A](batchSink: Sink[Seq[A]]) {
  private val buffer = mutable.ArrayBuffer.empty[A]

  def enqueue(update: A): Unit = {
    buffer += update
  }

  def flush(): Unit = {
    val seq = buffer.toVector
    buffer.clear()
    batchSink.push(seq)
  }
}

