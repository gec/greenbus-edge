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
package io.greenbus.edge.japi.impl

import java.util
import java.util.function.Consumer

import io.greenbus.edge
import io.greenbus.edge.api
import io.greenbus.edge.api.KeyMetadata
import io.greenbus.edge.data.japi.{ IndexableValue, SampleValue, Value }
import io.greenbus.edge.data.japi.impl.DataConversions
import io.greenbus.edge.japi
import io.greenbus.edge.japi.flow.Receiver
import io.greenbus.edge.japi._

import scala.collection.JavaConverters._

class EndpointBuilderShim(eb: api.EndpointBuilder) extends japi.EndpointBuilder {

  def setMetadata(metadata: util.Map[Path, Value]): Unit = {
    val m = metadata.asScala.map { case (p, v) => (Conversions.convertPathToScala(p), DataConversions.convertValueToScala(v)) }.toMap
    eb.setMetadata(m)
  }

  def addSeries(path: Path, metadata: util.Map[Path, Value]): SeriesValueHandle = {
    val m = metadata.asScala.map { case (p, v) => (Conversions.convertPathToScala(p), DataConversions.convertValueToScala(v)) }.toMap
    val handle = eb.seriesValue(Conversions.convertPathToScala(path), KeyMetadata(Map(), m))
    new SeriesValueHandle {
      def update(value: SampleValue, timeMs: Long): Unit = {
        handle.update(DataConversions.convertSampleValueToScala(value), timeMs)
      }
    }
  }

  def addKeyValue(path: Path, metadata: util.Map[Path, Value]): LatestKeyValueHandle = {
    val m = metadata.asScala.map { case (p, v) => (Conversions.convertPathToScala(p), DataConversions.convertValueToScala(v)) }.toMap
    val handle = eb.latestKeyValue(Conversions.convertPathToScala(path), KeyMetadata(Map(), m))
    new LatestKeyValueHandle {
      def update(value: Value): Unit = {
        handle.update(DataConversions.convertValueToScala(value))
      }
    }
  }

  def addEvents(path: Path, metadata: util.Map[Path, Value]): TopicEventHandle = {
    val m = metadata.asScala.map { case (p, v) => (Conversions.convertPathToScala(p), DataConversions.convertValueToScala(v)) }.toMap
    val handle = eb.topicEventValue(Conversions.convertPathToScala(path), KeyMetadata(Map(), m))
    new TopicEventHandle {
      def update(topic: Path, value: Value, timeMs: Long): Unit = {
        handle.update(Conversions.convertPathToScala(topic), DataConversions.convertValueToScala(value), timeMs)
      }
    }
  }

  def addActiveSet(path: Path, metadata: util.Map[Path, Value]): ActiveSetHandle = {
    val m = metadata.asScala.map { case (p, v) => (Conversions.convertPathToScala(p), DataConversions.convertValueToScala(v)) }.toMap
    val handle = eb.activeSet(Conversions.convertPathToScala(path), KeyMetadata(Map(), m))
    new ActiveSetHandle {
      def update(value: util.Map[IndexableValue, Value]): Unit = {
        val map = value.asScala.map { case (p, v) => (DataConversions.convertIndexableValueToScala(p), DataConversions.convertValueToScala(v)) }.toMap
        handle.update(map)
      }
    }
  }

  def addOutputStatus(path: Path, metadata: util.Map[Path, Value]): OutputStatusHandle = {
    val m = metadata.asScala.map { case (p, v) => (Conversions.convertPathToScala(p), DataConversions.convertValueToScala(v)) }.toMap
    val handle = eb.outputStatus(Conversions.convertPathToScala(path), KeyMetadata(Map(), m))
    new OutputStatusHandle {
      def update(status: OutputKeyStatus): Unit = {
        handle.update(Conversions.convertOutputKeyStatusToScala(status))
      }
    }
  }

  def registerOutput(path: Path): Receiver[OutputParams, OutputResult] = {
    val rcv = eb.registerOutput(Conversions.convertPathToScala(path))

    new Receiver[japi.OutputParams, japi.OutputResult] {
      def bind(responder: flow.Responder[OutputParams, OutputResult]): Unit = {
        val rsp = new edge.flow.Responder[api.OutputParams, api.OutputResult] {
          def handle(obj: api.OutputParams, respond: (api.OutputResult) => Unit): Unit = {
            responder.handle(Conversions.convertOutputParamsToJava(obj), new Consumer[japi.OutputResult] {
              def accept(t: OutputResult): Unit = {
                respond(Conversions.convertOutputResultToScala(t))
              }
            })
          }
        }
        rcv.bind(rsp)
      }
    }
  }

  def build(): ProducerHandle = {
    val h = eb.build()
    new ProducerHandle {
      def flush(): Unit = h.flush()
      def close(): Unit = h.close()
    }
  }
}
