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

import scala.collection.JavaConverters._

object Conversions {
  import io.greenbus.edge.api
  import io.greenbus.edge.japi
  import io.greenbus.edge.data.japi.impl.DataConversions._

  def convertPathToJava(path: api.Path): japi.Path = {
    new japi.Path(path.parts.asJava)
  }
  def convertPathToScala(path: japi.Path): api.Path = {
    api.Path(path.getValue.asScala)
  }

  def convertEndpointIdToJava(obj: api.EndpointId): japi.EndpointId = {
    new japi.EndpointId(convertPathToJava(obj.path))
  }
  def convertEndpointIdToScala(obj: japi.EndpointId): api.EndpointId = {
    api.EndpointId(convertPathToScala(obj.getPath))
  }

  def convertEndpointPathToJava(obj: api.EndpointPath): japi.EndpointPath = {
    new japi.EndpointPath(convertEndpointIdToJava(obj.endpoint), convertPathToJava(obj.path))
  }
  def convertEndpointPathToScala(obj: japi.EndpointPath): api.EndpointPath = {
    api.EndpointPath(convertEndpointIdToScala(obj.getEndpointId), convertPathToScala(obj.getPath))
  }

  def convertEndpointDescriptorToJava(obj: api.EndpointDescriptor): japi.EndpointDescriptor = {
    val metadata = obj.metadata.map {
      case (p, v) => (convertPathToJava(p), convertValueToJava(v))
    }.asJava

    val dataKeys = obj.dataKeySet.map {
      case (p, v) => (convertPathToJava(p), convertDataKeyDescriptorToJava(v))
    }.asJava

    val outputKeys = obj.outputKeySet.map {
      case (p, v) => (convertPathToJava(p), convertOutputKeyDescriptorToJava(v))
    }.asJava

    new japi.EndpointDescriptor(metadata, dataKeys, outputKeys)
  }
  def convertEndpointDescriptorToScala(obj: japi.EndpointDescriptor): api.EndpointDescriptor = {
    val metadata = obj.getMetadata.asScala.map { case (p, v) => (convertPathToScala(p), convertValueToScala(v)) }.toMap
    val dataKeys = obj.getDataKeySet.asScala.map { case (p, v) => (convertPathToScala(p), convertDataKeyDescriptorToScala(v)) }.toMap
    val outputKeys = obj.getOutputKeySet.asScala.map { case (p, v) => (convertPathToScala(p), convertOutputKeyDescriptorToScala(v)) }.toMap

    api.EndpointDescriptor(Map(), metadata, dataKeys, outputKeys)
  }

  def convertDataKeyDescriptorToJava(obj: api.DataKeyDescriptor): japi.DataKeyDescriptor = {
    val metadata = obj.metadata.map {
      case (p, v) => (convertPathToJava(p), convertValueToJava(v))
    }.asJava

    obj match {
      case desc: api.LatestKeyValueDescriptor => new japi.LatestKeyValueDescriptor(metadata)
      case desc: api.TimeSeriesValueDescriptor => new japi.TimeSeriesValueDescriptor(metadata)
      case desc: api.TopicEventValueDescriptor => new japi.TopicEventValueDescriptor(metadata)
      case desc: api.ActiveSetValueDescriptor => new japi.ActiveSetValueDescriptor(metadata)
    }
  }
  def convertDataKeyDescriptorToScala(obj: japi.DataKeyDescriptor): api.DataKeyDescriptor = {
    val metadata = obj.getMetadata.asScala.map { case (p, v) => (convertPathToScala(p), convertValueToScala(v)) }.toMap

    obj match {
      case desc: japi.LatestKeyValueDescriptor => api.LatestKeyValueDescriptor(Map(), metadata)
      case desc: japi.TimeSeriesValueDescriptor => api.TimeSeriesValueDescriptor(Map(), metadata)
      case desc: japi.TopicEventValueDescriptor => api.TopicEventValueDescriptor(Map(), metadata)
      case desc: japi.ActiveSetValueDescriptor => api.ActiveSetValueDescriptor(Map(), metadata)
    }
  }

  def convertOutputKeyDescriptorToJava(obj: api.OutputKeyDescriptor): japi.OutputKeyDescriptor = {
    val metadata = obj.metadata.map {
      case (p, v) => (convertPathToJava(p), convertValueToJava(v))
    }.asJava

    new japi.OutputKeyDescriptor(metadata)
  }
  def convertOutputKeyDescriptorToScala(obj: japi.OutputKeyDescriptor): api.OutputKeyDescriptor = {
    val metadata = obj.getMetadata.asScala.map { case (p, v) => (convertPathToScala(p), convertValueToScala(v)) }.toMap

    api.OutputKeyDescriptor(Map(), metadata)
  }

}
