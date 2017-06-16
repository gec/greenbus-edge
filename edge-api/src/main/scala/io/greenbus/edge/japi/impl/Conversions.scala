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

import io.greenbus.edge.api.{ DataKeyUpdate, EndpointDescriptor, EndpointSetUpdate, OutputKeyUpdate }

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

  def convertSubscriptionParamsToJava(obj: api.SubscriptionParams): japi.SubscriptionParams = {
    new japi.SubscriptionParams(
      obj.endpointPrefixSet.toSeq.map(convertPathToJava).asJava,
      obj.endpointDescriptors.toSeq.map(convertEndpointIdToJava).asJava,
      obj.dataKeys.toSeq.map(convertEndpointPathToJava).asJava,
      obj.outputKeys.toSeq.map(convertEndpointPathToJava).asJava)
  }
  def convertSubscriptionParamsToScala(obj: japi.SubscriptionParams): api.SubscriptionParams = {
    api.SubscriptionParams(
      obj.getEndpointPrefixSet.asScala.map(convertPathToScala).toSet,
      obj.getEndpointDescriptors.asScala.map(convertEndpointIdToScala).toSet,
      obj.getDataKeys.asScala.map(convertEndpointPathToScala).toSet,
      obj.getOutputKeys.asScala.map(convertEndpointPathToScala).toSet,
      Set())
  }

  def convertIdDataKeyUpdateToJava(obj: api.IdDataKeyUpdate): japi.IdDataKeyUpdate = {
    val (status, v) = obj.data match {
      case api.Pending => (japi.EdgeDataStatus.Pending, null)
      case api.DataUnresolved => (japi.EdgeDataStatus.Unresolved, null)
      case api.ResolvedAbsent => (japi.EdgeDataStatus.Absent, null)
      case status: api.ResolvedValue[DataKeyUpdate] => (japi.EdgeDataStatus.Resolved, convertDataKeyUpdateToJava(status.value))
    }
    new japi.IdDataKeyUpdate(convertEndpointPathToJava(obj.id), status, v)
  }
  def convertIdDataKeyUpdateToScala(obj: japi.IdDataKeyUpdate): api.IdDataKeyUpdate = {
    val endPath = convertEndpointPathToScala(obj.getId)
    val data = obj.getStatus match {
      case japi.EdgeDataStatus.Pending => api.Pending
      case japi.EdgeDataStatus.Unresolved => api.DataUnresolved
      case japi.EdgeDataStatus.Absent => api.ResolvedAbsent
      case japi.EdgeDataStatus.Resolved => api.ResolvedValue(convertDataKeyUpdateToScala(obj.getUpdate.get()))
    }
    api.IdDataKeyUpdate(endPath, data)
  }

  def convertDataKeyUpdateToJava(obj: api.DataKeyUpdate): japi.DataKeyUpdate = {
    val descConverted = obj.descriptor.map(convertDataKeyDescriptorToJava)
    new japi.DataKeyUpdate(descConverted.orNull, convertDataUpdateToJava(obj.value))
  }
  def convertDataKeyUpdateToScala(obj: japi.DataKeyUpdate): api.DataKeyUpdate = {
    val descOpt = if (obj.getDescriptor.isPresent) Some(obj.getDescriptor.get()) else None
    val descMappedOpt = descOpt.map(convertDataKeyDescriptorToScala)

    api.DataKeyUpdate(descMappedOpt, convertDataUpdateToScala(obj.getUpdate))
  }

  def convertDataUpdateToJava(obj: api.DataUpdate): japi.DataUpdate = {
    obj match {
      case up: api.KeyValueUpdate => new japi.KeyValueUpdate(convertValueToJava(up.value))
      case up: api.SeriesUpdate => new japi.SeriesUpdate(convertSampleValueToJava(up.value), up.time)
      case up: api.TopicEventUpdate => new japi.TopicEventUpdate(convertPathToJava(up.topic), convertValueToJava(up.value), up.time)
      case up: api.ActiveSetUpdate =>
        val value = up.value.map { case (k, v) => (convertIndexableValueToJava(k), convertValueToJava(v)) }.asJava
        val removed = up.removes.map(convertIndexableValueToJava).asJava
        val added = up.added.map { case (k, v) => (convertIndexableValueToJava(k), convertValueToJava(v)) }.toMap.asJava
        val modified = up.modified.map { case (k, v) => (convertIndexableValueToJava(k), convertValueToJava(v)) }.toMap.asJava
        new japi.ActiveSetUpdate(value, removed, added, modified)
    }
  }
  def convertDataUpdateToScala(obj: japi.DataUpdate): api.DataUpdate = {
    obj match {
      case up: japi.KeyValueUpdate => api.KeyValueUpdate(convertValueToScala(up.getValue))
      case up: japi.SeriesUpdate => api.SeriesUpdate(convertSampleValueToScala(up.getValue), up.getTime)
      case up: japi.TopicEventUpdate => api.TopicEventUpdate(convertPathToScala(up.getTopic), convertValueToScala(up.getValue), up.getTime)
      case up: japi.ActiveSetUpdate =>
        val value = up.getValue.asScala.map { case (k, v) => (convertIndexableValueToScala(k), convertValueToScala(v)) }.toMap
        val removed = up.getRemoves.asScala.map(convertIndexableValueToScala).toSet
        val added = up.getAdded.asScala.map { case (k, v) => (convertIndexableValueToScala(k), convertValueToScala(v)) }.toSet
        val modified = up.getModified.asScala.map { case (k, v) => (convertIndexableValueToScala(k), convertValueToScala(v)) }.toSet
        api.ActiveSetUpdate(value, removed, added, modified)
    }
  }

  def convertIdOutputKeyUpdateToJava(obj: api.IdOutputKeyUpdate): japi.IdOutputKeyUpdate = {
    val (status, v) = obj.data match {
      case api.Pending => (japi.EdgeDataStatus.Pending, null)
      case api.DataUnresolved => (japi.EdgeDataStatus.Unresolved, null)
      case api.ResolvedAbsent => (japi.EdgeDataStatus.Absent, null)
      case status: api.ResolvedValue[OutputKeyUpdate] => (japi.EdgeDataStatus.Resolved, convertOutputKeyUpdateToJava(status.value))
    }
    new japi.IdOutputKeyUpdate(convertEndpointPathToJava(obj.id), status, v)
  }
  def convertIdOutputKeyUpdateToScala(obj: japi.IdOutputKeyUpdate): api.IdOutputKeyUpdate = {
    val endPath = convertEndpointPathToScala(obj.getId)
    val data = obj.getStatus match {
      case japi.EdgeDataStatus.Pending => api.Pending
      case japi.EdgeDataStatus.Unresolved => api.DataUnresolved
      case japi.EdgeDataStatus.Absent => api.ResolvedAbsent
      case japi.EdgeDataStatus.Resolved => api.ResolvedValue(convertOutputKeyUpdateToScala(obj.getUpdate.get()))
    }
    api.IdOutputKeyUpdate(endPath, data)
  }

  def convertOutputKeyUpdateToJava(obj: api.OutputKeyUpdate): japi.OutputKeyUpdate = {
    val descConverted = obj.descriptor.map(convertOutputKeyDescriptorToJava)
    new japi.OutputKeyUpdate(descConverted.orNull, convertOutputKeyStatusToJava(obj.value))
  }
  def convertOutputKeyUpdateToScala(obj: japi.OutputKeyUpdate): api.OutputKeyUpdate = {
    val descOpt = if (obj.getDescriptor.isPresent) Some(obj.getDescriptor.get()) else None
    val descMappedOpt = descOpt.map(convertOutputKeyDescriptorToScala)

    api.OutputKeyUpdate(descMappedOpt, convertOutputKeyStatusToScala(obj.getUpdate))
  }

  def convertOutputKeyStatusToJava(obj: api.OutputKeyStatus): japi.OutputKeyStatus = {
    new japi.OutputKeyStatus(obj.session, obj.sequence, obj.valueOpt.map(convertValueToJava).orNull)
  }
  def convertOutputKeyStatusToScala(obj: japi.OutputKeyStatus): api.OutputKeyStatus = {
    api.OutputKeyStatus(obj.getSession, obj.getSequence, Option(obj.getValue).map(convertValueToScala))
  }

  def convertOutputParamsToJava(obj: api.OutputParams): japi.OutputParams = {
    new japi.OutputParams(
      obj.sessionOpt.orNull,
      obj.sequenceOpt.map(l => new java.lang.Long(l)).orNull,
      obj.compareValueOpt.map(convertValueToJava).orNull,
      obj.outputValueOpt.map(convertValueToJava).orNull)
  }
  def convertOutputParamsToScala(obj: japi.OutputParams): api.OutputParams = {
    api.OutputParams(
      Option(
        obj.getSession),
      Option(obj.getSequence),
      Option(obj.getCompareValue).map(convertValueToScala),
      Option(obj.getOutputValue).map(convertValueToScala))
  }

  def convertOutputRequestToJava(obj: api.OutputRequest): japi.OutputRequest = {
    new japi.OutputRequest(convertEndpointPathToJava(obj.key), convertOutputParamsToJava(obj.value))
  }
  def convertOutputRequestToScala(obj: japi.OutputRequest): api.OutputRequest = {
    api.OutputRequest(convertEndpointPathToScala(obj.getPath), convertOutputParamsToScala(obj.getValue))
  }

  def convertOutputResultToJava(obj: api.OutputResult): japi.OutputResult = {
    obj match {
      case r: api.OutputSuccess => new japi.OutputSuccess(r.valueOpt.map(convertValueToJava).orNull)
      case r: api.OutputFailure => new japi.OutputFailure(r.reason)
    }
  }
  def convertOutputResultToScala(obj: japi.OutputResult): api.OutputResult = {
    obj match {
      case r: japi.OutputSuccess => api.OutputSuccess(Option(r.getValue).map(convertValueToScala))
      case r: japi.OutputFailure => api.OutputFailure(r.getReason)
    }
  }

  def convertIdEndpointUpdateToJava(obj: api.IdEndpointUpdate): japi.IdEndpointUpdate = {
    val (status, v) = obj.data match {
      case api.Pending => (japi.EdgeDataStatus.Pending, null)
      case api.DataUnresolved => (japi.EdgeDataStatus.Unresolved, null)
      case api.ResolvedAbsent => (japi.EdgeDataStatus.Absent, null)
      case status: api.ResolvedValue[EndpointDescriptor] => (japi.EdgeDataStatus.Resolved, convertEndpointDescriptorToJava(status.value))
    }
    new japi.IdEndpointUpdate(convertEndpointIdToJava(obj.id), status, v)
  }
  def convertIdEndpointUpdateToScala(obj: japi.IdEndpointUpdate): api.IdEndpointUpdate = {
    val id = convertEndpointIdToScala(obj.getId)
    val data = obj.getStatus match {
      case japi.EdgeDataStatus.Pending => api.Pending
      case japi.EdgeDataStatus.Unresolved => api.DataUnresolved
      case japi.EdgeDataStatus.Absent => api.ResolvedAbsent
      case japi.EdgeDataStatus.Resolved => api.ResolvedValue(convertEndpointDescriptorToScala(obj.getUpdate.get()))
    }
    api.IdEndpointUpdate(id, data)
  }

  def convertIdEndpointPrefixUpdateToJava(obj: api.IdEndpointPrefixUpdate): japi.IdEndpointPrefixUpdate = {
    val (status, v) = obj.data match {
      case api.Pending => (japi.EdgeDataStatus.Pending, null)
      case api.DataUnresolved => (japi.EdgeDataStatus.Unresolved, null)
      case api.ResolvedAbsent => (japi.EdgeDataStatus.Absent, null)
      case status: api.ResolvedValue[EndpointSetUpdate] => (japi.EdgeDataStatus.Resolved, convertEndpointSetUpdateToJava(status.value))
    }
    new japi.IdEndpointPrefixUpdate(convertPathToJava(obj.prefix), status, v)
  }
  def convertIdEndpointPrefixUpdateToScala(obj: japi.IdEndpointPrefixUpdate): api.IdEndpointPrefixUpdate = {
    val id = convertPathToScala(obj.getPrefix)
    val data = obj.getStatus match {
      case japi.EdgeDataStatus.Pending => api.Pending
      case japi.EdgeDataStatus.Unresolved => api.DataUnresolved
      case japi.EdgeDataStatus.Absent => api.ResolvedAbsent
      case japi.EdgeDataStatus.Resolved => api.ResolvedValue(convertEndpointSetUpdateToScala(obj.getUpdate.get()))
    }
    api.IdEndpointPrefixUpdate(id, data)
  }

  def convertEndpointSetUpdateToJava(obj: api.EndpointSetUpdate): japi.EndpointSetUpdate = {
    new japi.EndpointSetUpdate(
      obj.set.map(convertEndpointIdToJava).asJava,
      obj.removes.map(convertEndpointIdToJava).asJava,
      obj.adds.map(convertEndpointIdToJava).asJava)
  }
  def convertEndpointSetUpdateToScala(obj: japi.EndpointSetUpdate): api.EndpointSetUpdate = {
    api.EndpointSetUpdate(
      obj.getValue.asScala.map(convertEndpointIdToScala).toSet,
      obj.getRemoves.asScala.map(convertEndpointIdToScala).toSet,
      obj.getAdds.asScala.map(convertEndpointIdToScala).toSet)
  }

  def convertIdentifiedEdgeUpdateToJava(obj: api.IdentifiedEdgeUpdate): japi.IdentifiedEdgeUpdate = {
    obj match {
      case up: api.IdEndpointPrefixUpdate => convertIdEndpointPrefixUpdateToJava(up)
      case up: api.IdEndpointUpdate => convertIdEndpointUpdateToJava(up)
      case up: api.IdDataKeyUpdate => convertIdDataKeyUpdateToJava(up)
      case up: api.IdOutputKeyUpdate => convertIdOutputKeyUpdateToJava(up)
    }
  }
  def convertIdentifiedEdgeUpdateToScala(obj: japi.IdentifiedEdgeUpdate): api.IdentifiedEdgeUpdate = {
    obj match {
      case up: japi.IdEndpointPrefixUpdate => convertIdEndpointPrefixUpdateToScala(up)
      case up: japi.IdEndpointUpdate => convertIdEndpointUpdateToScala(up)
      case up: japi.IdDataKeyUpdate => convertIdDataKeyUpdateToScala(up)
      case up: japi.IdOutputKeyUpdate => convertIdOutputKeyUpdateToScala(up)
    }
  }

}
