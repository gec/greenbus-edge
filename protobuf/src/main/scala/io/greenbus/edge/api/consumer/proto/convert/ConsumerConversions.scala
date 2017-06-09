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
package io.greenbus.edge.api.consumer.proto.convert

import io.greenbus.edge.api
import io.greenbus.edge.api.consumer.proto
import io.greenbus.edge.api.proto.convert.{ Conversions, OutputConversions }
import io.greenbus.edge.data.proto.convert.ValueConversions
import io.greenbus.edge.data.{ IndexableValue, Value }
import io.greenbus.edge.util.EitherUtil._

import scala.collection.JavaConversions._

object ConsumerConversions {

  def toProto(obj: api.IndexSubscriptionParams): proto.IndexSubscriptionParams = {
    val b = proto.IndexSubscriptionParams.newBuilder()
    obj.endpointPrefixes.map(Conversions.toProto).foreach(b.addEndpointPrefixes)
    obj.endpointIndexes.map(Conversions.toProto).foreach(b.addEndpointIndex)
    obj.dataKeyIndexes.map(Conversions.toProto).foreach(b.addDataKeyIndexes)
    obj.outputKeyIndexes.map(Conversions.toProto).foreach(b.addOutputKeyIndexes)
    b.build()
  }
  def fromProto(msg: proto.IndexSubscriptionParams): Either[String, api.IndexSubscriptionParams] = {
    for {
      prefixes <- rightSequence(msg.getEndpointPrefixesList.map(Conversions.fromProto))
      endIndexes <- rightSequence(msg.getEndpointIndexList.map(Conversions.fromProto))
      dataIndexes <- rightSequence(msg.getDataKeyIndexesList.map(Conversions.fromProto))
      outputIndexes <- rightSequence(msg.getOutputKeyIndexesList.map(Conversions.fromProto))
    } yield {
      api.IndexSubscriptionParams(
        endpointPrefixes = prefixes,
        endpointIndexes = endIndexes,
        dataKeyIndexes = dataIndexes,
        outputKeyIndexes = outputIndexes)
    }
  }

  def toProto(obj: api.DataKeySubscriptionParams): proto.DataKeySubscriptionParams = {
    val b = proto.DataKeySubscriptionParams.newBuilder()
    obj.series.map(Conversions.toProto).foreach(b.addSeries)
    obj.keyValues.map(Conversions.toProto).foreach(b.addKeyValues)
    obj.topicEvent.map(Conversions.toProto).foreach(b.addTopicEvents)
    obj.activeSet.map(Conversions.toProto).foreach(b.addActiveSets)
    b.build()
  }
  def fromProto(msg: proto.DataKeySubscriptionParams): Either[String, api.DataKeySubscriptionParams] = {
    for {
      ser <- rightSequence(msg.getSeriesList.map(Conversions.fromProto))
      end <- rightSequence(msg.getKeyValuesList.map(Conversions.fromProto))
      data <- rightSequence(msg.getTopicEventsList.map(Conversions.fromProto))
      out <- rightSequence(msg.getActiveSetsList.map(Conversions.fromProto))
    } yield {
      api.DataKeySubscriptionParams(
        series = ser,
        keyValues = end,
        topicEvent = data,
        activeSet = out)
    }
  }

  def toProto(obj: api.SubscriptionParams): proto.SubscriptionParams = {
    val b = proto.SubscriptionParams.newBuilder()
    obj.descriptors.map(Conversions.toProto).foreach(b.addDescriptors)
    b.setDataParams(toProto(obj.dataKeys))
    obj.outputKeys.map(Conversions.toProto).foreach(b.addOutputKeys)
    b.setIndexParams(toProto(obj.indexing))
    b.build()
  }
  def fromProto(msg: proto.SubscriptionParams): Either[String, api.SubscriptionParams] = {
    for {
      descs <- rightSequence(msg.getDescriptorsList.map(Conversions.fromProto))
      data <- if (msg.hasDataParams) fromProto(msg.getDataParams).map(r => Some(r)) else Right(None)
      outKeys <- rightSequence(msg.getOutputKeysList.map(Conversions.fromProto))
      index <- if (msg.hasIndexParams) fromProto(msg.getIndexParams).map(r => Some(r)) else Right(None)
    } yield {
      api.SubscriptionParams(
        descriptors = descs,
        dataKeys = data.getOrElse(api.DataKeySubscriptionParams()),
        outputKeys = outKeys,
        indexing = index.getOrElse(api.IndexSubscriptionParams()))
    }
  }

  def toProto(obj: api.KeyValueUpdate): proto.KeyValueUpdate = {
    val b = proto.KeyValueUpdate.newBuilder()
    b.setValue(ValueConversions.toProto(obj.value))
    b.build()
  }
  def fromProto(msg: proto.KeyValueUpdate): Either[String, api.KeyValueUpdate] = {
    if (msg.hasValue) {
      for {
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        api.KeyValueUpdate(value)
      }
    } else {
      Left("KeyValueUpdate missing key")
    }
  }

  def toProto(obj: api.SeriesUpdate): proto.SeriesUpdate = {
    val b = proto.SeriesUpdate.newBuilder()
    b.setValue(ValueConversions.toProto(obj.value))
    b.setTime(obj.time)
    b.build()
  }
  def fromProto(msg: proto.SeriesUpdate): Either[String, api.SeriesUpdate] = {
    if (msg.hasValue) {
      for {
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        api.SeriesUpdate(value, msg.getTime)
      }
    } else {
      Left("SeriesUpdate missing key")
    }
  }

  def toProto(obj: api.TopicEventUpdate): proto.TopicEventUpdate = {
    val b = proto.TopicEventUpdate.newBuilder()
    b.setTopic(Conversions.toProto(obj.topic))
    b.setValue(ValueConversions.toProto(obj.value))
    b.setTime(obj.time)
    b.build()
  }
  def fromProto(msg: proto.TopicEventUpdate): Either[String, api.TopicEventUpdate] = {
    if (msg.hasValue && msg.hasTopic) {
      for {
        path <- Conversions.fromProto(msg.getTopic)
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        api.TopicEventUpdate(path, value, msg.getTime)
      }
    } else {
      Left("TopicEventUpdate missing key")
    }
  }

  def toProto(obj: (IndexableValue, Value)): proto.MapKeyPair = {
    val b = proto.MapKeyPair.newBuilder()
    b.setKey(ValueConversions.toProto(obj._1))
    b.setValue(ValueConversions.toProto(obj._2))
    b.build()
  }
  def fromProto(msg: proto.MapKeyPair): Either[String, (IndexableValue, Value)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- ValueConversions.fromProto(msg.getKey)
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("MapKeyPair missing key")
    }
  }

  def toProto(obj: api.ActiveSetUpdate): proto.ActiveSetUpdate = {
    val b = proto.ActiveSetUpdate.newBuilder()
    obj.value.map(toProto).foreach(b.addValue)
    obj.removes.map(ValueConversions.toProto).foreach(b.addRemoves)
    obj.added.map(toProto).foreach(b.addAdds)
    obj.modified.map(toProto).foreach(b.addModifies)
    b.build()
  }
  def fromProto(msg: proto.ActiveSetUpdate): Either[String, api.ActiveSetUpdate] = {
    for {
      value <- rightSequence(msg.getValueList.map(fromProto))
      removes <- rightSequence(msg.getRemovesList.map(ValueConversions.fromProto))
      adds <- rightSequence(msg.getAddsList.map(fromProto))
      modifies <- rightSequence(msg.getModifiesList.map(fromProto))
    } yield {
      api.ActiveSetUpdate(value.toMap, removes.toSet, adds.toSet, modifies.toSet)
    }
  }

  def toProto(obj: api.EndpointSetUpdate): proto.EndpointSetUpdate = {
    val b = proto.EndpointSetUpdate.newBuilder()
    obj.set.map(Conversions.toProto).foreach(b.addValue)
    obj.removes.map(Conversions.toProto).foreach(b.addRemoves)
    obj.adds.map(Conversions.toProto).foreach(b.addAdds)
    b.build()
  }
  def fromProto(msg: proto.EndpointSetUpdate): Either[String, api.EndpointSetUpdate] = {
    for {
      value <- rightSequence(msg.getValueList.map(Conversions.fromProto))
      removes <- rightSequence(msg.getRemovesList.map(Conversions.fromProto))
      adds <- rightSequence(msg.getAddsList.map(Conversions.fromProto))
    } yield {
      api.EndpointSetUpdate(value.toSet, removes.toSet, adds.toSet)
    }
  }

  def toProto(obj: api.KeySetUpdate): proto.KeySetUpdate = {
    val b = proto.KeySetUpdate.newBuilder()
    obj.set.map(Conversions.toProto).foreach(b.addValue)
    obj.removes.map(Conversions.toProto).foreach(b.addRemoves)
    obj.adds.map(Conversions.toProto).foreach(b.addAdds)
    b.build()
  }
  def fromProto(msg: proto.KeySetUpdate): Either[String, api.KeySetUpdate] = {
    for {
      value <- rightSequence(msg.getValueList.map(Conversions.fromProto))
      removes <- rightSequence(msg.getRemovesList.map(Conversions.fromProto))
      adds <- rightSequence(msg.getAddsList.map(Conversions.fromProto))
    } yield {
      api.KeySetUpdate(value.toSet, removes.toSet, adds.toSet)
    }
  }

  def toProto(obj: api.DataKeyUpdate): proto.DataKeyUpdate = {
    val b = proto.DataKeyUpdate.newBuilder()
    obj.descriptor.map(Conversions.toProto).foreach(b.setDescriptorUpdate)
    obj.value match {
      case obj: api.KeyValueUpdate => b.setKeyValueUpdate(toProto(obj))
      case obj: api.SeriesUpdate => b.setSeriesUpdate(toProto(obj))
      case obj: api.TopicEventUpdate => b.setTopicEventUpdate(toProto(obj))
      case obj: api.ActiveSetUpdate => b.setActiveSetUpdate(toProto(obj))
    }
    b.build()
  }
  def fromProto(msg: proto.DataKeyUpdate): Either[String, api.DataKeyUpdate] = {

    val descOptEith = if (msg.hasDescriptorUpdate) Conversions.fromProto(msg.getDescriptorUpdate).map(r => Some(r)) else Right(None)

    val valueEith = msg.getTypesCase match {
      case proto.DataKeyUpdate.TypesCase.KEY_VALUE_UPDATE => fromProto(msg.getKeyValueUpdate)
      case proto.DataKeyUpdate.TypesCase.SERIES_UPDATE => fromProto(msg.getSeriesUpdate)
      case proto.DataKeyUpdate.TypesCase.TOPIC_EVENT_UPDATE => fromProto(msg.getTopicEventUpdate)
      case proto.DataKeyUpdate.TypesCase.ACTIVE_SET_UPDATE => fromProto(msg.getActiveSetUpdate)
      case proto.DataKeyUpdate.TypesCase.TYPES_NOT_SET => Left("Type not set")
    }

    for {
      descOpt <- descOptEith
      value <- valueEith
    } yield {
      api.DataKeyUpdate(descOpt, value)
    }
  }

  def toProto(obj: api.OutputKeyUpdate): proto.OutputKeyUpdate = {
    val b = proto.OutputKeyUpdate.newBuilder()
    obj.descriptor.map(Conversions.toProto).foreach(b.setDescriptorUpdate)
    b.setStatusUpdate(OutputConversions.toProto(obj.value))
    b.build()
  }
  def fromProto(msg: proto.OutputKeyUpdate): Either[String, api.OutputKeyUpdate] = {
    if (msg.hasStatusUpdate) {
      val descOptEith = if (msg.hasDescriptorUpdate) Conversions.fromProto(msg.getDescriptorUpdate).map(r => Some(r)) else Right(None)
      for {
        descOpt <- descOptEith
        value <- OutputConversions.fromProto(msg.getStatusUpdate)
      } yield {
        api.OutputKeyUpdate(descOpt, value)
      }
    } else {
      Left("Output key update lacking status update")
    }
  }

  def updateToProto[A, B](obj: api.EdgeDataStatus[A]): (proto.StatusType, Option[A]) = {
    obj match {
      case api.Pending => (proto.StatusType.PENDING, None)
      case api.DataUnresolved => (proto.StatusType.DATA_UNRESOLVED, None)
      case api.ResolvedAbsent => (proto.StatusType.RESOLVED_ABSENT, None)
      case v: api.ResolvedValue[A] => (proto.StatusType.RESOLVED_VALUE, Some(v.value))
    }
  }

  def updateFromProto[A](msg: proto.StatusType, vOpt: Option[A]): Either[String, api.EdgeDataStatus[A]] = {
    msg match {
      case proto.StatusType.PENDING => Right(api.Pending)
      case proto.StatusType.DATA_UNRESOLVED => Right(api.DataUnresolved)
      case proto.StatusType.RESOLVED_ABSENT => Right(api.ResolvedAbsent)
      case proto.StatusType.RESOLVED_VALUE => vOpt.map(v => Right(api.ResolvedValue(v))).getOrElse(Left("Resolved value with no value update"))
      case proto.StatusType.UNRECOGNIZED => Left("Unrecognized status type")
    }
  }

  def toProto(obj: api.IdEndpointUpdate): proto.IdEndpointUpdate = {
    val b = proto.IdEndpointUpdate.newBuilder()
    b.setId(Conversions.toProto(obj.id))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(Conversions.toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdEndpointUpdate): Either[String, api.IdEndpointUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) Conversions.fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- Conversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdEndpointUpdate(id, status)
      }
    } else {
      Left("IdEndpointUpdate missing id")
    }
  }

  def toProto(obj: api.IdDataKeyUpdate): proto.IdDataKeyUpdate = {
    val b = proto.IdDataKeyUpdate.newBuilder()
    b.setId(Conversions.toProto(obj.id))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdDataKeyUpdate): Either[String, api.IdDataKeyUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- Conversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdDataKeyUpdate(id, status)
      }
    } else {
      Left("IdDataKeyUpdate missing id")
    }
  }

  def toProto(obj: api.IdOutputKeyUpdate): proto.IdOutputKeyUpdate = {
    val b = proto.IdOutputKeyUpdate.newBuilder()
    b.setId(Conversions.toProto(obj.id))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdOutputKeyUpdate): Either[String, api.IdOutputKeyUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- Conversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdOutputKeyUpdate(id, status)
      }
    } else {
      Left("IdOutputKeyUpdate missing id")
    }
  }

  def toProto(obj: api.IdEndpointPrefixUpdate): proto.IdEndpointPrefixUpdate = {
    val b = proto.IdEndpointPrefixUpdate.newBuilder()
    b.setId(Conversions.toProto(obj.prefix))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdEndpointPrefixUpdate): Either[String, api.IdEndpointPrefixUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- Conversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdEndpointPrefixUpdate(id, status)
      }
    } else {
      Left("IdOutputKeyUpdate missing id")
    }
  }

  def toProto(obj: api.IdEndpointIndexUpdate): proto.IdEndpointIndexUpdate = {
    val b = proto.IdEndpointIndexUpdate.newBuilder()
    b.setId(Conversions.toProto(obj.specifier))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdEndpointIndexUpdate): Either[String, api.IdEndpointIndexUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- Conversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdEndpointIndexUpdate(id, status)
      }
    } else {
      Left("IdOutputKeyUpdate missing id")
    }
  }

  def toProto(obj: api.IdDataKeyIndexUpdate): proto.IdDataKeyIndexUpdate = {
    val b = proto.IdDataKeyIndexUpdate.newBuilder()
    b.setId(Conversions.toProto(obj.specifier))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdDataKeyIndexUpdate): Either[String, api.IdDataKeyIndexUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- Conversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdDataKeyIndexUpdate(id, status)
      }
    } else {
      Left("IdDataKeyIndexUpdate missing id")
    }
  }

  def toProto(obj: api.IdOutputKeyIndexUpdate): proto.IdOutputKeyIndexUpdate = {
    val b = proto.IdOutputKeyIndexUpdate.newBuilder()
    b.setId(Conversions.toProto(obj.specifier))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdOutputKeyIndexUpdate): Either[String, api.IdOutputKeyIndexUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- Conversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdOutputKeyIndexUpdate(id, status)
      }
    } else {
      Left("IdOutputKeyIndexUpdate missing id")
    }
  }

  def toProto(obj: api.IdentifiedEdgeUpdate): proto.IdentifiedEdgeUpdate = {
    val b = proto.IdentifiedEdgeUpdate.newBuilder()
    obj match {
      case obj: api.IdEndpointUpdate => b.setEndpointUpdate(toProto(obj))
      case obj: api.IdDataKeyUpdate => b.setDataKeyUpdate(toProto(obj))
      case obj: api.IdOutputKeyUpdate => b.setOutputKeyUpdate(toProto(obj))
      case obj: api.IdEndpointPrefixUpdate => b.setEndpointPrefixUpdate(toProto(obj))
      case obj: api.IdEndpointIndexUpdate => b.setEndpointIndexUpdate(toProto(obj))
      case obj: api.IdDataKeyIndexUpdate => b.setDataKeyIndexUpdate(toProto(obj))
      case obj: api.IdOutputKeyIndexUpdate => b.setOutputKeyIndexUpdate(toProto(obj))
    }
    b.build()
  }
  def fromProto(msg: proto.IdentifiedEdgeUpdate): Either[String, api.IdentifiedEdgeUpdate] = {
    msg.getTypeCase match {
      case proto.IdentifiedEdgeUpdate.TypeCase.ENDPOINT_UPDATE => fromProto(msg.getEndpointUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.DATA_KEY_UPDATE => fromProto(msg.getDataKeyUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.OUTPUT_KEY_UPDATE => fromProto(msg.getOutputKeyUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.ENDPOINT_PREFIX_UPDATE => fromProto(msg.getEndpointPrefixUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.ENDPOINT_INDEX_UPDATE => fromProto(msg.getEndpointIndexUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.DATA_KEY_INDEX_UPDATE => fromProto(msg.getDataKeyIndexUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.OUTPUT_KEY_INDEX_UPDATE => fromProto(msg.getOutputKeyIndexUpdate)
      case _ => Left(s"Unrecognized edge update type")
    }
  }
}

