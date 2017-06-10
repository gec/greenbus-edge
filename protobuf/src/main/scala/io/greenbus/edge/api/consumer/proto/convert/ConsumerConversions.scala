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

  def toProto(obj: api.SubscriptionParams): proto.SubscriptionParams = {
    val b = proto.SubscriptionParams.newBuilder()
    obj.endpointPrefixSet.map(Conversions.toProto).foreach(b.addEndpointPrefixSet)
    obj.endpointDescriptors.map(Conversions.toProto).foreach(b.addEndpointDescriptors)
    obj.dataKeys.map(Conversions.toProto).foreach(b.addDataKeys)
    obj.outputKeys.map(Conversions.toProto).foreach(b.addOutputKeys)
    obj.dynamicDataKeys.map(Conversions.toProto).foreach(b.addDynamicDataKeys)
    b.build()
  }
  def fromProto(msg: proto.SubscriptionParams): Either[String, api.SubscriptionParams] = {
    for {
      prefixSet <- rightSequence(msg.getEndpointPrefixSetList.map(Conversions.fromProto))
      descs <- rightSequence(msg.getEndpointDescriptorsList.map(Conversions.fromProto))
      dataKeys <- rightSequence(msg.getDataKeysList.map(Conversions.fromProto))
      outKeys <- rightSequence(msg.getOutputKeysList.map(Conversions.fromProto))
      dynDataKeys <- rightSequence(msg.getDynamicDataKeysList.map(Conversions.fromProto))
    } yield {
      api.SubscriptionParams(
        endpointPrefixSet = prefixSet.toSet,
        endpointDescriptors = descs.toSet,
        dataKeys = dataKeys.toSet,
        outputKeys = outKeys.toSet,
        dynamicDataKeys = dynDataKeys.toSet)
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

  def toProto(obj: api.IdDynamicDataKeyUpdate): proto.IdDynamicDataKeyUpdate = {
    val b = proto.IdDynamicDataKeyUpdate.newBuilder()
    b.setId(Conversions.toProto(obj.id))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdDynamicDataKeyUpdate): Either[String, api.IdDynamicDataKeyUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- Conversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdDynamicDataKeyUpdate(id, status)
      }
    } else {
      Left("IdDynamicDataKeyUpdate missing id")
    }
  }

  def toProto(obj: api.IdentifiedEdgeUpdate): proto.IdentifiedEdgeUpdate = {
    val b = proto.IdentifiedEdgeUpdate.newBuilder()
    obj match {
      case obj: api.IdEndpointUpdate => b.setEndpointUpdate(toProto(obj))
      case obj: api.IdDataKeyUpdate => b.setDataKeyUpdate(toProto(obj))
      case obj: api.IdOutputKeyUpdate => b.setOutputKeyUpdate(toProto(obj))
      case obj: api.IdEndpointPrefixUpdate => b.setEndpointPrefixUpdate(toProto(obj))
      case obj: api.IdDynamicDataKeyUpdate => b.setDynamicDataKeyUpdate(toProto(obj))
    }
    b.build()
  }
  def fromProto(msg: proto.IdentifiedEdgeUpdate): Either[String, api.IdentifiedEdgeUpdate] = {
    msg.getTypeCase match {
      case proto.IdentifiedEdgeUpdate.TypeCase.ENDPOINT_UPDATE => fromProto(msg.getEndpointUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.DATA_KEY_UPDATE => fromProto(msg.getDataKeyUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.OUTPUT_KEY_UPDATE => fromProto(msg.getOutputKeyUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.ENDPOINT_PREFIX_UPDATE => fromProto(msg.getEndpointPrefixUpdate)
      case proto.IdentifiedEdgeUpdate.TypeCase.DYNAMIC_DATA_KEY_UPDATE => fromProto(msg.getDynamicDataKeyUpdate)
      case _ => Left(s"Unrecognized edge update type")
    }
  }
}

