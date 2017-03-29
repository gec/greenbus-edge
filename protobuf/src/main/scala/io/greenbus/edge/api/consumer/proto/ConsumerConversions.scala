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
package io.greenbus.edge.api.consumer.proto

import io.greenbus.edge.util.EitherUtil._
import io.greenbus.edge.api
import io.greenbus.edge.api.IndexableValue
import io.greenbus.edge.api.consumer.proto
import io.greenbus.edge.api.proto.EndpointDescriptor
import io.greenbus.edge.api.proto.convert.{ Conversions, OutputConversions, ValueConversions }

import scala.collection.JavaConversions._

object ConsumerConversions {

  def toProto(obj: api.IndexSubscriptionParams): proto.IndexSubscriptionParams = {
    val b = proto.IndexSubscriptionParams.newBuilder()
    obj.endpointPrefixes.map(ValueConversions.toProto).foreach(b.addEndpointPrefixes)
    obj.endpointIndexes.map(Conversions.toProto).foreach(b.addEndpointIndex)
    obj.dataKeyIndexes.map(Conversions.toProto).foreach(b.addDataKeyIndexes)
    obj.outputKeyIndexes.map(Conversions.toProto).foreach(b.addOutputKeyIndexes)
    b.build()
  }
  def fromProto(msg: proto.IndexSubscriptionParams): Either[String, api.IndexSubscriptionParams] = {
    for {
      prefixes <- rightSequence(msg.getEndpointPrefixesList.map(ValueConversions.fromProto))
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
    obj.series.map(ValueConversions.toProto).foreach(b.addSeries)
    obj.keyValues.map(ValueConversions.toProto).foreach(b.addKeyValues)
    obj.topicEvent.map(ValueConversions.toProto).foreach(b.addTopicEvents)
    obj.activeSet.map(ValueConversions.toProto).foreach(b.addActiveSets)
    b.build()
  }
  def fromProto(msg: proto.DataKeySubscriptionParams): Either[String, api.DataKeySubscriptionParams] = {
    for {
      ser <- rightSequence(msg.getSeriesList.map(ValueConversions.fromProto))
      end <- rightSequence(msg.getKeyValuesList.map(ValueConversions.fromProto))
      data <- rightSequence(msg.getTopicEventsList.map(ValueConversions.fromProto))
      out <- rightSequence(msg.getActiveSetsList.map(ValueConversions.fromProto))
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
    obj.descriptors.map(ValueConversions.toProto).foreach(b.addDescriptors)
    b.setDataKeys(toProto(obj.dataKeys))
    obj.outputKeys.map(ValueConversions.toProto).foreach(b.addOutputKeys)
    b.setIndexing(toProto(obj.indexing))
    b.build()
  }
  def fromProto(msg: proto.SubscriptionParams): Either[String, api.SubscriptionParams] = {
    for {
      descs <- rightSequence(msg.getDescriptorsList.map(ValueConversions.fromProto))
      data <- if (msg.hasDataKeys) fromProto(msg.getDataKeys).map(r => Some(r)) else Right(None)
      outKeys <- rightSequence(msg.getOutputKeysList.map(ValueConversions.fromProto))
      index <- if (msg.hasIndexing) fromProto(msg.getIndexing).map(r => Some(r)) else Right(None)
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
    b.setTopic(ValueConversions.toProto(obj.topic))
    b.setValue(ValueConversions.toProto(obj.value))
    b.setTime(obj.time)
    b.build()
  }
  def fromProto(msg: proto.TopicEventUpdate): Either[String, api.TopicEventUpdate] = {
    if (msg.hasValue && msg.hasTopic) {
      for {
        path <- ValueConversions.fromProto(msg.getTopic)
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        api.TopicEventUpdate(path, value, msg.getTime)
      }
    } else {
      Left("TopicEventUpdate missing key")
    }
  }

  def toProto(obj: (api.IndexableValue, api.Value)): proto.MapKeyPair = {
    val b = proto.MapKeyPair.newBuilder()
    b.setKey(ValueConversions.toProto(obj._1))
    b.setValue(ValueConversions.toProto(obj._2))
    b.build()
  }
  def fromProto(msg: proto.MapKeyPair): Either[String, (api.IndexableValue, api.Value)] = {
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
    obj.set.map(ValueConversions.toProto).foreach(b.addValue)
    obj.removes.map(ValueConversions.toProto).foreach(b.addRemoves)
    obj.adds.map(ValueConversions.toProto).foreach(b.addAdds)
    b.build()
  }
  def fromProto(msg: proto.EndpointSetUpdate): Either[String, api.EndpointSetUpdate] = {
    for {
      value <- rightSequence(msg.getValueList.map(ValueConversions.fromProto))
      removes <- rightSequence(msg.getRemovesList.map(ValueConversions.fromProto))
      adds <- rightSequence(msg.getAddsList.map(ValueConversions.fromProto))
    } yield {
      api.EndpointSetUpdate(value.toSet, removes.toSet, adds.toSet)
    }
  }

  def toProto(obj: api.KeySetUpdate): proto.KeySetUpdate = {
    val b = proto.KeySetUpdate.newBuilder()
    obj.set.map(ValueConversions.toProto).foreach(b.addValue)
    obj.removes.map(ValueConversions.toProto).foreach(b.addRemoves)
    obj.adds.map(ValueConversions.toProto).foreach(b.addAdds)
    b.build()
  }
  def fromProto(msg: proto.KeySetUpdate): Either[String, api.KeySetUpdate] = {
    for {
      value <- rightSequence(msg.getValueList.map(ValueConversions.fromProto))
      removes <- rightSequence(msg.getRemovesList.map(ValueConversions.fromProto))
      adds <- rightSequence(msg.getAddsList.map(ValueConversions.fromProto))
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

  /*def toProto(obj: api.IdentifiedEdgeUpdate): proto.IdentifiedEdgeUpdate = {
    val b = proto.IdentifiedEdgeUpdate.newBuilder()
    obj match {
      case up: api.IdEndpointUpdate => b.setEndpointUpdate(toProto(up))
      case up: api.IdDataKeyUpdate => b.setSeriesUpdate(toProto(up))
      case up: api.IdOutputKeyUpdate => b.setTopicEventUpdate(toProto(up))
      case up: api.ActiveSetUpdate => b.setActiveSetUpdate(toProto(up))
    }
    b.build()
  }
  def fromProto(msg: proto.IdentifiedEdgeUpdate): Either[String, api.IdentifiedEdgeUpdate] = {
    msg.getTypesCase match {
      case proto.IdentifiedEdgeUpdate.TypesCase.KEY_VALUE_UPDATE => fromProto(msg.getKeyValueUpdate)
      case proto.IdentifiedEdgeUpdate.TypesCase.SERIES_UPDATE => fromProto(msg.getSeriesUpdate)
      case proto.IdentifiedEdgeUpdate.TypesCase.TOPIC_EVENT_UPDATE => fromProto(msg.getTopicEventUpdate)
      case proto.IdentifiedEdgeUpdate.TypesCase.ACTIVE_SET_UPDATE => fromProto(msg.getActiveSetUpdate)
    }
  }*/

  /*def updateToProto[A, Msg](obj: api.EdgeDataStatus[A], toProtoFun: A => Msg): (proto.StatusType, Option[Msg]) = {
    obj match {
      case api.Pending => (proto.StatusType.PENDING, None)
      case api.DataUnresolved => (proto.StatusType.DATA_UNRESOLVED, None)
      case api.ResolvedAbsent => (proto.StatusType.RESOLVED_ABSENT, None)
      case v: api.ResolvedValue[A] => (proto.StatusType.RESOLVED_VALUE, Some(toProtoFun(v.value)))
      case api.Disconnected => (proto.StatusType.DISCONNECTED, None)
    }
  }*/
  def updateToProto[A, B](obj: api.EdgeDataStatus[A]): (proto.StatusType, Option[A]) = {
    obj match {
      case api.Pending => (proto.StatusType.PENDING, None)
      case api.DataUnresolved => (proto.StatusType.DATA_UNRESOLVED, None)
      case api.ResolvedAbsent => (proto.StatusType.RESOLVED_ABSENT, None)
      case v: api.ResolvedValue[A] => (proto.StatusType.RESOLVED_VALUE, Some(v.value))
      case api.Disconnected => (proto.StatusType.DISCONNECTED, None)
    }
  }

  def updateFromProto[A](msg: proto.StatusType, vOpt: Option[A]): Either[String, api.EdgeDataStatus[A]] = {
    msg match {
      case proto.StatusType.PENDING => Right(api.Pending)
      case proto.StatusType.DATA_UNRESOLVED => Right(api.DataUnresolved)
      case proto.StatusType.RESOLVED_ABSENT => Right(api.ResolvedAbsent)
      case proto.StatusType.RESOLVED_VALUE => vOpt.map(v => Right(api.ResolvedValue(v))).getOrElse(Left("Resolved value with no value update"))
      case proto.StatusType.DISCONNECTED => Right(api.Disconnected)
      case proto.StatusType.UNRECOGNIZED => Left("Unrecognized status type")
    }
  }

  /*

sealed trait IdentifiedEdgeUpdate
case class IdEndpointUpdate(id: EndpointId, data: EdgeDataStatus[EndpointDescriptor]) extends IdentifiedEdgeUpdate
case class IdDataKeyUpdate(id: EndpointPath, data: EdgeDataStatus[DataKeyUpdate]) extends IdentifiedEdgeUpdate
case class IdOutputKeyUpdate(id: EndpointPath, data: EdgeDataStatus[OutputKeyUpdate]) extends IdentifiedEdgeUpdate

case class IdEndpointPrefixUpdate(prefix: Path, data: EdgeDataStatus[EndpointSetUpdate]) extends IdentifiedEdgeUpdate
case class IdEndpointIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[EndpointSetUpdate]) extends IdentifiedEdgeUpdate
case class IdDataKeyIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[KeySetUpdate]) extends IdentifiedEdgeUpdate
case class IdOutputKeyIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[KeySetUpdate]) extends IdentifiedEdgeUpdate

sealed trait EdgeDataStatus[+A]
case object Pending extends EdgeDataStatus[Nothing]
case object DataUnresolved extends EdgeDataStatus[Nothing]
case object ResolvedAbsent extends EdgeDataStatus[Nothing]
case class ResolvedValue[A](value: A) extends EdgeDataStatus[A]
case object Disconnected extends EdgeDataStatus[Nothing]
   */

  def toProto(obj: api.IdEndpointUpdate): proto.IdEndpointUpdate = {
    val b = proto.IdEndpointUpdate.newBuilder()
    b.setId(ValueConversions.toProto(obj.id))
    val (status, optV) = updateToProto(obj.data)
    b.setType(status)
    optV.map(Conversions.toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.IdEndpointUpdate): Either[String, api.IdEndpointUpdate] = {
    if (msg.hasId) {

      val vOptEith = if (msg.hasValue) Conversions.fromProto(msg.getValue).map(r => Some(r)) else Right(None)

      for {
        id <- ValueConversions.fromProto(msg.getId)
        vOpt <- vOptEith
        status <- updateFromProto(msg.getType, vOpt)
      } yield {
        api.IdEndpointUpdate(id, status)
      }
    } else {
      Left("MapKeyPair missing key")
    }
  }
  /*


 message IdentifiedEdgeUpdate {
    oneof type {
        IdEndpointUpdate endpoint_update = 1;
        IdDataKeyUpdate data_key_update = 2;
        IdOutputKeyUpdate output_key_update = 3;

        IdEndpointPrefixUpdate endpoint_prefix_update = 4;
        IdEndpointIndexUpdate endpoint_index_update = 5;
        IdDataKeyIndexUpdate data_key_index_update = 6;
        IdOutputKeyIndexUpdate output_key_index_update = 7;
    }
}


message IdEndpointUpdate {
    edge.EndpointId id = 1;
    StatusType type = 2;
    edge.EndpointDescriptor value = 3;
}
message IdDataKeyUpdate {
    edge.EndpointPath id = 1;
    StatusType type = 2;
    DataKeyUpdate value = 3;
}
message IdOutputKeyUpdate {
    edge.EndpointPath id = 1;
    StatusType type = 2;
    OutputKeyUpdate value = 3;
}




message IdEndpointPrefixUpdate {
    edge.Path id = 1;
    StatusType type = 2;
    EndpointSetUpdate value = 3;
}
message IdEndpointIndexUpdate {
    edge.Path id = 1;
    StatusType type = 2;
    EndpointSetUpdate value = 3;
}
message IdDataKeyIndexUpdate {
    edge.IndexSpecifier id = 1;
    StatusType type = 2;
    KeySetUpdate value = 3;
}
message IdOutputKeyIndexUpdate {
    edge.IndexSpecifier id = 1;
    StatusType type = 2;
    KeySetUpdate value = 3;
}


   */
}

