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
package io.greenbus.edge.api.proto.convert

import io.greenbus.edge.api
import io.greenbus.edge.api.proto
import io.greenbus.edge.data._
import io.greenbus.edge.data.proto.convert.ValueConversions

import scala.collection.JavaConversions._

object Conversions {
  import ConversionUtil._

  def toProto(obj: api.DataKeyDescriptor): proto.DataKeyDescriptor = {
    val b = proto.DataKeyDescriptor.newBuilder()
    obj.indexes.map(toProto).foreach(b.addIndexes)
    obj.metadata.map(toProto).foreach(b.addMetadata)

    obj match {
      case d: api.LatestKeyValueDescriptor => b.setLatestKeyValue(proto.LatestKeyValueDescriptor.newBuilder().build())
      case d: api.TimeSeriesValueDescriptor => b.setTimeSeriesValue(proto.TimeSeriesValueDescriptor.newBuilder().build())
      case d: api.EventTopicValueDescriptor => b.setEventTopicValue(proto.EventTopicValueDescriptor.newBuilder().build())
      case d: api.ActiveSetValueDescriptor => b.setActiveSetValue(proto.ActiveSetValueDescriptor.newBuilder().build())
    }
    b.build()
  }
  def fromProto(msg: proto.DataKeyDescriptor): Either[String, api.DataKeyDescriptor] = {

    for {
      indexes <- rightSequence(msg.getIndexesList.map(fromProto))
      metadata <- rightSequence(msg.getMetadataList.map(fromProto))
    } yield {
      msg.getValueTypesCase match {
        case proto.DataKeyDescriptor.ValueTypesCase.LATEST_KEY_VALUE =>
          api.LatestKeyValueDescriptor(indexes.toMap, metadata.toMap)
        case proto.DataKeyDescriptor.ValueTypesCase.TIME_SERIES_VALUE =>
          api.TimeSeriesValueDescriptor(indexes.toMap, metadata.toMap)
        case proto.DataKeyDescriptor.ValueTypesCase.EVENT_TOPIC_VALUE =>
          api.EventTopicValueDescriptor(indexes.toMap, metadata.toMap)
        case proto.DataKeyDescriptor.ValueTypesCase.ACTIVE_SET_VALUE =>
          api.ActiveSetValueDescriptor(indexes.toMap, metadata.toMap)
        case _ =>
          api.UnrecognizedValueDescriptor(indexes.toMap, metadata.toMap)
      }
    }
  }

  def toProto(obj: api.OutputKeyDescriptor): proto.OutputKeyDescriptor = {
    val b = proto.OutputKeyDescriptor.newBuilder()
    obj.indexes.map(toProto).foreach(b.addIndexes)
    obj.metadata.map(toProto).foreach(b.addMetadata)
    b.build()
  }
  def fromProto(msg: proto.OutputKeyDescriptor): Either[String, api.OutputKeyDescriptor] = {
    for {
      indexes <- rightSequence(msg.getIndexesList.map(fromProto))
      metadata <- rightSequence(msg.getMetadataList.map(fromProto))
    } yield {
      api.OutputKeyDescriptor(indexes.toMap, metadata.toMap)
    }
  }

  def toProto(obj: (api.Path, IndexableValue)): proto.IndexKeyValue = {
    proto.IndexKeyValue.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(ValueConversions.toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.IndexKeyValue): Either[String, (api.Path, IndexableValue)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: (api.Path, Value)): proto.MetadataKeyValue = {
    proto.MetadataKeyValue.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(ValueConversions.toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.MetadataKeyValue): Either[String, (api.Path, Value)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: (api.Path, api.DataKeyDescriptor)): proto.DataKeyValue = {
    proto.DataKeyValue.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.DataKeyValue): Either[String, (api.Path, api.DataKeyDescriptor)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: (api.Path, api.OutputKeyDescriptor)): proto.OutputKeyValue = {
    proto.OutputKeyValue.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.OutputKeyValue): Either[String, (api.Path, api.OutputKeyDescriptor)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: api.EndpointDescriptor): proto.EndpointDescriptor = {
    val b = proto.EndpointDescriptor.newBuilder()
    obj.indexes.map(toProto).foreach(b.addIndexes)
    obj.metadata.map(toProto).foreach(b.addMetadata)
    obj.dataKeySet.map(toProto).foreach(b.addDataKeySet)
    obj.outputKeySet.map(toProto).foreach(b.addOutputKeySet)
    b.build()
  }
  def fromProto(msg: proto.EndpointDescriptor): Either[String, api.EndpointDescriptor] = {
    for {
      indexes <- rightSequence(msg.getIndexesList.map(fromProto))
      metadata <- rightSequence(msg.getMetadataList.map(fromProto))
      dataKeys <- rightSequence(msg.getDataKeySetList.map(fromProto))
      outputKeys <- rightSequence(msg.getOutputKeySetList.map(fromProto))
    } yield {
      api.EndpointDescriptor(
        indexes = indexes.toMap,
        metadata = metadata.toMap,
        dataKeySet = dataKeys.toMap,
        outputKeySet = outputKeys.toMap)
    }
  }

  def toProto(obj: api.Path): proto.Path = {
    val b = proto.Path.newBuilder()
    obj.parts.foreach(b.addPart)
    b.build()
  }

  def fromProtoSimple(msg: proto.Path): api.Path = {
    api.Path(msg.getPartList.toVector)
  }

  def fromProto(msg: proto.Path): Either[String, api.Path] = {
    Right(fromProtoSimple(msg))
  }

  def toProto(obj: api.EndpointId): proto.EndpointId = {
    val b = proto.EndpointId.newBuilder()
    b.setName(toProto(obj.path))
    b.build()
  }

  def fromProto(msg: proto.EndpointId): Either[String, api.EndpointId] = {
    if (msg.hasName) {
      Right(api.EndpointId(fromProtoSimple(msg.getName)))
    } else {
      Left("EndpointId unrecognized")
    }
  }

  def toProto(obj: api.EndpointPath): proto.EndpointPath = {
    val b = proto.EndpointPath.newBuilder()
    b.setEndpointId(toProto(obj.endpoint))
    b.setKey(toProto(obj.path))
    b.build()
  }

  def fromProto(msg: proto.EndpointPath): Either[String, api.EndpointPath] = {
    if (msg.hasEndpointId && msg.hasKey) {
      for {
        id <- fromProto(msg.getEndpointId)
        key <- fromProto(msg.getKey)
      } yield {
        api.EndpointPath(id, key)
      }
    } else {
      Left("EndpointPath missing endpoint id or key")
    }
  }

  def toProto(obj: api.DynamicPath): proto.DynamicPath = {
    val b = proto.DynamicPath.newBuilder()
    b.setSet(obj.set)
    b.setPath(toProto(obj.path))
    b.build()
  }

  def fromProto(msg: proto.DynamicPath): Either[String, api.DynamicPath] = {
    if (msg.hasPath) {
      fromProto(msg.getPath).map(p => api.DynamicPath(msg.getSet, p))
    } else {
      Left("EndpointId unrecognized")
    }
  }

  def toProto(obj: api.EndpointDynamicPath): proto.EndpointDynamicPath = {
    val b = proto.EndpointDynamicPath.newBuilder()
    b.setEndpointId(toProto(obj.endpoint))
    b.setKey(toProto(obj.key))
    b.build()
  }
  def fromProto(msg: proto.EndpointDynamicPath): Either[String, api.EndpointDynamicPath] = {
    if (msg.hasEndpointId && msg.hasKey) {
      for {
        id <- fromProto(msg.getEndpointId)
        key <- fromProto(msg.getKey)
      } yield {
        api.EndpointDynamicPath(id, key)
      }
    } else {
      Left("EndpointDynamicPath missing endpoint id or key")
    }
  }

  def fromProtoSimple(msg: proto.UUID): java.util.UUID = {
    new java.util.UUID(msg.getHigh, msg.getLow)
  }
  def toProto(uuid: java.util.UUID): proto.UUID = {
    proto.UUID.newBuilder().setLow(uuid.getLeastSignificantBits).setHigh(uuid.getMostSignificantBits).build()
  }

  def toOptionUInt64(v: Long): proto.OptionalUInt64 = {
    proto.OptionalUInt64.newBuilder().setValue(v).build()
  }
  def toOptionString(s: String): proto.OptionalString = {
    proto.OptionalString.newBuilder().setValue(s).build()
  }
  def toOptionBool(s: Boolean): proto.OptionalBool = {
    proto.OptionalBool.newBuilder().setValue(s).build()
  }
}

object OutputConversions {

  def toProto(obj: api.OutputKeyStatus): proto.OutputKeyStatus = {
    val b = proto.OutputKeyStatus.newBuilder()
    b.setSequenceSession(Conversions.toProto(obj.session))
    b.setSequence(obj.sequence)
    obj.valueOpt.map(ValueConversions.toProto).foreach(b.setValue)
    b.build()
  }
  def fromProto(msg: proto.OutputKeyStatus): Either[String, api.OutputKeyStatus] = {
    if (msg.hasSequenceSession) {
      val sess = Conversions.fromProtoSimple(msg.getSequenceSession)
      for {
        valueOpt <- if (msg.hasValue) ValueConversions.fromProto(msg.getValue).map(r => Some(r)) else Right(None)
      } yield {
        api.OutputKeyStatus(sess, msg.getSequence, valueOpt)
      }
    } else {
      Left("OutputKeyStatus missing sequence session")
    }
  }

  def toProto(obj: api.OutputSuccess): proto.OutputSuccess = {
    val b = proto.OutputSuccess.newBuilder()
    obj.valueOpt.map(ValueConversions.toProto).foreach(b.setResult)
    b.build()
  }
  def fromProto(msg: proto.OutputSuccess): Either[String, api.OutputSuccess] = {
    if (msg.hasResult) {
      ValueConversions.fromProto(msg.getResult).map(r => api.OutputSuccess(Some(r)))
    } else {
      Right(api.OutputSuccess(None))
    }
  }

  def toProto(obj: api.OutputFailure): proto.OutputFailure = {
    proto.OutputFailure.newBuilder().setMessage(obj.reason).build()
  }

  def toProto(obj: api.OutputResult): proto.OutputResult = {
    val b = proto.OutputResult.newBuilder()
    obj match {
      case r: api.OutputSuccess => b.setSuccess(toProto(r))
      case r: api.OutputFailure => b.setFailure(toProto(r))
    }
    b.build()
  }
  def fromProto(msg: proto.OutputResult): Either[String, api.OutputResult] = {
    msg.getResultCase match {
      case proto.OutputResult.ResultCase.FAILURE => Right(api.OutputFailure(msg.getFailure.getMessage))
      case proto.OutputResult.ResultCase.SUCCESS => fromProto(msg.getSuccess)
      case _ => Left("OutputResult type unrecognized")
    }
  }

  def toProto(obj: api.OutputParams): proto.OutputParams = {
    val b = proto.OutputParams.newBuilder()
    obj.sessionOpt.map(Conversions.toProto).foreach(b.setSequenceSession)
    obj.sequenceOpt.map(Conversions.toOptionUInt64).foreach(b.setSequence)
    obj.compareValueOpt.map(ValueConversions.toProto).foreach(b.setCompareValue)
    obj.outputValueOpt.map(ValueConversions.toProto).foreach(b.setOutputValue)
    b.build()
  }
  def fromProto(msg: proto.OutputParams): Either[String, api.OutputParams] = {
    val sessOpt = if (msg.hasSequenceSession) Some(Conversions.fromProtoSimple(msg.getSequenceSession)) else None
    val compareEither = if (msg.hasCompareValue) ValueConversions.fromProto(msg.getCompareValue).map(r => Some(r)) else Right(None)
    val outputEither = if (msg.hasOutputValue) ValueConversions.fromProto(msg.getOutputValue).map(r => Some(r)) else Right(None)
    val seqOpt = if (msg.hasSequence) Some(msg.getSequence.getValue) else None
    for {
      compareOpt <- compareEither
      outOpt <- outputEither
    } yield {
      api.OutputParams(sessOpt, seqOpt, compareOpt, outOpt)
    }
  }
}

object ConversionUtil {

  def rightSequence[L, R](seq: Seq[Either[L, R]]): Either[L, Seq[R]] = {
    var continue = true
    var leftOpt = Option.empty[L]
    val results = Vector.newBuilder[R]
    val iter = seq.iterator
    while (continue && iter.hasNext) {
      val obj = iter.next()
      obj match {
        case Left(l) =>
          leftOpt = Some(l)
          continue = false
        case Right(r) =>
          results += r
      }
    }
    leftOpt match {
      case None => Right(results.result())
      case Some(l) => Left(l)
    }
  }
}