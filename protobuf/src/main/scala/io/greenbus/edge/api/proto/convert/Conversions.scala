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

import com.google.protobuf.ByteString
import io.greenbus.edge.api.proto
import io.greenbus.edge.api
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

  def toProto(obj: (api.Path, api.IndexableValue)): proto.IndexKeyValue = {
    proto.IndexKeyValue.newBuilder()
      .setKey(ValueConversions.toProto(obj._1))
      .setValue(ValueConversions.toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.IndexKeyValue): Either[String, (api.Path, api.IndexableValue)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- ValueConversions.fromProto(msg.getKey)
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: (api.Path, api.Value)): proto.MetadataKeyValue = {
    proto.MetadataKeyValue.newBuilder()
      .setKey(ValueConversions.toProto(obj._1))
      .setValue(ValueConversions.toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.MetadataKeyValue): Either[String, (api.Path, api.Value)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- ValueConversions.fromProto(msg.getKey)
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
      .setKey(ValueConversions.toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.DataKeyValue): Either[String, (api.Path, api.DataKeyDescriptor)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- ValueConversions.fromProto(msg.getKey)
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
      .setKey(ValueConversions.toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.OutputKeyValue): Either[String, (api.Path, api.OutputKeyDescriptor)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- ValueConversions.fromProto(msg.getKey)
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

object ValueConversions {
  import ConversionUtil._

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
    b.setKey(toProto(obj.key))
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

  def fromProtoSimple(msg: proto.UUID): java.util.UUID = {
    new java.util.UUID(msg.getHigh, msg.getLow)
  }
  def toProto(uuid: java.util.UUID): proto.UUID = {
    proto.UUID.newBuilder().setLow(uuid.getLeastSignificantBits).setHigh(uuid.getMostSignificantBits).build()
  }

  def fromProto(msg: proto.ArrayValue): Either[String, api.ValueArray] = {
    rightSequence(msg.getElementList.map(fromProto).toVector).map(s => api.ValueArray(s.toIndexedSeq))
  }
  def toProto(obj: api.ValueArray): proto.ArrayValue = {
    val b = proto.ArrayValue.newBuilder()
    obj.seq.foreach(v => b.addElement(toProto(v)))
    b.build()
  }

  def fromProto(msg: proto.ObjectValue): Either[String, api.ValueObject] = {
    val fields = msg.getFieldsMap.toVector.map { case (k, v) => fromProto(v).map(ve => (k, ve)) }
    rightSequence(fields).map { all =>
      api.ValueObject(all.toMap)
    }
  }
  def toProto(obj: api.ValueObject): proto.ObjectValue = {
    val b = proto.ObjectValue.newBuilder()
    obj.map.foreach {
      case (k, v) =>
        b.putFields(k, toProto(v))
    }
    b.build()
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

  def fromProto(msg: proto.TextValue): api.ValueText = {
    val mimeOpt = if (msg.hasMimeType) Some(msg.getMimeType.getValue) else None
    api.ValueText(msg.getValue, mimeOpt)
  }

  def toProto(obj: api.ValueText): proto.TextValue = {
    val b = proto.TextValue.newBuilder().setValue(obj.v)
    obj.mimeType.foreach(s => b.setMimeType(toOptionString(s)))
    b.build()
  }

  def fromProto(msg: proto.AnnotatedBytesValue): api.ValueAnnotatedBytes = {
    val mimeOpt = if (msg.hasMimeType) Some(msg.getMimeType.getValue) else None
    val textOpt = if (msg.hasIsText) Some(msg.getIsText.getValue) else None
    api.ValueAnnotatedBytes(msg.getValue.toByteArray, mimeOpt, textOpt)
  }

  def toProto(obj: api.ValueAnnotatedBytes): proto.AnnotatedBytesValue = {
    val b = proto.AnnotatedBytesValue.newBuilder().setValue(ByteString.copyFrom(obj.v))
    obj.mimeType.foreach(v => b.setMimeType(toOptionString(v)))
    obj.isText.foreach(v => b.setIsText(toOptionBool(v)))
    b.build()
  }

  def fromProto(msg: proto.Value): Either[String, api.Value] = {
    import proto.Value.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(api.ValueBool(msg.getBoolValue))
      case ValueTypesCase.FLOAT_VALUE => Right(api.ValueFloat(msg.getFloatValue))
      case ValueTypesCase.DOUBLE_VALUE => Right(api.ValueDouble(msg.getDoubleValue))
      case ValueTypesCase.SINT32_VALUE => Right(api.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(api.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(api.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(api.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.STRING_VALUE => Right(api.ValueString(msg.getStringValue))
      case ValueTypesCase.BYTES_VALUE => Right(api.ValueBytes(msg.getBytesValue.toByteArray))
      case ValueTypesCase.TEXT_VALUE => Right(fromProto(msg.getTextValue))
      case ValueTypesCase.BYTES_ANNOTATED_VALUE => Right(fromProto(msg.getBytesAnnotatedValue))
      case ValueTypesCase.UUID_VALUE => Right(api.ValueUuid(fromProtoSimple(msg.getUuidValue)))
      case ValueTypesCase.PATH_VALUE => Right(api.ValuePath(fromProtoSimple(msg.getPathValue)))
      case ValueTypesCase.ENDPOINT_PATH_VALUE => fromProto(msg.getEndpointPathValue).map(api.ValueEndpointPath)
      case ValueTypesCase.ARRAY_VALUE => fromProto(msg.getArrayValue)
      case ValueTypesCase.OBJECT_VALUE => fromProto(msg.getObjectValue)
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: api.Value): proto.Value = {
    obj match {
      case api.ValueBool(v) => proto.Value.newBuilder().setBoolValue(v).build()
      case api.ValueFloat(v) => proto.Value.newBuilder().setDoubleValue(v).build()
      case api.ValueDouble(v) => proto.Value.newBuilder().setDoubleValue(v).build()
      case api.ValueInt32(v) => proto.Value.newBuilder().setSint32Value(v).build()
      case api.ValueUInt32(v) => proto.Value.newBuilder().setUint32Value(v.toInt).build()
      case api.ValueInt64(v) => proto.Value.newBuilder().setSint64Value(v).build()
      case api.ValueUInt64(v) => proto.Value.newBuilder().setUint64Value(v).build()
      case v: api.ValueString => proto.Value.newBuilder().setStringValue(v.v).build()
      case v: api.ValueBytes => proto.Value.newBuilder().setBytesValue(ByteString.copyFrom(v.v)).build()
      case v: api.ValueText => proto.Value.newBuilder().setTextValue(toProto(v)).build()
      case v: api.ValueAnnotatedBytes => proto.Value.newBuilder().setBytesAnnotatedValue(toProto(v)).build()
      case v: api.ValueUuid => proto.Value.newBuilder().setUuidValue(toProto(v.v)).build()
      case v: api.ValuePath => proto.Value.newBuilder().setPathValue(toProto(v.v)).build()
      case v: api.ValueEndpointPath => proto.Value.newBuilder().setEndpointPathValue(toProto(v.v)).build()
      case v: api.ValueArray => proto.Value.newBuilder().setArrayValue(toProto(v)).build()
      case v: api.ValueObject => proto.Value.newBuilder().setObjectValue(toProto(v)).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }

  def fromProto(msg: proto.IndexableValue): Either[String, api.IndexableValue] = {
    import proto.IndexableValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(api.ValueBool(msg.getBoolValue))
      case ValueTypesCase.FLOAT_VALUE => Right(api.ValueFloat(msg.getFloatValue))
      case ValueTypesCase.DOUBLE_VALUE => Right(api.ValueDouble(msg.getDoubleValue))
      case ValueTypesCase.SINT32_VALUE => Right(api.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(api.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(api.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(api.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.STRING_VALUE => Right(api.ValueString(msg.getStringValue))
      case ValueTypesCase.BYTES_VALUE => Right(api.ValueBytes(msg.getBytesValue.toByteArray))
      case ValueTypesCase.UUID_VALUE => Right(api.ValueUuid(fromProtoSimple(msg.getUuidValue)))
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: api.IndexableValue): proto.IndexableValue = {
    obj match {
      case api.ValueBool(v) => proto.IndexableValue.newBuilder().setBoolValue(v).build()
      case api.ValueFloat(v) => proto.IndexableValue.newBuilder().setDoubleValue(v).build()
      case api.ValueDouble(v) => proto.IndexableValue.newBuilder().setDoubleValue(v).build()
      case api.ValueInt32(v) => proto.IndexableValue.newBuilder().setSint32Value(v).build()
      case api.ValueUInt32(v) => proto.IndexableValue.newBuilder().setUint32Value(v.toInt).build()
      case api.ValueInt64(v) => proto.IndexableValue.newBuilder().setSint64Value(v).build()
      case api.ValueUInt64(v) => proto.IndexableValue.newBuilder().setUint64Value(v).build()
      case v: api.ValueString => proto.IndexableValue.newBuilder().setStringValue(v.v).build()
      case v: api.ValueBytes => proto.IndexableValue.newBuilder().setBytesValue(ByteString.copyFrom(v.v)).build()
      case v: api.ValueUuid => proto.IndexableValue.newBuilder().setUuidValue(toProto(v.v)).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }

  def fromProto(msg: proto.SampleValue): Either[String, api.SampleValue] = {
    import proto.SampleValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(api.ValueBool(msg.getBoolValue))
      case ValueTypesCase.FLOAT_VALUE => Right(api.ValueFloat(msg.getFloatValue))
      case ValueTypesCase.DOUBLE_VALUE => Right(api.ValueDouble(msg.getDoubleValue))
      case ValueTypesCase.SINT32_VALUE => Right(api.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(api.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(api.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(api.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: api.SampleValue): proto.SampleValue = {
    obj match {
      case api.ValueBool(v) => proto.SampleValue.newBuilder().setBoolValue(v).build()
      case api.ValueFloat(v) => proto.SampleValue.newBuilder().setDoubleValue(v).build()
      case api.ValueDouble(v) => proto.SampleValue.newBuilder().setDoubleValue(v).build()
      case api.ValueInt32(v) => proto.SampleValue.newBuilder().setSint32Value(v).build()
      case api.ValueUInt32(v) => proto.SampleValue.newBuilder().setUint32Value(v.toInt).build()
      case api.ValueInt64(v) => proto.SampleValue.newBuilder().setSint64Value(v).build()
      case api.ValueUInt64(v) => proto.SampleValue.newBuilder().setUint64Value(v).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }
}