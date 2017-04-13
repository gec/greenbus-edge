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
package io.greenbus.edge.data.proto.convert

import com.google.protobuf.ByteString
import io.greenbus.edge.data.proto
import io.greenbus.edge.data

import scala.collection.JavaConverters._

object ValueConversions {

  import io.greenbus.edge.util.EitherUtil._

  def fromProto(msg: proto.ListValue): Either[String, data.ValueArray] = {
    rightSequence(msg.getElementList.asScala.map(fromProto).toVector).map(s => data.ValueArray(s.toIndexedSeq))
  }
  def toProto(obj: data.ValueArray): proto.ListValue = {
    val b = proto.ListValue.newBuilder()
    obj.seq.foreach(v => b.addElement(toProto(v)))
    b.build()
  }

  def fromProto(msg: proto.MapValue): Either[String, data.ValueObject] = {
    val fields = msg.getFieldsMap.asScala.toVector.map { case (k, v) => fromProto(v).map(ve => (k, ve)) }
    rightSequence(fields).map { all =>
      data.ValueObject(all.toMap)
    }
  }
  def toProto(obj: data.ValueObject): proto.MapValue = {
    val b = proto.MapValue.newBuilder()
    obj.map.foreach {
      case (k, v) =>
        b.putFields(k, toProto(v))
    }
    b.build()
  }

  def fromProto(msg: proto.Value): Either[String, data.Value] = {
    import proto.Value.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(data.ValueBool(msg.getBoolValue))
      case ValueTypesCase.FLOAT_VALUE => Right(data.ValueFloat(msg.getFloatValue))
      case ValueTypesCase.DOUBLE_VALUE => Right(data.ValueDouble(msg.getDoubleValue))
      case ValueTypesCase.SINT32_VALUE => Right(data.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(data.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(data.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(data.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.STRING_VALUE => Right(data.ValueString(msg.getStringValue))
      case ValueTypesCase.BYTES_VALUE => Right(data.ValueBytes(msg.getBytesValue.toByteArray))
      case ValueTypesCase.LIST_VALUE => fromProto(msg.getListValue)
      case ValueTypesCase.MAP_VALUE => fromProto(msg.getMapValue)
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: data.Value): proto.Value = {
    obj match {
      case data.ValueBool(v) => proto.Value.newBuilder().setBoolValue(v).build()
      case data.ValueFloat(v) => proto.Value.newBuilder().setDoubleValue(v).build()
      case data.ValueDouble(v) => proto.Value.newBuilder().setDoubleValue(v).build()
      case data.ValueInt32(v) => proto.Value.newBuilder().setSint32Value(v).build()
      case data.ValueUInt32(v) => proto.Value.newBuilder().setUint32Value(v.toInt).build()
      case data.ValueInt64(v) => proto.Value.newBuilder().setSint64Value(v).build()
      case data.ValueUInt64(v) => proto.Value.newBuilder().setUint64Value(v).build()
      case data.ValueString(v) => proto.Value.newBuilder().setStringValue(v).build()
      case v: data.ValueArray => proto.Value.newBuilder().setListValue(toProto(v)).build()
      case v: data.ValueObject => proto.Value.newBuilder().setMapValue(toProto(v)).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }

  def fromProto(msg: proto.IndexableValue): Either[String, data.IndexableValue] = {
    import proto.IndexableValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(data.ValueBool(msg.getBoolValue))
      case ValueTypesCase.SINT32_VALUE => Right(data.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(data.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(data.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(data.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.STRING_VALUE => Right(data.ValueString(msg.getStringValue))
      case ValueTypesCase.BYTES_VALUE => Right(data.ValueBytes(msg.getBytesValue.toByteArray))
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: data.IndexableValue): proto.IndexableValue = {
    obj match {
      case data.ValueBool(v) => proto.IndexableValue.newBuilder().setBoolValue(v).build()
      case data.ValueInt32(v) => proto.IndexableValue.newBuilder().setSint32Value(v).build()
      case data.ValueUInt32(v) => proto.IndexableValue.newBuilder().setUint32Value(v.toInt).build()
      case data.ValueInt64(v) => proto.IndexableValue.newBuilder().setSint64Value(v).build()
      case data.ValueUInt64(v) => proto.IndexableValue.newBuilder().setUint64Value(v).build()
      case v: data.ValueString => proto.IndexableValue.newBuilder().setStringValue(v.v).build()
      case v: data.ValueBytes => proto.IndexableValue.newBuilder().setBytesValue(ByteString.copyFrom(v.v)).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }

  def fromProto(msg: proto.SampleValue): Either[String, data.SampleValue] = {
    import proto.SampleValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(data.ValueBool(msg.getBoolValue))
      case ValueTypesCase.FLOAT_VALUE => Right(data.ValueFloat(msg.getFloatValue))
      case ValueTypesCase.DOUBLE_VALUE => Right(data.ValueDouble(msg.getDoubleValue))
      case ValueTypesCase.SINT32_VALUE => Right(data.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(data.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(data.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(data.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: data.SampleValue): proto.SampleValue = {
    obj match {
      case data.ValueBool(v) => proto.SampleValue.newBuilder().setBoolValue(v).build()
      case data.ValueFloat(v) => proto.SampleValue.newBuilder().setDoubleValue(v).build()
      case data.ValueDouble(v) => proto.SampleValue.newBuilder().setDoubleValue(v).build()
      case data.ValueInt32(v) => proto.SampleValue.newBuilder().setSint32Value(v).build()
      case data.ValueUInt32(v) => proto.SampleValue.newBuilder().setUint32Value(v.toInt).build()
      case data.ValueInt64(v) => proto.SampleValue.newBuilder().setSint64Value(v).build()
      case data.ValueUInt64(v) => proto.SampleValue.newBuilder().setUint64Value(v).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }
}
