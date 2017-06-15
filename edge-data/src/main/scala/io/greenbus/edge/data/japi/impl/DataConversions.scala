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
package io.greenbus.edge.data.japi.impl

import scala.collection.JavaConverters._

object DataConversions {
  import io.greenbus.edge.data
  import io.greenbus.edge.data.japi

  def convertValueToJava(obj: data.Value): japi.Value = {
    obj match {
      case v: data.TaggedValue => convertTaggedValueToJava(v)
      case v: data.BasicValue => convertBasicValueToJava(v)
    }
  }
  def convertValueToScala(obj: japi.Value): data.Value = {
    obj match {
      case v: japi.TaggedValue => convertTaggedValueToScala(v)
      case v: japi.BasicValue => convertBasicValueToScala(v)
    }
  }

  def convertBasicValueToJava(obj: data.BasicValue): japi.BasicValue = {
    obj match {
      case v: data.ValueBool => convertValueBoolToJava(v)
      case v: data.ValueByte => convertValueByteToJava(v)
      case v: data.ValueFloat => convertValueFloatToJava(v)
      case v: data.ValueDouble => convertValueDoubleToJava(v)
      case v: data.ValueInt32 => convertValueInt32ToJava(v)
      case v: data.ValueUInt32 => convertValueUInt32ToJava(v)
      case v: data.ValueInt64 => convertValueInt64ToJava(v)
      case v: data.ValueUInt64 => convertValueUInt64ToJava(v)
      case v: data.ValueBytes => convertValueBytesToJava(v)
      case v: data.ValueString => convertValueStringToJava(v)
      case v: data.ValueList => convertValueListToJava(v)
      case v: data.ValueMap => convertValueMapToJava(v)
      case data.ValueNone => new japi.ValueNone()
    }
  }
  def convertBasicValueToScala(obj: japi.BasicValue): data.BasicValue = {
    obj match {
      case v: japi.ValueBool => convertValueBoolToScala(v)
      case v: japi.ValueByte => convertValueByteToScala(v)
      case v: japi.ValueFloat => convertValueFloatToScala(v)
      case v: japi.ValueDouble => convertValueDoubleToScala(v)
      case v: japi.ValueInt32 => convertValueInt32ToScala(v)
      case v: japi.ValueUInt32 => convertValueUInt32ToScala(v)
      case v: japi.ValueInt64 => convertValueInt64ToScala(v)
      case v: japi.ValueUInt64 => convertValueUInt64ToScala(v)
      case v: japi.ValueBytes => convertValueBytesToScala(v)
      case v: japi.ValueString => convertValueStringToScala(v)
      case v: japi.ValueList => convertValueListToScala(v)
      case v: japi.ValueMap => convertValueMapToScala(v)
      case v: japi.ValueNone => data.ValueNone
    }
  }

  def convertTaggedValueToJava(obj: data.TaggedValue): japi.TaggedValue = {
    new japi.TaggedValue(obj.tag, convertBasicValueToJava(obj.value))
  }
  def convertTaggedValueToScala(obj: japi.TaggedValue): data.TaggedValue = {
    data.TaggedValue(obj.getTag, convertBasicValueToScala(obj.getValue))
  }

  def convertValueBoolToJava(obj: data.ValueBool): japi.ValueBool = {
    new japi.ValueBool(obj.value)
  }
  def convertValueBoolToScala(obj: japi.ValueBool): data.ValueBool = {
    data.ValueBool(obj.getValue)
  }

  def convertValueByteToJava(obj: data.ValueByte): japi.ValueByte = {
    new japi.ValueByte(obj.value)
  }
  def convertValueByteToScala(obj: japi.ValueByte): data.ValueByte = {
    data.ValueByte(obj.getValue)
  }

  def convertValueFloatToJava(obj: data.ValueFloat): japi.ValueFloat = {
    new japi.ValueFloat(obj.value)
  }
  def convertValueFloatToScala(obj: japi.ValueFloat): data.ValueFloat = {
    data.ValueFloat(obj.getValue)
  }

  def convertValueDoubleToJava(obj: data.ValueDouble): japi.ValueDouble = {
    new japi.ValueDouble(obj.value)
  }
  def convertValueDoubleToScala(obj: japi.ValueDouble): data.ValueDouble = {
    data.ValueDouble(obj.getValue)
  }

  def convertValueInt32ToJava(obj: data.ValueInt32): japi.ValueInt32 = {
    new japi.ValueInt32(obj.value)
  }
  def convertValueInt32ToScala(obj: japi.ValueInt32): data.ValueInt32 = {
    data.ValueInt32(obj.getValue)
  }

  def convertValueUInt32ToJava(obj: data.ValueUInt32): japi.ValueUInt32 = {
    new japi.ValueUInt32(obj.value.toInt)
  }
  def convertValueUInt32ToScala(obj: japi.ValueUInt32): data.ValueUInt32 = {
    data.ValueUInt32(obj.getValue)
  }

  def convertValueInt64ToJava(obj: data.ValueInt64): japi.ValueInt64 = {
    new japi.ValueInt64(obj.value)
  }
  def convertValueInt64ToScala(obj: japi.ValueInt64): data.ValueInt64 = {
    data.ValueInt64(obj.getValue)
  }

  def convertValueUInt64ToJava(obj: data.ValueUInt64): japi.ValueUInt64 = {
    new japi.ValueUInt64(obj.value)
  }
  def convertValueUInt64ToScala(obj: japi.ValueUInt64): data.ValueUInt64 = {
    data.ValueUInt64(obj.getValue)
  }

  def convertValueStringToJava(obj: data.ValueString): japi.ValueString = {
    new japi.ValueString(obj.value)
  }
  def convertValueStringToScala(obj: japi.ValueString): data.ValueString = {
    data.ValueString(obj.getValue)
  }

  def convertValueBytesToJava(obj: data.ValueBytes): japi.ValueBytes = {
    new japi.ValueBytes(obj.value)
  }
  def convertValueBytesToScala(obj: japi.ValueBytes): data.ValueBytes = {
    data.ValueBytes(obj.getValue)
  }

  def convertValueListToJava(obj: data.ValueList): japi.ValueList = {
    val converted = obj.value.map(convertValueToJava).asJava
    new japi.ValueList(converted)
  }
  def convertValueListToScala(obj: japi.ValueList): data.ValueList = {
    val converted = obj.getValue.asScala.map(convertValueToScala)
    data.ValueList(converted)
  }

  def convertValueMapToJava(obj: data.ValueMap): japi.ValueMap = {
    val converted = obj.value.map { case (k, v) => (convertValueToJava(k), convertValueToJava(v)) }.asJava
    new japi.ValueMap(converted)
  }
  def convertValueMapToScala(obj: japi.ValueMap): data.ValueMap = {
    val converted = obj.getValue.asScala.map { case (k, v) => (convertValueToScala(k), convertValueToScala(v)) }.toMap
    data.ValueMap(converted)
  }
}
