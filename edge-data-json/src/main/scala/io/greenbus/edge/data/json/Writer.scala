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
package io.greenbus.edge.data.json

import java.io.{ OutputStream, PrintWriter }
import java.util.Base64

import com.google.gson.stream.JsonWriter
import io.greenbus.edge.data._

object Writer {

  def write(value: Value, os: OutputStream): Unit = {
    val pw = new PrintWriter(os)
    val w = new JsonWriter(pw)

    writeValue(value, w)

    pw.flush()
  }

  private def writeValue(value: Value, w: JsonWriter): Unit = {
    value match {
      case TaggedValue(tag, basic) => writeValue(basic, w)
      case v: ValueList => {
        w.beginArray()
        v.value.foreach(writeValue(_, w))
        w.endArray()
      }
      case v: ValueMap => {
        w.beginObject()
        v.value.foreach {
          case (key, subValue) =>
            key match {
              case keyV: ValueString => w.name(keyV.value)
              case other => w.name(other.toString)
            }
            writeValue(subValue, w)
        }
        w.endObject()
      }
      case v: ValueString => w.value(v.value)
      case v: ValueByte => w.value(v.value.toLong)
      case v: ValueBool => w.value(v.value)
      case v: IntegerValue => w.value(v.toLong)
      case v: FloatingPointValue => w.value(v.toDouble)
      case v: ValueBytes => Base64.getEncoder.encodeToString(v.value)
      case ValueNone => w.nullValue()
    }
  }
}
