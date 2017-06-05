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

import java.io.{ InputStream, InputStreamReader }

import com.google.gson.stream.{ JsonReader, JsonToken }
import io.greenbus.edge.data._

import scala.collection.mutable.ArrayBuffer

object EdgeJsonReader {

  def read(is: InputStream): Option[Value] = {
    val r = new JsonReader(new InputStreamReader(is, "UTF-8"))
    Some(readValue(r))
  }

  private def readValue(r: JsonReader): Value = {
    r.peek() match {
      case JsonToken.STRING => ValueString(r.nextString())
      case JsonToken.BOOLEAN => ValueBool(r.nextBoolean())
      case JsonToken.NUMBER => parseNumeric(r.nextString())
      case JsonToken.BEGIN_ARRAY =>
        r.beginArray()
        readArray(r)
      case JsonToken.BEGIN_OBJECT =>
        r.beginObject()
        readMap(r)
      case JsonToken.NULL =>
        r.nextNull(); ValueNone
      case other => throw new IllegalArgumentException(s"Could not handle json token: $other at ${r.getPath}")
    }
  }

  private def readMap(r: JsonReader): Value = {
    val buffer = ArrayBuffer.empty[(Value, Value)]
    var tagOpt = Option.empty[String]
    var continue = true
    while (continue) {
      r.peek() match {
        case JsonToken.NAME => {
          val name = r.nextName()
          if (name == "@tag") {
            tagOpt = Some(r.nextString())
          } else {
            val value = readValue(r)
            buffer += ((ValueString(name), value))
          }
        }
        case JsonToken.END_OBJECT =>
          r.endObject()
          continue = false
        case other =>
          throw new IllegalArgumentException(s"Expecting name or object end, could not handle json token: $other at ${r.getPath}")
      }
    }

    tagOpt match {
      case None => ValueMap(buffer.toMap)
      case Some(tag) =>
        TaggedValue(tag, ValueMap(buffer.toMap))
    }

  }

  private def readArray(r: JsonReader) = {
    val buffer = ArrayBuffer.empty[Value]

    var continue = true
    while (continue) {
      r.peek() match {
        case JsonToken.END_ARRAY =>
          r.endArray()
          continue = false
        case _ => buffer += readValue(r)
      }
    }

    ValueList(buffer)
  }

  private def parseNumeric(s: String): Value = {
    try {
      ValueInt64(java.lang.Long.parseLong(s))
    } catch {
      case ex: NumberFormatException =>
        try {
          ValueDouble(java.lang.Double.parseDouble(s))
        } catch {
          case ex: NumberFormatException =>
            ValueString(s)
        }
    }
  }
}
