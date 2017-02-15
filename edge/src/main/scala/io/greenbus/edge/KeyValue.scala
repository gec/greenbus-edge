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
package io.greenbus.edge

import java.util.UUID

sealed trait Key
case class KeyString(v: String) extends Key
case class KeyUuid(v: UUID) extends Key

// TODO: type/type param system?
sealed trait Value

sealed trait IndexableValue extends Value

sealed trait NumericConvertible extends IndexableValue {
  def toDouble: Double
  def toLong: Long
  def toBoolean: Boolean
}

sealed trait SampleValue extends NumericConvertible

object IntegerValue {
  def unapply(v: IntegerValue): Option[Long] = {
    Some(v.toLong)
  }
}
sealed trait IntegerValue extends NumericConvertible

object FloatingPointValue {
  def unapply(v: FloatingPointValue): Option[Double] = {
    Some(v.toDouble)
  }
}
sealed trait FloatingPointValue extends NumericConvertible

case class ValueFloat(v: Float) extends FloatingPointValue with SampleValue {
  def toDouble: Double = v.toDouble
  def toLong: Long = v.toLong
  def toBoolean: Boolean = v != 0.0
}
case class ValueDouble(v: Double) extends FloatingPointValue with SampleValue {
  def toDouble: Double = v
  def toLong: Long = v.toLong
  def toBoolean: Boolean = v != 0.0
}
case class ValueInt32(v: Int) extends IntegerValue with SampleValue {
  def toDouble: Double = v.toDouble
  def toLong: Long = v.toLong
  def toBoolean: Boolean = v != 0
}
case class ValueUInt32(v: Long) extends IntegerValue with SampleValue {
  def toDouble: Double = v.toDouble
  def toLong: Long = v.toLong
  def toBoolean: Boolean = v != 0
}
case class ValueInt64(v: Long) extends IntegerValue with SampleValue {
  def toDouble: Double = v.toDouble
  def toLong: Long = v.toLong
  def toBoolean: Boolean = v != 0
}
case class ValueUInt64(v: Long) extends IntegerValue with SampleValue {
  def toDouble: Double = v.toDouble
  def toLong: Long = v.toLong
  def toBoolean: Boolean = v != 0
}
case class ValueBool(v: Boolean) extends NumericConvertible with SampleValue {
  def toDouble: Double = if (v) 1.0 else 0.0
  def toLong: Long = if (v) 1 else 0
  def toBoolean: Boolean = v
}
case class ValueUuid(v: UUID) extends IndexableValue
case class ValueString(v: String, mimeType: Option[String] = None) extends IndexableValue
case class ValueBytes(v: Array[Byte], mimeType: Option[String] = None, isText: Option[Boolean] = None) extends IndexableValue

case class ValueArray(seq: IndexedSeq[Value]) extends Value
case class ValueObject(map: Map[String, Value]) extends Value

/*
data types:

bool/ints/floats
string
uuid
path (string seq)
byte array
annotated byte array (mime-type, etc)
object array
object map

annotated string (i.e. json)??
enum types?

keys are typed? optionally? root value object contains key type? type params (map[string->init])?
all objects contain (optional?) typing? user-defined schema?

a database view of the system could use the key/type combination as the full id of the key
 */ 