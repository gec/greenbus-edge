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
package io.greenbus.edge.data

import java.util
import java.util.UUID

sealed trait Value

case class TaggedValue(tag: String, value: BasicValue) extends Value

sealed trait BasicValue extends Value

sealed trait IndexableValue extends BasicValue

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
sealed trait IntegerValue extends NumericConvertible {
  def toInt: Int
  def toLong: Long
}

object FloatingPointValue {
  def unapply(v: FloatingPointValue): Option[Double] = {
    Some(v.toDouble)
  }
}
sealed trait FloatingPointValue extends NumericConvertible {
  def toFloat: Float
  def toDouble: Double
}

case class ValueFloat(value: Float) extends FloatingPointValue with SampleValue {
  def toFloat: Float = value.toFloat
  def toDouble: Double = value.toDouble
  def toLong: Long = value.toLong
  def toBoolean: Boolean = value != 0.0
}
case class ValueDouble(value: Double) extends FloatingPointValue with SampleValue {
  def toFloat: Float = value.toFloat
  def toDouble: Double = value
  def toLong: Long = value.toLong
  def toBoolean: Boolean = value != 0.0
}
case class ValueInt32(value: Int) extends IntegerValue with SampleValue {
  def toDouble: Double = value.toDouble
  def toInt: Int = value.toInt
  def toLong: Long = value.toLong
  def toBoolean: Boolean = value != 0
}
case class ValueUInt32(value: Long) extends IntegerValue with SampleValue {
  def toDouble: Double = value.toDouble
  def toInt: Int = value.toInt
  def toLong: Long = value.toLong
  def toBoolean: Boolean = value != 0
}
case class ValueInt64(value: Long) extends IntegerValue with SampleValue {
  def toDouble: Double = value.toDouble
  def toInt: Int = value.toInt
  def toLong: Long = value.toLong
  def toBoolean: Boolean = value != 0
}
case class ValueUInt64(value: Long) extends IntegerValue with SampleValue {
  def toDouble: Double = value.toDouble
  def toInt: Int = value.toInt
  def toLong: Long = value.toLong
  def toBoolean: Boolean = value != 0
}
case class ValueBool(value: Boolean) extends NumericConvertible with SampleValue {
  def toDouble: Double = if (value) 1.0 else 0.0
  def toLong: Long = if (value) 1 else 0
  def toBoolean: Boolean = value
}
case class ValueByte(value: Byte) extends NumericConvertible with SampleValue {
  def toDouble: Double = value.toDouble
  def toLong: Long = value.toLong
  def toBoolean: Boolean = value != 0
}
case class ValueString(value: String) extends IndexableValue

case class ValueList(value: Seq[Value]) extends BasicValue
case class ValueMap(value: Map[Value, Value]) extends BasicValue

case class ValueBytes(value: Array[Byte]) extends IndexableValue {

  override def equals(r: Any): Boolean = {
    r match {
      case rv: ValueBytes => util.Arrays.equals(value, rv.value)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    util.Arrays.hashCode(value)
  }
}

case object ValueNone extends BasicValue