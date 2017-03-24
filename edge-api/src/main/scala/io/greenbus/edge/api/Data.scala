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
package io.greenbus.edge.api

import java.util
import java.util.UUID

/*sealed trait EndpointId
case class NamedEndpointId(name: Path) extends EndpointId
case class UuidEndpointId(uuid: UUID, display: Path) extends EndpointId*/

case class EndpointId(path: Path)

case class EndpointPath(endpoint: EndpointId, key: Path)

object Path {
  def isPrefixOf(l: Path, r: Path): Boolean = {
    val lIter = l.parts.iterator
    val rIter = r.parts.iterator
    var okay = true
    var continue = true
    while (continue) {
      (lIter.hasNext, rIter.hasNext) match {
        case (true, true) =>
          if (lIter.next() != rIter.next()) {
            okay = false
            continue = false
          }
        case (false, true) => continue = false
        case (true, false) =>
          okay = false; continue = false
        case (false, false) => continue = false
      }
    }
    okay
  }

  def apply(part: String): Path = {
    Path(Seq(part))
  }
}
case class Path(parts: Seq[String])

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
case class ValueString(v: String) extends IndexableValue
case class ValueUuid(v: UUID) extends IndexableValue
case class ValuePath(v: Path) extends Value
case class ValueEndpointPath(v: EndpointPath) extends Value
case class ValueText(v: String, mimeType: Option[String] = None) extends Value

case class ValueArray(seq: IndexedSeq[Value]) extends Value
case class ValueObject(map: Map[String, Value]) extends Value

case class ValueBytes(v: Array[Byte]) extends IndexableValue {

  override def equals(r: Any): Boolean = {
    r match {
      case rv: ValueBytes => util.Arrays.equals(v, rv.v)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    util.Arrays.hashCode(v)
  }
}
case class ValueAnnotatedBytes(v: Array[Byte], mimeType: Option[String] = None, isText: Option[Boolean] = None) extends Value {

  override def equals(r: Any): Boolean = {
    r match {
      case rv: ValueAnnotatedBytes => util.Arrays.equals(v, rv.v) && rv.mimeType == mimeType && rv.isText == isText
      case _ => false
    }
  }

  override def hashCode(): Int = {
    util.Arrays.hashCode(v) * mimeType.hashCode() * isText.hashCode()
  }
}