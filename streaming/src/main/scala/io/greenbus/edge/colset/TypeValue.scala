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
package io.greenbus.edge.colset

import java.util
import java.util.UUID

//trait TypeTrait
trait TypeDesc {
  def instanceOf(v: TypeValue): Boolean
}
trait TypeValue {
  def typeDesc: TypeDesc
  def simpleString(): String
}

trait FinalTypeDesc extends TypeDesc {
  def instanceOf(v: TypeValue): Boolean = v.typeDesc == this
}

case object AnyDesc extends TypeDesc {
  def instanceOf(v: TypeValue): Boolean = true
}
case object NothingDesc extends TypeDesc {
  def instanceOf(v: TypeValue): Boolean = false
}

trait IndexableTypeValue extends TypeValue

trait OrderedTypeValue extends TypeValue {
  def isEqualTo(r: OrderedTypeValue): Boolean
  def isLessThan(r: OrderedTypeValue): Option[Boolean]
}

trait SequencedTypeValue extends OrderedTypeValue {
  def precedes(r: SequencedTypeValue): Boolean
  def next: SequencedTypeValue
}

abstract class TypeValueDecl(desc: TypeDesc) extends TypeValue {
  def typeDesc: TypeDesc = desc
}

case object SymbolDesc extends FinalTypeDesc
case class SymbolVal(v: String) extends TypeValueDecl(SymbolDesc) with OrderedTypeValue {
  def isEqualTo(r: OrderedTypeValue): Boolean = r == this

  def isLessThan(r: OrderedTypeValue): Option[Boolean] = {
    r match {
      case SymbolVal(rv) => Some(v.compareTo(rv) < 0)
      case _ => None
    }
  }

  def simpleString(): String = v
}

case object TextDesc extends FinalTypeDesc
case class TextVal(v: String) extends TypeValueDecl(TextDesc) with OrderedTypeValue {
  def isEqualTo(r: OrderedTypeValue): Boolean = r == this

  def isLessThan(r: OrderedTypeValue): Option[Boolean] = {
    r match {
      case TextVal(rv) => Some(v.compareTo(rv) < 0)
      case _ => None
    }
  }

  def simpleString(): String = v
}

object ArrayCompare {

  def compare(l: Array[Byte], r: Array[Byte]): Int = {
    var continue = true
    var result = l.length - r.length
    var i = 0
    val lowestLen = Math.min(l.length, r.length)
    while (continue && i < lowestLen) {
      val lint: Int = l(i) & 0xff
      val rint: Int = r(i) & 0xff
      if (lint != rint) {
        result = lint - rint
        continue = false
      }
      i += 1
    }
    result
  }

}

case object BytesDesc extends FinalTypeDesc
case class BytesVal(v: Array[Byte]) extends TypeValueDecl(BytesDesc) with OrderedTypeValue {
  def isEqualTo(r: OrderedTypeValue): Boolean = {
    r match {
      case BytesVal(rv) => util.Arrays.equals(v, rv)
      case _ => false
    }
  }

  def isLessThan(r: OrderedTypeValue): Option[Boolean] = {
    r match {
      case BytesVal(rv) => Some(ArrayCompare.compare(v, rv) < 0)
      case _ => None
    }
  }

  def simpleString(): String = s"Bytes[${v.length}]"

  override def equals(r: Any): Boolean = {
    r match {
      case rv: BytesVal => util.Arrays.equals(v, rv.v)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    util.Arrays.hashCode(v)
  }
}

case object UuidDesc extends FinalTypeDesc
case class UuidVal(v: UUID) extends TypeValueDecl(UuidDesc) with OrderedTypeValue {
  def isEqualTo(r: OrderedTypeValue): Boolean = r == this

  def isLessThan(r: OrderedTypeValue): Option[Boolean] = {
    r match {
      case UuidVal(rv) => Some(v.compareTo(rv) < 0)
      case _ => None
    }
  }

  def simpleString(): String = v.toString
}

case object BoolDesc extends FinalTypeDesc
case class BoolVal(v: Boolean) extends TypeValueDecl(BoolDesc) with OrderedTypeValue {
  def isEqualTo(r: OrderedTypeValue): Boolean = r == this

  def isLessThan(r: OrderedTypeValue): Option[Boolean] = {
    r match {
      case BoolVal(rv) => Some(!v && rv)
      case _ => None
    }
  }

  def simpleString(): String = v.toString
}

case object DoubleDesc extends FinalTypeDesc
case class DoubleVal(v: Double) extends TypeValueDecl(DoubleDesc) with OrderedTypeValue {
  def isEqualTo(r: OrderedTypeValue): Boolean = r == this

  def isLessThan(r: OrderedTypeValue): Option[Boolean] = {
    r match {
      case DoubleVal(rv) => Some(v < rv)
      case _ => None
    }
  }

  def simpleString(): String = v.toString
}

case object Int64Desc extends FinalTypeDesc
case class Int64Val(v: Long) extends TypeValueDecl(Int64Desc) with SequencedTypeValue {
  def precedes(r: SequencedTypeValue): Boolean = {
    r match {
      case Int64Val(rv) => v == (rv - 1)
      case _ => false
    }
  }
  def next: SequencedTypeValue = Int64Val(v + 1)
  def isEqualTo(r: OrderedTypeValue): Boolean = r == this
  def isLessThan(r: OrderedTypeValue): Option[Boolean] = {
    r match {
      case Int64Val(rv) => Some(v < rv)
      case _ => None
    }
  }

  def simpleString(): String = v.toString
}

case class TupleDesc(elementsDesc: Seq[TypeDesc]) extends FinalTypeDesc
case class TupleVal(elements: Seq[TypeValue]) extends TypeValueDecl(TupleDesc(elements.map(_.typeDesc))) {

  def simpleString(): String = elements.map(_.simpleString()).mkString("(", ", ", ")")
}

case class OptionDesc(elementDesc: TypeDesc) extends FinalTypeDesc
case class OptionVal(element: Option[TypeValue]) extends TypeValueDecl(OptionDesc(element.map(_.typeDesc).getOrElse(NothingDesc))) {

  def simpleString(): String = element.map(v => s"Some(${v.simpleString()})").getOrElse("None")
}

object TypeValueConversions {

  def toTypeValue(session: PeerSessionId): TypeValue = {
    TupleVal(Seq(UuidVal(session.persistenceId), Int64Val(session.instanceId)))
  }

}