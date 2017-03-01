package io.greenbus.edge.colset

import java.util.UUID

trait TypeDesc
trait TypeValue {
  def typeDesc: TypeDesc
}
/*
trait OrderedTypeValue[Self] extends TypeValue {
  def isEqualTo(r: Self): Boolean
  def isLessThan(r: Self): Boolean
}

trait SequencedTypeValue[Self] extends OrderedTypeValue[Self] {
  def precedes(r: Self): Boolean
  def next: Self
}*/

trait IndexableTypeValue

trait OrderedTypeValue extends TypeValue {
  def isEqualTo(r: OrderedTypeValue): Boolean
  def isLessThan(r: OrderedTypeValue): Boolean
}

trait SequencedTypeValue extends OrderedTypeValue {
  def precedes(r: SequencedTypeValue): Boolean
  def next: SequencedTypeValue
}

abstract class TypeValueDecl(desc: TypeDesc) extends TypeValue {
  def typeDesc: TypeDesc = desc
}

case object SymbolDesc extends TypeDesc
case class SymbolVal(v: String) extends TypeValueDecl(SymbolDesc)

case object UuidDesc extends TypeDesc
case class UuidVal(v: UUID) extends TypeValueDecl(UuidDesc)

case object UInt64Desc extends TypeDesc
case class UInt64Val(v: Long) extends TypeValueDecl(UInt64Desc) with SequencedTypeValue {
  def precedes(r: SequencedTypeValue): Boolean = {
    r match {
      case UInt64Val(rv) => v == (rv - 1)
      case _ => false
    }
  }
  def next: SequencedTypeValue = UInt64Val(v + 1)
  def isEqualTo(r: OrderedTypeValue): Boolean = r == this
  def isLessThan(r: OrderedTypeValue): Boolean = {
    r match {
      case UInt64Val(rv) => v < rv
      case _ => false
    }
  }
}

case class TupleDesc(elementDesc: Seq[TypeDesc]) extends TypeDesc
case class TupleVal(elements: Seq[TypeValue]) extends TypeValueDecl(TupleDesc(elements.map(_.typeDesc)))

object TypeValueConversions {

  def toTypeValue(session: PeerSessionId): TypeValue = {
    TupleVal(Seq(UuidVal(session.persistenceId), UInt64Val(session.instanceId)))
  }

}