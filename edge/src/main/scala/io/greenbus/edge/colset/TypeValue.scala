package io.greenbus.edge.colset

import java.util.UUID


//trait TypeTrait
trait TypeDesc {
  def instanceOf(v: TypeValue): Boolean
}
trait TypeValue {
  def typeDesc: TypeDesc
}

/*object DerivedTypeDesc {
  def hasSuperTypeDesc(desc: TypeDesc)
}
abstract class DerivedTypeDesc(val superTypes: Set[TypeDesc])*/

//abstract class TraitCarryingTypeDesc(val traits: Set[TypeDesc]) extends TypeDesc

trait FinalTypeDesc extends TypeDesc {
  def instanceOf(v: TypeValue): Boolean = v.typeDesc == this
}

case object AnyDesc extends TypeDesc {
  def instanceOf(v: TypeValue): Boolean = true
}
case object NothingDesc extends TypeDesc {
  def instanceOf(v: TypeValue): Boolean = false
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

/*case object IndexableTypeDesc extends TypeDesc {
  def instanceOf(v: TypeValue): Boolean = {
    v.typeDesc match {
      case dtd: DerivedTypeDesc => {

      }
      case _ => false
    }
  }
}*/
trait IndexableTypeValue extends TypeValue

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

case object SymbolDesc extends FinalTypeDesc
case class SymbolVal(v: String) extends TypeValueDecl(SymbolDesc)

case object UuidDesc extends FinalTypeDesc
case class UuidVal(v: UUID) extends TypeValueDecl(UuidDesc)

case object UInt64Desc extends FinalTypeDesc
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

case class TupleDesc(elementsDesc: Seq[TypeDesc]) extends FinalTypeDesc
case class TupleVal(elements: Seq[TypeValue]) extends TypeValueDecl(TupleDesc(elements.map(_.typeDesc)))

case class OptionDesc(elementDesc: TypeDesc) extends FinalTypeDesc
case class OptionVal(element: Option[TypeValue]) extends TypeValueDecl(OptionDesc(element.map(_.typeDesc).getOrElse(NothingDesc)))

object TypeValueConversions {

  def toTypeValue(session: PeerSessionId): TypeValue = {
    TupleVal(Seq(UuidVal(session.persistenceId), UInt64Val(session.instanceId)))
  }

}