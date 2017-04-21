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
package io.greenbus.edge.data.codegen

import java.io.PrintWriter

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.data.schema._

object Gen extends LazyLogging {

  sealed trait FieldTypeDef
  case class SimpleTypeDef(typ: ValueType) extends FieldTypeDef
  case class ParamTypeDef(typ: ValueType) extends FieldTypeDef
  case class TagTypeDef(tag: String) extends FieldTypeDef

  case class FieldDef(name: String, typ: FieldTypeDef)

  sealed trait ObjDef
  case class StructDef(fields: Seq[FieldDef]) extends ObjDef
  case class WrapperDef(field: FieldDef) extends ObjDef
  case class UnionDef(tags: Seq[String]) extends ObjDef
  case class EnumTypeDef(enum: TEnum) extends ObjDef

  def objDefForExtType(typ: TExt): ObjDef = {

    def singleParam(typ: ValueType): ObjDef = {
      WrapperDef(FieldDef("value", ParamTypeDef(typ)))
    }

    typ.reprType match {
      case struct: TStruct => {
        val fields = struct.fields.map { fd =>
          val ftd = fd.typ match {
            case t: TExt => TagTypeDef(t.tag)
            case t: TList => ParamTypeDef(t)
            case t: TMap => ParamTypeDef(t)
            case t: TUnion => ParamTypeDef(t)
            case t: TOption => ParamTypeDef(t)
            case t: TEither => ParamTypeDef(t)
            case t => SimpleTypeDef(t)
          }

          FieldDef(fd.name, ftd)
        }
        StructDef(fields)
      }
      case list: TList => singleParam(list)
      case map: TMap => singleParam(map)
      case union: TUnion => {
        val tags = union.unionTypes.map {
          case t: TExt => t.tag
          case other => throw new IllegalArgumentException(s"Union type must be extensible types: $other")
        }
        UnionDef(tags.toSeq)
      }
      case either: TEither => singleParam(either)
      case option: TOption => singleParam(option)
      case enum: TEnum => EnumTypeDef(enum)
      case basic => WrapperDef(FieldDef("value", SimpleTypeDef(basic)))
    }
  }

  def collectObjDefs(typ: VTValueElem, seen: Map[String, ObjDef]): Map[String, ObjDef] = {
    typ match {
      case ext: TExt => {
        val obj = objDefForExtType(ext)
        Map(ext.tag -> obj) ++ collectObjDefs(ext.reprType, seen)
      }
      case struct: TStruct =>
        struct.fields.foldLeft(seen) { (accum, fd) => collectObjDefs(fd.typ, accum) }
      case list: TList =>
        collectObjDefs(list.paramType, seen)
      case map: TMap =>
        collectObjDefs(map.keyType, seen) ++ collectObjDefs(map.valueType, seen)
      case union: TUnion =>
        union.unionTypes.foldLeft(seen) { (accum, t) => collectObjDefs(t, accum) }
      case either: TEither =>
        collectObjDefs(either.leftType, seen) ++ collectObjDefs(either.rightType, seen)
      case option: TOption =>
        collectObjDefs(option.paramType, seen)
      case basic => seen
    }
  }

  def superTypeMap(objs: Map[String, ObjDef]): Map[String, String] = {
    // TODO: handle multiple?
    val result = Vector.newBuilder[(String, String)]
    objs.foreach {
      case (tag, objDef) => {
        objDef match {
          case UnionDef(subs) => subs.foreach(sub => result += (sub -> tag))
          case _ =>
        }
      }
      case _ =>
    }
    result.result().toMap
  }

  def output(pkg: String, objs: Map[String, ObjDef], pw: PrintWriter): Unit = {

    val superMap = superTypeMap(objs)

    pw.println(s"package $pkg")
    pw.println()
    pw.println("import io.greenbus.edge.data.mapping._")
    pw.println("import io.greenbus.edge.data._")
    pw.println()

    objs.foreach {
      case (tag, obj) =>
        obj match {
          case d: StructDef => writeStatic(tag, d, superMap.get(tag), pw)
          case d: WrapperDef => writeWrapperStatic(tag, d, superMap.get(tag), pw)
          case d: UnionDef => writeUnionStatic(tag, d, pw)
          case d: EnumTypeDef => writeEnumStatic(tag, d, pw)
        }
    }

    pw.println()
  }

  val utilKlass = "MappingLibrary"

  def readFuncForTypeParam(typ: ValueType): String = {
    typ match {
      //case t: VTTuple =>
      case t: TExt => s"${t.tag}.read"
      case t => readFuncForSimpleTyp(t)
    }
  }

  def signatureFor(ftd: FieldTypeDef): String = {
    ftd match {
      case td: SimpleTypeDef => fieldSignatureFor(td.typ)
      case td: ParamTypeDef => fieldSignatureFor(td.typ)
      case td: TagTypeDef => td.tag
    }
  }

  def fieldSignatureFor(typ: ValueType): String = {
    typ match {
      case TBool => "Boolean"
      case TInt32 => "Int"
      case TUInt32 => "Int"
      case TInt64 => "Long"
      case TUInt64 => "Long"
      case TFloat => "Float"
      case TDouble => "Double"
      case TString => "String"
      case t: TList => s"Seq[${fieldSignatureFor(t.paramType)}]"
      case t: TOption => s"Option[${fieldSignatureFor(t.paramType)}]"
      //case t: TUnion => "Any"
      case t: TExt => t.tag
      case t => throw new IllegalArgumentException(s"Type signature unhandled: $t")
    }
  }

  def readFuncForSimpleTyp(typ: ValueType): String = {
    typ match {
      case TBool => s"$utilKlass.readBool"
      case TInt32 => s"$utilKlass.readInt"
      case TUInt32 => s"$utilKlass.readInt"
      case TInt64 => s"$utilKlass.readLong"
      case TUInt64 => s"$utilKlass.readLong"
      case TFloat => s"$utilKlass.readFloat"
      case TDouble => s"$utilKlass.readDouble"
      case TString => s"$utilKlass.readString"
      case t => throw new IllegalArgumentException(s"Simple type unhandled: $t")
    }
  }

  def writeCallFor(ftd: FieldTypeDef, paramDeref: String): String = {
    ftd match {
      case SimpleTypeDef(t) => writeFuncFor(t) + s"($paramDeref)"
      case ParamTypeDef(t) => {
        t match {
          case list: TList => {
            s"$utilKlass.writeList($paramDeref, ${writeFuncFor(list.paramType)})"
          }
          case opt: TOption => {
            s"$paramDeref.map(p => " + writeFuncFor(opt.paramType) + s"(p)" + ").getOrElse(ValueNone)"
          }
          case _ => throw new IllegalArgumentException(s"Unhandled parameterized type def")
        }
      }
      case TagTypeDef(tag) => s"$tag.write($paramDeref)"
    }
  }

  def writeFuncFor(ftd: FieldTypeDef): String = {
    ftd match {
      case SimpleTypeDef(t) => writeFuncFor(t)
      case ParamTypeDef(t) => writeFuncFor(t)
      case TagTypeDef(tag) => s"$tag.write"
    }
  }

  def writeFuncFor(typ: ValueType): String = {
    typ match {
      case t: TExt => s"${t.tag}.write"
      case t =>
        typ match {
          case TBool => "ValueBool"
          case TInt32 => "ValueInt32"
          case TUInt32 => "ValueUInt32"
          case TInt64 => "ValueInt64"
          case TUInt64 => "ValueUInt64"
          case TFloat => "ValueFloat"
          case TDouble => "ValueDouble"
          case TString => "ValueString"
          case _ => throw new IllegalArgumentException(s"Type unhandled: $typ")
        }
    }
  }

  def inputSignatureFor(typ: ValueType, containerTag: String): String = {
    typ match {
      case t: TExt => t.tag
      case t: TList => "ValueList"
      case t: TMap => "ValueMap"
      // case t: TUnion => containerTag
      case TBool => "ValueBool"
      case TInt32 => "ValueInt32"
      case TUInt32 => "ValueUInt32"
      case TInt64 => "ValueInt64"
      case TUInt64 => "ValueUInt64"
      case TFloat => "ValueFloat"
      case TDouble => "ValueDouble"
      case TString => "ValueString"
      case _ => throw new IllegalArgumentException(s"Type unhandled: $typ")
    }
  }

  def tab(n: Int): String = Range(0, n).map(_ => "  ").mkString("")

  def writeEnumStatic(name: String, enumDef: EnumTypeDef, pw: PrintWriter): Unit = {
    logger.debug(s"Writing $name")

    pw.println(s"object $name {")
    pw.println()

    val itemDefs = enumDef.enum.enumDefs

    itemDefs.foreach { ed =>
      pw.println(tab(1) + s"""case object ${ed.label} extends $name("${ed.label}", ${ed.value})""")
    }
    pw.println()

    pw.println(tab(1) + s"def read(element: Value, ctx: ReaderContext): Either[String, $name] = {")
    pw.println(tab(2) + s"element match {")
    pw.println(tab(3) + s"case data: IntegerValue => readInteger(data, ctx)")
    pw.println(tab(3) + s"case data: ValueString => readString(data, ctx)")
    pw.println(tab(3) + s"case tagged: TaggedValue =>")
    pw.println(tab(4) + s"tagged.value match {")
    pw.println(tab(5) + s"case data: IntegerValue => readInteger(data, ctx)")
    pw.println(tab(5) + s"case data: ValueString => readString(data, ctx)")
    pw.println(tab(5) + s"""case other => Left("Type $name did not recognize value type " + other)""")
    pw.println(tab(4) + "}")
    pw.println(tab(3) + s"""case other => Left("Type $name did not recognize value type " + other)""")
    pw.println(tab(2) + "}")
    pw.println(tab(1) + "}")

    pw.println(tab(1) + s"def readInteger(element: IntegerValue, ctx: ReaderContext): Either[String, $name] = {")
    pw.println(tab(2) + s"element.toInt match {")
    itemDefs.foreach(ed => pw.println(tab(3) + s"""case ${ed.value} => Right(${ed.label})"""))
    pw.println(tab(3) + s"""case other => Left("Enum $name did not recognize integer value " + other)""")
    pw.println(tab(2) + "}")
    pw.println(tab(1) + "}")

    pw.println(tab(1) + s"def readString(element: ValueString, ctx: ReaderContext): Either[String, $name] = {")
    pw.println(tab(2) + s"element.value match {")
    itemDefs.foreach(ed => pw.println(tab(3) + s"""case "${ed.label}" => Right(${ed.label})"""))
    pw.println(tab(3) + s"""case other => Left("Enum $name did not recognize string value " + other)""")
    pw.println(tab(2) + "}")
    pw.println(tab(1) + "}")

    pw.println(tab(1) + s"def write(obj: $name): TaggedValue = {")
    pw.println(tab(2) + s"""TaggedValue("$name", ValueUInt32(obj.value))""")
    pw.println(tab(1) + "}")

    pw.println("}")

    pw.println(s"""sealed abstract class $name(val name: String, val value: Int)""")
    pw.println()
  }

  def writeUnionStatic(name: String, unionDef: UnionDef, pw: PrintWriter): Unit = {
    logger.debug(s"Writing $name")

    pw.println(s"object $name {")
    pw.println()

    pw.println(tab(1) + s"def read(element: Value, ctx: ReaderContext): Either[String, $name] = {")
    pw.println(tab(2) + s"element match {")
    pw.println(tab(3) + s"""case t: TaggedValue => """)
    pw.println(tab(4) + s"""t.tag match {""")
    unionDef.tags.foreach(tag => pw.println(tab(5) + s"""case "$tag" => $tag.read(element, ctx)"""))
    pw.println(tab(5) + s"""case other => throw new IllegalArgumentException("Type $name did not union type tag " + other)""")
    pw.println(tab(4) + s"""}""")

    pw.println(tab(3) + s"""case other => throw new IllegalArgumentException("Type $name did not recognize " + other)""")
    pw.println(tab(2) + s"}")
    pw.println(tab(1) + "}")

    pw.println(tab(1) + s"def write(obj: $name): TaggedValue = {")
    pw.println(tab(2) + s"obj match {")
    unionDef.tags.foreach(tag => pw.println(tab(3) + s"case data: $tag => $tag.write(data)"))
    pw.println(tab(3) + s"""case other => throw new IllegalArgumentException("Type $name did not recognize " + other)""")
    pw.println(tab(2) + s"}")
    pw.println(tab(1) + "}")

    pw.println("}")

    pw.println(s"""sealed trait $name""")
    pw.println()
  }

  def writeWrapperStatic(name: String, wrapper: WrapperDef, superType: Option[String], pw: PrintWriter): Unit = {
    pw.println(s"object $name {")
    pw.println()

    val typeSignature = wrapper.field.typ match {
      case SimpleTypeDef(typ) => inputSignatureFor(typ, name)
      case ParamTypeDef(typ) => inputSignatureFor(typ, name)
      case TagTypeDef(tag) => tag
    }

    pw.println(tab(1) + s"def read(element: Value, ctx: ReaderContext): Either[String, $name] = {")
    pw.println(tab(2) + s"element match {")
    pw.println(tab(3) + s"case data: $typeSignature => readRepr(data, ctx)")
    pw.println(tab(3) + s"case tagged: TaggedValue =>")
    pw.println(tab(4) + s"tagged.value match {")
    pw.println(tab(5) + s"case data: $typeSignature => readRepr(data, ctx)")
    pw.println(tab(5) + s"""case other => Left("Type $name did not recognize value type " + other)""")
    pw.println(tab(4) + "}")
    pw.println(tab(3) + s"""case other => Left("Type $name did not recognize value type " + other)""")
    pw.println(tab(2) + "}")
    pw.println(tab(1) + "}")

    pw.println(tab(1) + s"def readRepr(element: $typeSignature, ctx: ReaderContext): Either[String, $name] = {")
    wrapper.field.typ match {
      case std: SimpleTypeDef => {
        val readFun = readFuncForSimpleTyp(std.typ)
        pw.println(tab(2) + "" + s"""$readFun(element, ctx).map(result => $name(result))""")
      }
      case ptd: ParamTypeDef => {
        ptd.typ match {
          case typ: TList => {
            val paramRead = readFuncForTypeParam(typ.paramType)
            val paramSig = fieldSignatureFor(typ.paramType)
            pw.println(tab(2) + "" + s"""$utilKlass.readList[$paramSig](element, $utilKlass.readTup[$paramSig](_, _, $paramRead), ctx).map(result => $name(result))""")
          }
          case other => throw new IllegalArgumentException(s"Parameterized type not handled: $other")
        }
      }
      case ttd: TagTypeDef => {
        val tagName = ttd.tag
        pw.println(tab(2) + "" + s"""$utilKlass.readFieldSubStruct("$name", element, "$tagName", $tagName.read, ctx).map(result => $name(result))""")
      }
    }
    pw.println(tab(1) + "}")

    pw.println(tab(1) + s"def write(obj: $name): TaggedValue = {")
    pw.println(tab(2) + "val built = " + writeCallFor(wrapper.field.typ, s"obj.${wrapper.field.name}"))
    pw.println()
    pw.println(tab(2) + s"""TaggedValue("$name", built)""")
    pw.println(tab(1) + "}")

    pw.println("}")

    val extStr = superType.map(t => s" extends $t").getOrElse("")

    pw.println(s"""case class $name(${wrapper.field.name}: ${signatureFor(wrapper.field.typ)})$extStr""")
    pw.println()
  }

  def writeStatic(name: String, objDef: StructDef, superType: Option[String], pw: PrintWriter): Unit = {
    pw.println(s"object $name {")
    pw.println()

    pw.println(tab(1) + s"def read(element: Value, ctx: ReaderContext): Either[String, $name] = {")
    pw.println(tab(2) + s"element match {")
    pw.println(tab(3) + s"case data: ValueMap => readMap(data, ctx)")
    pw.println(tab(3) + s"case tagged: TaggedValue =>")
    pw.println(tab(4) + s"tagged.value match {")
    pw.println(tab(5) + s"case data: ValueMap => readMap(data, ctx)")
    pw.println(tab(5) + s"""case other => Left("Type $name did not recognize value type " + other)""")
    pw.println(tab(4) + "}")
    pw.println(tab(3) + s"""case other => Left("Type $name did not recognize value type " + other)""")
    pw.println(tab(2) + "}")
    pw.println(tab(1) + "}")

    pw.println(tab(1) + s"def readMap(element: ValueMap, ctx: ReaderContext): Either[String, $name] = {")
    objDef.fields.foreach { fd =>
      val name = fd.name
      fd.typ match {
        case std: SimpleTypeDef => {
          val readFun = readFuncForSimpleTyp(std.typ)
          pw.println(tab(2) + "" + s"""val $name = $utilKlass.getMapField("$name", element).flatMap(elem => $readFun(elem, ctx))""")
        }
        case ptd: ParamTypeDef => {
          ptd.typ match {
            case typ: TList => {
              val paramRead = readFuncForTypeParam(typ.paramType)
              val paramSig = fieldSignatureFor(typ.paramType)
              pw.println(tab(2) + "" + s"""val $name = $utilKlass.getMapField("$name", element).flatMap(elem => $utilKlass.readList[$paramSig](elem, $utilKlass.readTup[$paramSig](_, _, $paramRead), ctx))""")
            }
            case optTyp: TOption => {
              val paramRead = readFuncForTypeParam(optTyp.paramType)
              pw.println(tab(2) + "" + s"""val $name = $utilKlass.optMapField("$name", element).flatMap(elem => $utilKlass.asOption(elem)).map(elem => $paramRead(elem, ctx).map(r => Some(r))).getOrElse(Right(None))""")

            }
            case other => throw new IllegalArgumentException(s"Parameterized type not handled: $other")
          }
        }
        case ttd: TagTypeDef => {
          val tagName = ttd.tag
          pw.println(tab(2) + "" + s"""val $name = $utilKlass.getMapField("$name", element).flatMap(elem => $utilKlass.readFieldSubStruct("$name", elem, "$tagName", $tagName.read, ctx))""")
        }
      }
    }
    pw.println()
    val isRightJoin = objDef.fields.map(f => f.name + ".isRight").mkString(" && ")
    pw.println(tab(2) + s"if ($isRightJoin) {")
    val rightGetJoin = objDef.fields.map(_.name + ".right.get").mkString(", ")
    pw.println(tab(3) + s"Right($name($rightGetJoin))")
    pw.println(tab(2) + "} else {")
    val leftJoin = objDef.fields.map(_.name + ".left.toOption").mkString(", ")
    pw.println(tab(3) + s"""Left(Seq($leftJoin).flatten.mkString(", "))""")
    pw.println(tab(2) + "}")
    pw.println(tab(1) + "}")
    pw.println()

    pw.println(tab(1) + s"def write(obj: $name): TaggedValue = {")
    pw.println(tab(2) + "val built = ValueMap(Map(")
    val buildList = objDef.fields.map { d =>
      s"""(ValueString("${d.name}"), ${writeCallFor(d.typ, s"obj.${d.name}")})"""
    }.mkString(tab(3), ",\n" + tab(3), "")
    pw.println(buildList)
    pw.println(tab(2) + "))")
    pw.println()
    pw.println(tab(2) + s"""TaggedValue("$name", built)""")
    pw.println(tab(1) + "}")

    pw.println("}")

    val fieldDescs = objDef.fields.map(fd => s"${fd.name}: ${signatureFor(fd.typ)}")

    val extStr = superType.map(t => s" extends $t").getOrElse("")

    pw.println(s"""case class $name(${fieldDescs.mkString(", ")})$extStr""")
    pw.println()
  }

}