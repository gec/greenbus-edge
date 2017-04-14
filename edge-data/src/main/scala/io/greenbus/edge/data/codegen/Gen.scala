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

import io.greenbus.edge.data.schema._

object Gen {

  case class TypeDefFrag(code: String)

  sealed trait FieldTypeDef
  case class SimpleTypeDef(typ: ValueType) extends FieldTypeDef
  case class ParamTypeDef(typ: ValueType) extends FieldTypeDef
  case class TagTypeDef(tag: String) extends FieldTypeDef

  case class FieldDef(name: String, typ: FieldTypeDef)

  sealed trait ObjDef
  case class StructDef(fields: Seq[FieldDef]) extends ObjDef
  case class WrapperDef(field: FieldDef) extends ObjDef

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
      case union: TUnion => singleParam(union)
      case either: TEither => singleParam(either)
      case option: TOption => singleParam(option)
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

  def output(pkg: String, objs: Map[String, ObjDef], pw: PrintWriter): Unit = {

    pw.println(s"package $pkg")
    pw.println()
    pw.println("import io.greenbus.edge.tag._")
    pw.println()

    objs.foreach {
      case (tag, obj) =>
        obj match {
          case d: StructDef => writeStatic(tag, d, pw)
          case d: WrapperDef => writeWrapperStatic(tag, d, pw)
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
          case TBool => "VBool"
          case TInt32 => "VInt32"
          case TUInt32 => "VUInt32"
          case TInt64 => "VInt64"
          case TUInt64 => "VUInt64"
          case TFloat => "VFloat"
          case TDouble => "VDouble"
          case TString => "VString"
          case _ => throw new IllegalArgumentException(s"Type unhandled: $typ")
        }
    }
  }

  def inputSignatureFor(typ: ValueType): String = {
    typ match {
      case t: TExt => s"${t.tag}"
      case t: TList => "VList"
      case t: TMap => "VMap"
      case TBool => "VBool"
      case TInt32 => "VInt32"
      case TUInt32 => "VUInt32"
      case TInt64 => "VInt64"
      case TUInt64 => "VUInt64"
      case TFloat => "VFloat"
      case TDouble => "VDouble"
      case TString => "VString"
      case _ => throw new IllegalArgumentException(s"Type unhandled: $typ")
    }
  }

  def tab(n: Int): String = Range(0, n).map(_ => "  ").mkString("")

  def writeWrapperStatic(name: String, wrapper: WrapperDef, pw: PrintWriter): Unit = {
    pw.println(s"object $name {")
    pw.println()

    val typeSignature = wrapper.field.typ match {
      case SimpleTypeDef(typ) => inputSignatureFor(typ)
      case ParamTypeDef(typ) => inputSignatureFor(typ)
      case TagTypeDef(tag) => tag
    }

    pw.println(tab(1) + s"def read(element: ValueElement, ctx: ReaderContext): Either[String, $name] = {")
    pw.println(tab(2) + s"element match {")
    pw.println(tab(3) + s"case data: $typeSignature =>")
    wrapper.field.typ match {
      case std: SimpleTypeDef => {
        val readFun = readFuncForSimpleTyp(std.typ)
        pw.println(tab(4) + "" + s"""$readFun(data, ctx).map(result => $name(result))""")
      }
      case ptd: ParamTypeDef => {
        ptd.typ match {
          case typ: TList => {
            val paramRead = readFuncForTypeParam(typ.paramType)
            val paramSig = fieldSignatureFor(typ.paramType)
            pw.println(tab(4) + "" + s"""$utilKlass.readList[$paramSig](data, $utilKlass.readTup[$paramSig](_, _, $paramRead), ctx).map(result => $name(result))""")
          }
          case other => throw new IllegalArgumentException(s"Parameterized type not handled: $other")
        }
      }
      case ttd: TagTypeDef => {
        val tagName = ttd.tag
        pw.println(tab(4) + "" + s"""$utilKlass.readFieldSubStruct("$name", data, "$tagName", $tagName.read, ctx).map(result => $name(result))""")
      }
    }
    pw.println(tab(3) + s"""case _ => Left("$name must be $typeSignature type")""")
    pw.println(tab(2) + s"}")
    pw.println(tab(1) + "}")

    pw.println(tab(1) + s"def write(obj: $name): TaggedValue = {")

    pw.println(tab(2) + "val built = " + writeCallFor(wrapper.field.typ, s"obj.${wrapper.field.name}"))

    //pw.println(tab(2) + "))")
    pw.println()
    pw.println(tab(2) + s"""TaggedValue("$name", built)""")
    pw.println(tab(1) + "}")

    pw.println("}")

    pw.println(s"""case class $name(${wrapper.field.name}: ${signatureFor(wrapper.field.typ)})""")
    pw.println()
  }

  def writeStatic(name: String, objDef: StructDef, pw: PrintWriter): Unit = {
    pw.println(s"object $name {")
    pw.println()

    pw.println(tab(1) + s"def read(element: ValueElement, ctx: ReaderContext): Either[String, $name] = {")
    pw.println(tab(2) + s"element match {")
    pw.println(tab(3) + s"case data: VMap =>")
    objDef.fields.foreach { fd =>
      val name = fd.name
      fd.typ match {
        case std: SimpleTypeDef => {
          val readFun = readFuncForSimpleTyp(std.typ)
          pw.println(tab(4) + "" + s"""val $name = $utilKlass.getMapField("$name", data).flatMap(elem => $readFun(elem, ctx))""")
        }
        case ptd: ParamTypeDef => {
          ptd.typ match {
            case typ: TList => {
              val paramRead = readFuncForTypeParam(typ.paramType)
              val paramSig = fieldSignatureFor(typ.paramType)
              pw.println(tab(4) + "" + s"""val $name = $utilKlass.getMapField("$name", data).flatMap(elem => $utilKlass.readList[$paramSig](elem, $utilKlass.readTup[$paramSig](_, _, $paramRead), ctx))""")
            }
            case other => throw new IllegalArgumentException(s"Parameterized type not handled: $other")
          }
        }
        case ttd: TagTypeDef => {
          val tagName = ttd.tag
          pw.println(tab(4) + "" + s"""val $name = $utilKlass.getMapField("$name", data).flatMap(elem => $utilKlass.readFieldSubStruct("$name", elem, "$tagName", $tagName.read, ctx))""")
        }
      }
    }
    pw.println()

    val isRightJoin = objDef.fields.map(f => f.name + ".isRight").mkString(" && ")
    pw.println(tab(4) + s"if ($isRightJoin) {")
    val rightGetJoin = objDef.fields.map(_.name + ".right.get").mkString(", ")
    pw.println(tab(5) + s"Right($name($rightGetJoin))")
    pw.println(tab(4) + "} else {")
    val leftJoin = objDef.fields.map(_.name + ".left.toOption").mkString(", ")
    pw.println(tab(5) + s"""Left(Seq($leftJoin).flatten.mkString(", "))""")
    pw.println(tab(4) + "}")
    pw.println()
    pw.println(tab(3) + s"""case _ => Left("$name must be VMap type")""")
    pw.println(tab(2) + s"}")
    pw.println(tab(1) + "}")
    pw.println()

    pw.println(tab(1) + s"def write(obj: $name): TaggedValue = {")
    pw.println(tab(2) + "val built = VMap(Map(")
    val buildList = objDef.fields.map { d =>
      s"""(VString("${d.name}"), ${writeCallFor(d.typ, s"obj.${d.name}")})"""
    }.mkString(tab(3), ",\n" + tab(3), "")
    pw.println(buildList)
    pw.println(tab(2) + "))")
    pw.println()
    pw.println(tab(2) + s"""TaggedValue("$name", built)""")
    pw.println(tab(1) + "}")

    pw.println("}")

    val fieldDescs = objDef.fields.map(fd => s"${fd.name}: ${signatureFor(fd.typ)}")

    pw.println(s"""case class $name(${fieldDescs.mkString(", ")})""")
    pw.println()
  }

}