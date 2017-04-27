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
package io.greenbus.edge.data.xml

import java.io.OutputStream
import javax.xml.stream.{ XMLOutputFactory, XMLStreamWriter }

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter
import io.greenbus.edge.data._
import io.greenbus.edge.data.schema._

case class XmlNsDecl(prefix: String, uri: String)
case class XmlNamespaceInfo(defaultTypeNs: String, namespaceMap: Map[String, XmlNsDecl])

object SchemaGuidedXmlWriter {

  def xmlNamespace(ns: TypeNamespace, info: XmlNamespaceInfo): XmlNsDecl = {
    info.namespaceMap.getOrElse(ns.name, throw new IllegalArgumentException(s"Type namespace ${ns.name} did not have corresponding xmlns"))
  }

  def nsDeclFor(ns: TypeNamespace, info: XmlNamespaceInfo): Option[XmlNsDecl] = {
    if (info.defaultTypeNs == ns.name) {
      None
    } else {
      val decl = info.namespaceMap.getOrElse(ns.name, throw new IllegalArgumentException(s"Type namespace ${ns.name} did not have corresponding xmlns"))
      Some(decl)
    }
  }

  case class NsContext(explicitOpt: Option[XmlNsDecl], info: XmlNamespaceInfo) {
    def writeStartElement(w: XMLStreamWriter, name: String): Unit = {
      explicitOpt match {
        case Some(ctx) => w.writeStartElement(ctx.prefix, name, ctx.uri)
        case None => w.writeStartElement(name)
      }
    }
  }

  def write(value: Value, schema: TExt, os: OutputStream, namespaceInfo: XmlNamespaceInfo): Unit = {

    val output = XMLOutputFactory.newFactory()
    val base = output.createXMLStreamWriter(os)
    val w = new IndentingXMLStreamWriter(base)
    w.writeStartDocument("UTF-8", "1.0")

    val currentNs = nsDeclFor(schema.ns, namespaceInfo)

    writeStruct(value, schema, w, NsContext(currentNs, namespaceInfo), None, isRoot = true)

    w.writeEndDocument()
    w.flush()
  }

  def writeRootNsDeclarations(w: XMLStreamWriter, namespaceInfo: XmlNamespaceInfo): Unit = {
    val defaultDeclOpt = namespaceInfo.namespaceMap.get(namespaceInfo.defaultTypeNs)
    defaultDeclOpt.foreach { decl =>
      w.writeAttribute("xmlns", decl.uri)
    }
    namespaceInfo.namespaceMap.toVector.
      filterNot(_._1 == namespaceInfo.defaultTypeNs)
      .sortBy(_._1).foreach {
        case (_, decl) =>
          w.writeAttribute(s"xmlns:${decl.prefix}", decl.uri)
      }
  }

  def writeSimple(typName: String, value: String, w: XMLStreamWriter, nsCtx: NsContext, ctxName: Option[String] = None): Unit = {
    nsCtx.writeStartElement(w, ctxName.getOrElse("value"))
    //w.writeStartElement(ctxName.getOrElse("value"))
    w.writeCharacters(value)
    w.writeEndElement()
  }

  def writeEnum(value: Value, typ: TEnum, w: XMLStreamWriter, nsCtx: NsContext, ctxName: String): Unit = {
    val labelSet = typ.enumDefs.map(_.label).toSet
    val numToLabel = typ.enumDefs.map(e => (e.value, e.label)).toMap

    val enumValue: String = value match {
      case v: ValueString =>
        if (labelSet.contains(v.value)) {
          v.value
        } else {
          throw new IllegalArgumentException(s"Enumeration value ${v.value} did not match enumeration type")
        }
      case v: IntegerValue => {
        numToLabel.getOrElse(v.toInt, throw new IllegalArgumentException(s"Value ${v.toInt} was not in enumeration type"))
      }
      case _ => throw new IllegalArgumentException(s"Representation for enum was not string nor integer")
    }

    //w.writeStartElement(ctxName)
    nsCtx.writeStartElement(w, ctxName)
    w.writeCharacters(enumValue)
    w.writeEndElement()

  }

  def writeTypedValue(value: Value, typ: VTValueElem, w: XMLStreamWriter, nsCtx: NsContext, ctxName: Option[String] = None): Unit = {

    def simpleMismatch(typ: String) = throw new IllegalArgumentException(s"Boolean value did not have boolean representation: $value")

    typ match {
      case t: TExt => writeStruct(value, t, w, nsCtx, ctxName)
      case t: TList => {
        value match {
          case v: ValueList => {
            nsCtx.writeStartElement(w, ctxName.getOrElse("list"))
            //w.writeStartElement(ctxName.getOrElse("list"))
            v.value.foreach(item => writeTypedValue(item, t.paramType, w, nsCtx, None))
            w.writeEndElement()
          }
          case _ => throw new IllegalArgumentException(s"List type did not have list value")
        }
      }
      case t: TMap => {
        System.err.println("SKIPPED MAP")
      }
      case t: TEnum => {
        val name = ctxName.getOrElse(throw new IllegalArgumentException(s"Enum must have contextual name"))
        writeEnum(value, t, w, nsCtx, name)
      }
      case t: TOption => {
        value match {
          case ValueNone =>
          case v => writeTypedValue(v, t.paramType, w, nsCtx, ctxName)
        }
      }
      case TBool => {
        value match {
          case v: ValueBool => writeSimple("bool", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("bool")
        }
      }
      case TByte => {
        value match {
          case v: ValueByte => writeSimple("byte", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("byte")
        }
      }
      case TInt32 => {
        value match {
          case v: ValueInt32 => writeSimple("int32", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("int32")
        }
      }
      case TUInt32 => {
        value match {
          case v: ValueUInt32 => writeSimple("uint32", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("uint32")
        }
      }
      case TInt64 => {
        value match {
          case v: ValueInt64 => writeSimple("int64", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("int64")
        }
      }
      case TUInt64 => {
        value match {
          case v: ValueUInt64 => writeSimple("uint64", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("uint64")
        }
      }
      case TFloat => {
        value match {
          case v: ValueFloat => writeSimple("float", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("float")
        }
      }
      case TDouble => {
        value match {
          case v: ValueDouble => writeSimple("double", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("double")
        }
      }
      case TString => {
        value match {
          case v: ValueString => writeSimple("string", v.value.toString, w, nsCtx, ctxName)
          case _ => simpleMismatch("string")
        }
      }
      case other => throw new IllegalArgumentException(s"Unhandled type: " + typ)
    }
  }

  def writeStruct(value: Value, typ: TExt, w: XMLStreamWriter, prevCtx: NsContext, ctxName: Option[String] = None, isRoot: Boolean = false): Unit = {
    //println(value.getClass.getSimpleName + " : " + ctxName)

    val (tagOpt, repr) = value match {
      case TaggedValue(tag, v) => (Some(tag), v)
      case v => (None, v)
    }

    val nextCtx = prevCtx.copy(explicitOpt = nsDeclFor(typ.ns, prevCtx.info))

    typ.reprType match {
      case t: TStruct => {
        repr match {
          case v: ValueMap => {
            //w.writeStartElement(ctxName.getOrElse(typ.tag))
            val nsToUse = if (ctxName.isEmpty) {
              nextCtx
            } else {
              prevCtx
            }

            nsToUse.writeStartElement(w, ctxName.getOrElse(typ.tag))
            if (isRoot) {
              writeRootNsDeclarations(w, nextCtx.info)
            }

            val labelMapBuilder = Map.newBuilder[String, Value]
            val numberMapBuilder = Map.newBuilder[Int, Value]

            v.value.foreach {
              case (key, mapValue) => {
                key match {
                  case ks: ValueString => labelMapBuilder += (ks.value -> mapValue)
                  case ki: IntegerValue => numberMapBuilder += (ki.toInt -> mapValue)
                  case _ =>
                }
              }
            }

            val labelMap = labelMapBuilder.result()
            val numberMap = numberMapBuilder.result()

            t.fields.foreach { sfd =>
              val fieldValueOpt = labelMap.get(sfd.name).orElse(numberMap.get(sfd.number))
              fieldValueOpt.foreach { fieldValue =>
                writeTypedValue(fieldValue, sfd.typ, w, nextCtx, Some(sfd.name))
              }
            }

            w.writeEndElement()
          }
          case other => throw new IllegalArgumentException(s"Representation of ${typ.tag} did not match type Map: $repr")
        }
      }
      case t: TList => {
        repr match {
          case v: ValueList => {
            //w.writeStartElement(ctxName.getOrElse(typ.tag))
            nextCtx.writeStartElement(w, ctxName.getOrElse(typ.tag))
            if (isRoot) {
              writeRootNsDeclarations(w, nextCtx.info)
            }
            v.value.foreach { itemValue =>
              writeTypedValue(itemValue, t.paramType, w, nextCtx, None)
            }
            w.writeEndElement()
          }
          case other => throw new IllegalArgumentException(s"Representation of ${typ.tag} did not match type List")
        }
      }
      case t: TEnum => {
        writeEnum(repr, t, w, nextCtx, ctxName.getOrElse(typ.tag))
      }
      case t: TUnion => {
        val tagMap = t.unionTypes.map {
          case ext: TExt => ext.tag -> ext
          case _ => throw new IllegalArgumentException(s"Union types must only include user typed values (${typ.tag})")
        }.toMap

        ctxName.foreach { name =>
          prevCtx.writeStartElement(w, name)
        }

        tagOpt match {
          case None => throw new IllegalArgumentException(s"Values of union types must be tagged (${typ.tag})")
          case Some(tag) =>
            tagMap.get(tag) match {
              case None => throw new IllegalArgumentException(s"Tag on value: $tag was not part of union ${typ.tag}")
              case Some(tagType) => {
                writeTypedValue(repr, tagType, w, nextCtx, None)
              }
            }
        }

        if (ctxName.nonEmpty) {
          w.writeEndElement()
        }
      }
      case other => System.err.println("UNHANDLED: " + other)
    }
  }

}
