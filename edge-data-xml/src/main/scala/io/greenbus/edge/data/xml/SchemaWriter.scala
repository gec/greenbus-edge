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
import io.greenbus.edge.data.schema._

object SchemaWriter {
  val xmlSchemaNs = "http://www.w3.org/2001/XMLSchema"

  def write(types: Seq[TExt], concreteTypes: Seq[TExt], info: XmlNamespaceInfo, os: OutputStream): Unit = {

    val output = XMLOutputFactory.newFactory()
    val base = output.createXMLStreamWriter(os)
    val w = new IndentingXMLStreamWriter(base)
    w.writeStartDocument("UTF-8", "1.0")

    w.writeStartElement("xs", "schema", xmlSchemaNs)
    w.writeAttribute("xmlns:xs", xmlSchemaNs)
    //w.writeAttribute("xmlns", uri)
    writeRootNsDeclarations(w, info)
    w.writeAttribute("elementFormDefault", "qualified")

    types.foreach(writeType(_, info, w))

    concreteTypes.foreach { typ =>
      writeExtension(typ.tag, typ.tag, typ.ns, info, w)
    }

    w.writeEndElement()

    w.writeEndDocument()
    w.flush()
  }

  def writeRootNsDeclarations(w: XMLStreamWriter, namespaceInfo: XmlNamespaceInfo): Unit = {
    val defaultDeclOpt = namespaceInfo.namespaceMap.get(namespaceInfo.defaultTypeNs)
    defaultDeclOpt.foreach { decl =>
      w.writeAttribute("xmlns", decl.uri)
      w.writeAttribute("targetNamespace", decl.uri)
    }
    namespaceInfo.namespaceMap.toVector.
      filterNot(_._1 == namespaceInfo.defaultTypeNs)
      .sortBy(_._1).foreach {
        case (_, decl) =>
          w.writeAttribute(s"xmlns:${decl.prefix}", decl.uri)
      }
  }

  def writeType(typ: TExt, nsInfo: XmlNamespaceInfo, w: XMLStreamWriter): Unit = {
    System.err.println(s"${typ.tag}")
    typ.reprType match {
      case t: TStruct => writeExtStruct(typ.tag, t, typ.ns, nsInfo, w)
      case t: TList => writeExtList(typ.tag, t, nsInfo, w)
      case t: TEnum => writeEnum(typ.tag, t, w)
      case t: TUnion => writeUnion(typ.tag, t, w)
      case t => System.err.println("Unhandled: " + t.getClass.getSimpleName + ": " + typ.tag)
    }
  }

  /*
      <!--<xs:element name="transforms">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="TransformDescriptor" minOccurs="0" maxOccurs="unbounded">
              <xs:complexType>
                <xs:complexContent>
                  <xs:extension base="TransformDescriptor"/>
                </xs:complexContent>
              </xs:complexType>
            </xs:element>
          </xs:sequence>
        </xs:complexType>
      </xs:element>-->
      <xs:element name="transforms">
        <xs:complexType>
          <xs:choice minOccurs="0" maxOccurs="unbounded">
            <xs:element name="LinearTransform" type="LinearTransform"/>
            <xs:element name="SimpleTransform" type="SimpleTransform"/>
            <xs:element name="TypeCast" type="TypeCast"/>
          </xs:choice>
        </xs:complexType>
      </xs:element>
   */

  def writeUnion(tag: String, union: TUnion, w: XMLStreamWriter, anon: Boolean = false, minOccurs: String = "1", maxOccurs: String = "1"): Unit = {

    w.writeStartElement("xs", "complexType", xmlSchemaNs)
    if (!anon) { w.writeAttribute("name", tag) }
    w.writeStartElement("xs", "choice", xmlSchemaNs)
    if (minOccurs != "1") w.writeAttribute("minOccurs", minOccurs)
    if (maxOccurs != "1") w.writeAttribute("maxOccurs", maxOccurs)

    val tags = union.unionTypes.map {
      case t: TExt => t.tag
      case other => throw new IllegalArgumentException(s"Type unhandled in union: $other")
    }.toVector

    tags.sorted.foreach { name =>
      w.writeEmptyElement("xs", "element", xmlSchemaNs)
      w.writeAttribute("name", name)
      w.writeAttribute("type", name)
    }

    w.writeEndElement()
    w.writeEndElement()
  }

  def writeEnum(tag: String, enum: TEnum, w: XMLStreamWriter): Unit = {
    w.writeStartElement("xs", "simpleType", xmlSchemaNs)
    w.writeAttribute("name", tag)
    w.writeStartElement("xs", "restriction", xmlSchemaNs)
    w.writeAttribute("base", "xs:string")

    enum.enumDefs.foreach { ed =>
      w.writeEmptyElement("xs", "enumeration", xmlSchemaNs)
      w.writeAttribute("value", ed.label)
    }

    w.writeEndElement()
    w.writeEndElement()
  }

  def writeExtension(elemName: String, extName: String, tns: TypeNamespace, nsInfo: XmlNamespaceInfo, w: XMLStreamWriter, minOccurs: String = "1", maxOccurs: String = "1"): Unit = {

    w.writeStartElement("xs", "element", xmlSchemaNs)
    w.writeAttribute("name", elemName)
    if (minOccurs != "1") w.writeAttribute("minOccurs", minOccurs)
    if (maxOccurs != "1") w.writeAttribute("maxOccurs", maxOccurs)
    w.writeStartElement("xs", "complexType", xmlSchemaNs)
    w.writeStartElement("xs", "complexContent", xmlSchemaNs)
    w.writeEmptyElement("xs", "extension", xmlSchemaNs)
    w.writeAttribute("base", nameWithPrefix(extName, tns, nsInfo))
    w.writeEndElement()
    w.writeEndElement()

    w.writeEndElement()
  }

  def writeSimple(name: String, restrict: String, w: XMLStreamWriter, minOccurs: String = "1", maxOccurs: String = "1"): Unit = {
    w.writeStartElement("xs", "element", xmlSchemaNs)
    w.writeAttribute("name", name)
    if (minOccurs != "1") w.writeAttribute("minOccurs", minOccurs)
    if (maxOccurs != "1") w.writeAttribute("maxOccurs", maxOccurs)
    w.writeStartElement("xs", "simpleType", xmlSchemaNs)
    w.writeEmptyElement("xs", "restriction", xmlSchemaNs)
    w.writeAttribute("base", restrict)
    w.writeEndElement()
    w.writeEndElement()
  }

  def namePrefix(ns: TypeNamespace, info: XmlNamespaceInfo): Option[String] = {
    info.namespaceMap.get(ns.name).map(_.prefix)
  }

  def nameWithPrefix(name: String, ns: TypeNamespace, info: XmlNamespaceInfo): String = {
    namePrefix(ns, info) match {
      case None => name
      case Some(pre) =>
        if (ns.name == info.defaultTypeNs) {
          name
        } else {
          s"$pre:$name"
        }
    }
  }

  def writeExtStruct(tag: String, struct: TStruct, tns: TypeNamespace, nsInfo: XmlNamespaceInfo, w: XMLStreamWriter): Unit = {
    w.writeStartElement("xs", "complexType", xmlSchemaNs)
    w.writeAttribute("name", nameWithPrefix(tag, tns, nsInfo))

    w.writeStartElement("xs", "all", xmlSchemaNs)

    struct.fields.foreach { sfd =>
      writeField(sfd.name, sfd.typ, nsInfo, w)
    }

    w.writeEndElement()

    w.writeEndElement()
  }

  private def writeField(name: String, typ: VTValueElem, nsInfo: XmlNamespaceInfo, w: XMLStreamWriter, minOccurs: String = "1", maxOccurs: String = "1"): Unit = {

    typ match {
      case t: TExt => {
        writeExtension(name, t.tag, t.ns, nsInfo, w, minOccurs, maxOccurs)
      }
      case t: TList => {
        writeListConcrete(name, t, nsInfo, w, minOccurs, maxOccurs)
      }
      case TBool => writeSimple(name, "xs:boolean", w, minOccurs, maxOccurs)
      case TByte => writeSimple(name, "xs:byte", w, minOccurs, maxOccurs)
      case TInt32 => writeSimple(name, "xs:int", w, minOccurs, maxOccurs)
      case TUInt32 => writeSimple(name, "xs:unsignedInt", w, minOccurs, maxOccurs)
      case TInt64 => writeSimple(name, "xs:long", w, minOccurs, maxOccurs)
      case TUInt64 => writeSimple(name, "xs:unsignedLong", w, minOccurs, maxOccurs)
      case TFloat => writeSimple(name, "xs:decimal", w, minOccurs, maxOccurs)
      case TDouble => writeSimple(name, "xs:decimal", w, minOccurs, maxOccurs)
      case TString => writeSimple(name, "xs:string", w, minOccurs, maxOccurs)
      case t: TOption => writeField(name, t.paramType, nsInfo, w, minOccurs = "0")
      case _ =>
    }
  }

  def writeListConcrete(name: String, list: TList, nsInfo: XmlNamespaceInfo, w: XMLStreamWriter, minOccurs: String = "1", maxOccurs: String = "1"): Unit = {
    w.writeStartElement("xs", "element", xmlSchemaNs)
    w.writeAttribute("name", name)
    if (minOccurs != "1") w.writeAttribute("minOccurs", minOccurs)
    if (maxOccurs != "1") w.writeAttribute("maxOccurs", maxOccurs)

    writeListComplex(None, list, nsInfo, w)

    w.writeEndElement()
  }

  def writeListComplex(nameOpt: Option[String], list: TList, nsInfo: XmlNamespaceInfo, w: XMLStreamWriter): Unit = {
    // w.writeStartElement("xs", "sequence", xmlSchemaNs)

    def wrapSequence(f: => Unit) = {
      w.writeStartElement("xs", "complexType", xmlSchemaNs)
      nameOpt.foreach(name => w.writeAttribute("name", name))
      w.writeStartElement("xs", "sequence", xmlSchemaNs)
      f
      w.writeEndElement()
      w.writeEndElement()
    }

    list.paramType match {
      case ext: TExt => {
        ext.reprType match {
          case t: TUnion => writeUnion(ext.tag, t, w, anon = true, minOccurs = "0", maxOccurs = "unbounded")
          case other => wrapSequence { writeExtension(ext.tag, ext.tag, ext.ns, nsInfo, w, minOccurs = "0", maxOccurs = "unbounded") }
        }
        //writeExtension(ext.tag, ext.tag, ext.ns, nsInfo, w, minOccurs = "0", maxOccurs = "unbounded")
      }
      case TBool => wrapSequence { writeSimple("value", "xs:boolean", w, minOccurs = "0", maxOccurs = "unbounded") }
      case TByte => wrapSequence { writeSimple("value", "xs:byte", w, minOccurs = "0", maxOccurs = "unbounded") }
      case TInt32 => wrapSequence { writeSimple("value", "xs:int", w, minOccurs = "0", maxOccurs = "unbounded") }
      case TUInt32 => wrapSequence { writeSimple("value", "xs:unsignedInt", w, minOccurs = "0", maxOccurs = "unbounded") }
      case TInt64 => wrapSequence { writeSimple("value", "xs:long", w, minOccurs = "0", maxOccurs = "unbounded") }
      case TUInt64 => wrapSequence { writeSimple("value", "xs:unsignedLong", w, minOccurs = "0", maxOccurs = "unbounded") }
      case TFloat => wrapSequence { writeSimple("value", "xs:decimal", w, minOccurs = "0", maxOccurs = "unbounded") }
      case TDouble => wrapSequence { writeSimple("value", "xs:decimal", w, minOccurs = "0", maxOccurs = "unbounded") }
      case TString => wrapSequence { writeSimple("value", "xs:string", w, minOccurs = "0", maxOccurs = "unbounded") }
      case other => throw new IllegalArgumentException(s"Not handling list param type: " + other)
    }
    //w.writeEndElement()
  }

  def writeExtList(tag: String, list: TList, nsInfo: XmlNamespaceInfo, w: XMLStreamWriter): Unit = {
    writeListComplex(Some(tag), list, nsInfo, w)
  }
}