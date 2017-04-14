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

  def write(types: Seq[TExt], concreteTypes: Seq[TExt], uri: String, os: OutputStream): Unit = {

    val output = XMLOutputFactory.newFactory()
    val base = output.createXMLStreamWriter(os)
    val w = new IndentingXMLStreamWriter(base)
    w.writeStartDocument("UTF-8", "1.0")

    w.writeStartElement("xs", "schema", xmlSchemaNs)
    w.writeAttribute("xmlns:xs", xmlSchemaNs)
    w.writeAttribute("xmlns", uri)
    w.writeAttribute("targetNamespace", uri)
    w.writeAttribute("elementFormDefault", "qualified")

    types.foreach(writeType(_, w))

    concreteTypes.foreach { typ =>
      writeExtension(typ.tag, typ.tag, w)
    }

    w.writeEndElement()

    w.writeEndDocument()
    w.flush()
  }

  def writeType(typ: TExt, w: XMLStreamWriter): Unit = {
    typ.reprType match {
      case t: TStruct => writeExtStruct(typ.tag, t, w)
      case t: TList => writeExtList(typ.tag, t, w)
      case t => //System.err.println(t.getClass.getSimpleName + ": " + typ.tag)
    }
  }

  def writeExtension(elemName: String, extName: String, w: XMLStreamWriter, minOccurs: String = "1", maxOccurs: String = "1"): Unit = {

    w.writeStartElement("xs", "element", xmlSchemaNs)
    w.writeAttribute("name", elemName)
    if (minOccurs != "1") w.writeAttribute("minOccurs", minOccurs)
    if (maxOccurs != "1") w.writeAttribute("maxOccurs", maxOccurs)
    w.writeStartElement("xs", "complexType", xmlSchemaNs)
    w.writeStartElement("xs", "complexContent", xmlSchemaNs)
    w.writeEmptyElement("xs", "extension", xmlSchemaNs)
    w.writeAttribute("base", extName)
    w.writeEndElement()
    w.writeEndElement()

    w.writeEndElement()
  }

  def writeSimple(name: String, restrict: String, w: XMLStreamWriter): Unit = {
    w.writeStartElement("xs", "element", xmlSchemaNs)
    w.writeAttribute("name", name)
    w.writeStartElement("xs", "simpleType", xmlSchemaNs)
    w.writeEmptyElement("xs", "restriction", xmlSchemaNs)
    w.writeAttribute("base", restrict)
    w.writeEndElement()
    w.writeEndElement()
  }

  def writeExtStruct(tag: String, struct: TStruct, w: XMLStreamWriter): Unit = {
    w.writeStartElement("xs", "complexType", xmlSchemaNs)
    w.writeAttribute("name", tag)

    w.writeStartElement("xs", "all", xmlSchemaNs)

    struct.fields.foreach { sfd =>

      sfd.typ match {
        case t: TExt => {
          writeExtension(sfd.name, t.tag, w)
        }
        case t: TList => {
          writeListConcrete(sfd.name, t, w)
        }
        case TBool => writeSimple(sfd.name, "xs:boolean", w)
        case TByte => writeSimple(sfd.name, "xs:byte", w)
        case TInt32 => writeSimple(sfd.name, "xs:int", w)
        case TUInt32 => writeSimple(sfd.name, "xs:unsignedInt", w)
        case TInt64 => writeSimple(sfd.name, "xs:long", w)
        case TUInt64 => writeSimple(sfd.name, "xs:unsignedLong", w)
        case TFloat => writeSimple(sfd.name, "xs:decimal", w)
        case TDouble => writeSimple(sfd.name, "xs:decimal", w)
        case TString => writeSimple(sfd.name, "xs:string", w)
        case _ =>
      }

    }

    w.writeEndElement()

    w.writeEndElement()
  }

  def writeListConcrete(name: String, list: TList, w: XMLStreamWriter): Unit = {
    w.writeStartElement("xs", "element", xmlSchemaNs)
    w.writeAttribute("name", name)

    writeListComplex(None, list, w)

    w.writeEndElement()
  }

  def writeListComplex(nameOpt: Option[String], list: TList, w: XMLStreamWriter): Unit = {
    w.writeStartElement("xs", "complexType", xmlSchemaNs)
    nameOpt.foreach(name => w.writeAttribute("name", name))
    w.writeStartElement("xs", "sequence", xmlSchemaNs)

    list.paramType match {
      case ext: TExt => {
        writeExtension(ext.tag, ext.tag, w, minOccurs = "0", maxOccurs = "unbounded")
      }
      case other => throw new IllegalArgumentException(s"Not handling list param type: " + other)
    }
    w.writeEndElement()
    w.writeEndElement()
  }

  def writeExtList(tag: String, list: TList, w: XMLStreamWriter): Unit = {
    writeListComplex(Some(tag), list, w)
  }
}