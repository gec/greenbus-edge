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

object XmlWriter {

  def write(value: Value, os: OutputStream, xmlnsOpt: Option[String] = None): Unit = {

    val output = XMLOutputFactory.newFactory()
    val base = output.createXMLStreamWriter(os)
    val w = new IndentingXMLStreamWriter(base)
    w.writeStartDocument("UTF-8", "1.0")

    writeElem(value, w, None, xmlnsOpt)

    w.writeEndDocument()
    w.flush()
  }

  def writeValue(value: BasicValue, w: XMLStreamWriter, ctxName: Option[String] = None): Unit = {
    value match {
      case v: ValueList => {
        w.writeStartElement(ctxName.getOrElse("list"))
        v.value.foreach(elem => writeElem(elem, w))
        w.writeEndElement()
      }
      case v: ValueMap => {
        w.writeStartElement(ctxName.getOrElse("map"))
        v.value.foreach {
          case (keyElem, valueElem) =>
            w.writeStartElement("entry")
            w.writeStartElement("key")
            writeElem(keyElem, w)
            w.writeEndElement()
            w.writeStartElement("value")
            writeElem(valueElem, w)
            w.writeEndElement()
            w.writeEndElement()
        }
        w.writeEndElement()
      }
      case v: ValueBool => writeSimple("bool", v.value.toString, w, ctxName)
      case v: ValueByte => writeSimple("byte", v.value.toString, w, ctxName)
      case v: ValueInt32 => writeSimple("int32", v.value.toString, w, ctxName)
      case v: ValueUInt32 => writeSimple("uint32", v.value.toString, w, ctxName)
      case v: ValueInt64 => writeSimple("int64", v.value.toString, w, ctxName)
      case v: ValueUInt64 => writeSimple("uint64", v.value.toString, w, ctxName)
      case v: ValueFloat => writeSimple("float", v.value.toString, w, ctxName)
      case v: ValueDouble => writeSimple("double", v.value.toString, w, ctxName)
      case v: ValueString => writeSimple("string", v.value.toString, w, ctxName)
      case _ => throw new IllegalArgumentException(s"Type not handled: " + value)
    }
  }

  def writeElem(value: Value, w: XMLStreamWriter, ctxName: Option[String] = None, xmlnsOpt: Option[String] = None): Unit = {
    //println(value.getClass.getSimpleName + " : " + ctxName)
    value match {
      case tagged: TaggedValue => {
        tagged.value match {
          case v: ValueMap => {
            w.writeStartElement(ctxName.getOrElse(tagged.tag))
            xmlnsOpt.foreach(xmlns => w.writeAttribute("xmlns", xmlns))
            v.value.foreach {
              case (keyV, valueV) =>
                keyV match {
                  case ValueString(name) =>
                    writeElem(valueV, w, Some(name))
                  case _ => throw new IllegalArgumentException(s"Tagged map did not have string key")
                }
            }
            w.writeEndElement()
          }
          case v => writeValue(v, w, ctxName.orElse(Some(tagged.tag)))
        }
      }
      case untagged: BasicValue => {
        writeValue(untagged, w, ctxName)
      }
    }
  }

  def writeSimple(typName: String, value: String, w: XMLStreamWriter, ctxName: Option[String] = None): Unit = {
    w.writeStartElement(ctxName.getOrElse(typName))
    w.writeCharacters(value)
    w.writeEndElement()
  }
}