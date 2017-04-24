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

import java.io.InputStream
import javax.xml.stream.XMLInputFactory

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.data._
import io.greenbus.edge.data.schema._

import scala.collection.mutable

object Node extends LazyLogging {

  def tryParse[A](value: String, f: String => A): Option[A] = {
    try {
      Some(f(value))
    } catch {
      case ex: Throwable =>
        logger.debug(s"Problem parsing value: $ex")
        None
    }
  }

  def termParse[A](parse: String => A, build: A => BasicValue): String => Option[BasicValue] = {
    s: String => { tryParse[A](s, parse).map(build) }
  }
  def term[A](parse: String => A, build: A => BasicValue, tagOpt: Option[String]): Node = {
    new TerminalNode(termParse(parse, build), tagOpt)
  }

  def simpleNodeFor(typ: VTValueElem, tagOpt: Option[String], elemName: String): Node = {
    typ match {
      case TByte => term(java.lang.Byte.parseByte, ValueByte, tagOpt)
      case TBool => term(java.lang.Boolean.parseBoolean, ValueBool, tagOpt)
      case TInt32 => term(java.lang.Integer.parseInt, ValueInt32, tagOpt)
      case TUInt32 => term(java.lang.Long.parseLong, ValueUInt32, tagOpt)
      case TInt64 => term(java.lang.Long.parseLong, ValueInt64, tagOpt)
      case TUInt64 => term(java.lang.Long.parseLong, ValueUInt64, tagOpt)
      case TFloat => term(java.lang.Float.parseFloat, ValueFloat, tagOpt)
      case TDouble => term(java.lang.Double.parseDouble, ValueDouble, tagOpt)
      case TString => term(s => s, ValueString, tagOpt)
      case t: TList => new ListNode(t, tagOpt)
      case t: TOption => nodeFor(t.paramType, elemName)
      case t: TEnum => term(s => s, ValueString, tagOpt)
      case t: TUnion => {
        val typeTags = t.unionTypes.flatMap {
          case ext: TExt => Some(ext.tag -> ext)
          case _ => None
        }.toMap

        typeTags.get(elemName).map(ext => nodeFor(ext, elemName)).getOrElse(new NullNode)
      }
      case _ => throw new IllegalArgumentException(s"Type unhandled: " + typ + " for: " + tagOpt)
    }
  }

  def nodeFor(typ: VTValueElem, elemName: String): Node = {
    typ match {
      case t: TExt => {
        t.reprType match {
          case extType: TStruct => new StructNode(t.tag, extType)
          case other => simpleNodeFor(other, Some(t.tag), elemName: String)
        }
      }
      case t => simpleNodeFor(typ, None, elemName: String)
    }
  }

}
trait Node {
  def setText(content: String): Unit
  def onPop(subElemOpt: Option[Value]): Unit
  def onPush(name: String): Node
  def result(): Option[Value]
}

class NullNode extends Node {
  def setText(content: String): Unit = {}

  def onPush(name: String): Node = new NullNode
  def onPop(subElemOpt: Option[Value]): Unit = {}

  def result(): Option[Value] = {
    None
  }
}

class TerminalNode(parser: String => Option[BasicValue], tagOpt: Option[String]) extends Node {

  private var textOpt = Option.empty[String]

  def setText(content: String): Unit = {
    textOpt = Some(content)
  }

  def onPush(name: String): Node = new NullNode
  def onPop(subElemOpt: Option[Value]): Unit = {}

  def result(): Option[Value] = {
    textOpt.flatMap(parser).map { elem =>
      tagOpt match {
        case None => elem
        case Some(tag) => TaggedValue(tag, elem)
      }
    }
  }
  override def toString: String = {
    s"Term($tagOpt)"
  }
}

class ListNode(typ: TList, tagOpt: Option[String]) extends Node {
  private val elems = mutable.ArrayBuffer.empty[Value]

  def setText(content: String): Unit = {}

  def onPush(name: String): Node = {
    Node.nodeFor(typ.paramType, name)
  }

  def onPop(subElemOpt: Option[Value]): Unit = {
    subElemOpt.foreach { elems += _ }
  }

  def result(): Option[Value] = {
    tagOpt match {
      case None => Some(ValueList(elems.toVector))
      case Some(tag) => Some(TaggedValue(tag, ValueList(elems.toVector)))
    }
  }
  override def toString: String = {
    s"List($tagOpt)"
  }
}

class MapNode(typ: TMap) extends Node {
  def setText(content: String): Unit = ???

  def onPop(subElemOpt: Option[Value]): Unit = ???

  def onPush(name: String): Node = ???

  def result(): Option[Value] = ???
}

object StructNode {
  def fieldIsOptional(sfd: StructFieldDef): Boolean = {
    sfd.typ match {
      case _: TOption => true
      case _: TList => true
      case _: TMap => true
      case _ => false
    }
  }
}
class StructNode(typeTag: String, vt: TStruct) extends Node {
  private val fieldMap: Map[String, StructFieldDef] = vt.fields.map { sfd =>
    /*val name = sfd.typ match {
      case t: TExt => t.tag
      case _ => sfd.name
    }*/
    (sfd.name, sfd)
  }.toMap

  private var builtFields = Map.empty[String, Value]

  private var currentField = Option.empty[String]

  def setText(content: String): Unit = {

  }

  def onPush(name: String): Node = {
    fieldMap.get(name) match {
      case None =>
        new NullNode
      case Some(sfd) => {
        println("Struct push: " + name)
        currentField = Some(sfd.name)
        Node.nodeFor(sfd.typ, name)
      }
    }
  }

  def onPop(subElemOpt: Option[Value]): Unit = {
    subElemOpt.foreach { subElem =>
      currentField.foreach { fieldName =>
        builtFields += (fieldName -> subElem)
      }
    }
    currentField = None
  }

  def result(): Option[Value] = {
    val kvs: Seq[(ValueString, Value)] = vt.fields.flatMap { sfd =>
      builtFields.get(sfd.name) match {
        case None =>
          if (!StructNode.fieldIsOptional(sfd)) {
            throw new IllegalArgumentException(s"Could not find required field ${sfd.name} for $typeTag")
          } else {
            None
          }
        case Some(elem) => {
          Some((ValueString(sfd.name), elem))
        }
      }
    }

    Some(TaggedValue(typeTag, ValueMap(kvs.toMap)))
  }

  override def toString: String = {
    s"Struct($typeTag)"
  }
}

class RootNode(rootType: VTValueElem) extends Node {

  private var resultOpt = Option.empty[Value]
  private var pushed = false
  private var popped = false

  def setText(content: String): Unit = {}

  def onPush(name: String): Node = {
    if (!pushed) {
      pushed = true
      Node.nodeFor(rootType, name)
    } else {
      new NullNode
    }
  }

  def onPop(subElemOpt: Option[Value]): Unit = {
    if (!popped) {
      popped = true
      resultOpt = subElemOpt
    }
  }

  def result(): Option[Value] = {
    resultOpt
  }

  override def toString: String = "Root"
}

object XmlReader {

  class ResultBuilder(name: String, attributes: Seq[(String, String)], var text: Option[String], sub: mutable.ArrayBuffer[Value])

  def read(is: InputStream, rootType: VTValueElem): Option[Value] = {
    val fac = XMLInputFactory.newInstance()
    val reader = fac.createXMLEventReader(is)

    var nodeStack = List.empty[Node]

    val root = new RootNode(rootType)
    nodeStack ::= root

    while (reader.hasNext) {
      println(nodeStack)
      val event = reader.nextEvent()

      if (event.isStartElement) {

        val elem = event.asStartElement()

        nodeStack.headOption.foreach { head =>
          val pushed = head.onPush(elem.getName.getLocalPart)
          nodeStack ::= pushed
        }

      } else if (event.isCharacters) {

        val trimmed = event.asCharacters().getData.trim
        if (trimmed.nonEmpty) {
          nodeStack.headOption.foreach { head =>
            head.setText(trimmed)
          }
        }

      } else if (event.isEndElement) {

        if (nodeStack.nonEmpty) {
          val current = nodeStack.head
          nodeStack = nodeStack.tail

          if (nodeStack.nonEmpty) {
            val prev = nodeStack.head
            prev.onPop(current.result())
          } else {
          }
        }
      }

    }

    root.result()
  }
}
