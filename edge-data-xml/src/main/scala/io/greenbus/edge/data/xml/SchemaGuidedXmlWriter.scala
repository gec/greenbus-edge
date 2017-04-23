package io.greenbus.edge.data.xml

import java.io.OutputStream
import javax.xml.stream.{XMLOutputFactory, XMLStreamWriter}

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter
import io.greenbus.edge.data._
import io.greenbus.edge.data.schema._

object SchemaGuidedXmlWriter {

  def xmlNamespace(ns: TypeNamespace): Option[String] = {
    ns.options.get("xmlns")
  }

  //case class NsContext(prefixOpt: String,)

  def write(value: Value, schema: TExt, os: OutputStream, xmlnsOpt: Option[String] = None): Unit = {

    val output = XMLOutputFactory.newFactory()
    val base = output.createXMLStreamWriter(os)
    val w = new IndentingXMLStreamWriter(base)
    w.writeStartDocument("UTF-8", "1.0")

    writeStruct(value, schema, w, None, isRoot = true)

    w.writeEndDocument()
    w.flush()
  }

  def writeEnum(value: Value, typ: TEnum, w: XMLStreamWriter, ctxName: String): Unit = {
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

    w.writeStartElement(ctxName)
    w.writeCharacters(enumValue)
    w.writeEndElement()

  }

  def writeTypedValue(value: Value, typ: VTValueElem, w: XMLStreamWriter, ctxName: Option[String] = None): Unit = {

    def simpleMismatch(typ: String) = throw new IllegalArgumentException(s"Boolean value did not have boolean representation: $value")

    typ match {
      case t: TExt => writeStruct(value, t, w, ctxName)
      case t: TList => {
        value match {
          case v: ValueList => {
            w.writeStartElement(ctxName.getOrElse("list"))
            v.value.foreach(item => writeTypedValue(item, t.paramType, w, None))
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
        writeEnum(value, t, w, name)
      }
      case t: TOption => {
        value match {
          case ValueNone =>
          case v => writeTypedValue(v, t.paramType, w, ctxName)
        }
      }
      case TBool => {
        value match {
          case v: ValueBool => writeSimple("bool", v.value.toString, w, ctxName)
          case _ => simpleMismatch("bool")
        }
      }
      case TByte => {
        value match {
          case v: ValueByte => writeSimple("byte", v.value.toString, w, ctxName)
          case _ => simpleMismatch("byte")
        }
      }
      case TInt32 => {
        value match {
          case v: ValueInt32 => writeSimple("int32", v.value.toString, w, ctxName)
          case _ => simpleMismatch("int32")
        }
      }
      case TUInt32 => {
        value match {
          case v: ValueUInt32 => writeSimple("uint32", v.value.toString, w, ctxName)
          case _ => simpleMismatch("uint32")
        }
      }
      case TInt64 => {
        value match {
          case v: ValueInt64 => writeSimple("int64", v.value.toString, w, ctxName)
          case _ => simpleMismatch("int64")
        }
      }
      case TUInt64 => {
        value match {
          case v: ValueUInt64 => writeSimple("uint64", v.value.toString, w, ctxName)
          case _ => simpleMismatch("uint64")
        }
      }
      case TFloat => {
        value match {
          case v: ValueFloat => writeSimple("float", v.value.toString, w, ctxName)
          case _ => simpleMismatch("float")
        }
      }
      case TDouble => {
        value match {
          case v: ValueDouble => writeSimple("double", v.value.toString, w, ctxName)
          case _ => simpleMismatch("double")
        }
      }
      case TString => {
        value match {
          case v: ValueString => writeSimple("string", v.value.toString, w, ctxName)
          case _ => simpleMismatch("string")
        }
      }
      case other => throw new IllegalArgumentException(s"Unhandled type: " + typ)
    }
  }

  def writeStruct(value: Value, typ: TExt, w: XMLStreamWriter, ctxName: Option[String] = None, isRoot: Boolean = false): Unit = {
    //println(value.getClass.getSimpleName + " : " + ctxName)

    val xmlnsOpt = xmlNamespace(typ.ns)

    val (tagOpt, repr) = value match {
      case TaggedValue(tag, v) => (Some(tag), v)
      case v => (None, v)
    }

    System.err.println("------")
    System.err.println(value)
    System.err.println(typ)
    typ.reprType match {
      case t: TStruct => {
        repr match {
          case v: ValueMap => {
            w.writeStartElement(ctxName.getOrElse(typ.tag))
            if (isRoot) {
              xmlnsOpt.foreach(xmlns => w.writeAttribute("xmlns", xmlns))
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
                writeTypedValue(fieldValue, sfd.typ, w, Some(sfd.name))
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
            w.writeStartElement(ctxName.getOrElse(typ.tag))
            if (isRoot) {
              xmlnsOpt.foreach(xmlns => w.writeAttribute("xmlns", xmlns))
            }
            v.value.foreach { itemValue =>
              writeTypedValue(itemValue, t.paramType, w, None)
            }
            w.writeEndElement()
          }
          case other => throw new IllegalArgumentException(s"Representation of ${typ.tag} did not match type List")
        }
      }
      case t: TEnum => {
        writeEnum(repr, t, w, ctxName.getOrElse(typ.tag))
      }
      case t: TUnion => {
        val tagMap = t.unionTypes.map {
          case ext: TExt => ext.tag -> ext
          case _ => throw new IllegalArgumentException(s"Union types must only include user typed values (${typ.tag})")
        }.toMap

        tagOpt match {
          case None => throw new IllegalArgumentException(s"Values of union types must be tagged (${typ.tag})")
          case Some(tag) =>
            tagMap.get(tag) match {
              case None => throw new IllegalArgumentException(s"Tag on value: $tag was not part of union ${typ.tag}")
              case Some(tagType) => {
                writeTypedValue(repr, tagType, w, None)
              }
            }
        }
        /*repr match {
          case tagged: TaggedValue => {
            tagMap.get(tagged.tag) match {
              case None => throw new IllegalArgumentException(s"Tag on value: ${tagged.tag} was not part of union ${typ.tag}")
              case Some(tagType) => {
                writeTypedValue()
              }
            }
          }
          case _ => throw new IllegalArgumentException(s"Values of union types must be tagged (${typ.tag})")
        }*/

      }
      case other => System.err.println("UNHANDLED: " + other)
    }
  }

  def writeSimple(typName: String, value: String, w: XMLStreamWriter, ctxName: Option[String] = None): Unit = {
    w.writeStartElement(ctxName.getOrElse("value"))
    w.writeCharacters(value)
    w.writeEndElement()
  }

}
