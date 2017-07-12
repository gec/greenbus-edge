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
package io.greenbus.edge.data.doc

import java.io.{ OutputStream, PrintWriter }

import io.greenbus.edge.data.schema._

object DocGen {

  def generate(userTypes: Seq[TExt], os: OutputStream): Unit = {
    val w = new PrintWriter(os)
    val sorted = userTypes.sortBy(_.tag)

    sorted.foreach(t => typeEntry(w, t))

    w.flush()
  }

  def typeEntry(w: PrintWriter, typ: TExt): Unit = {
    typ.reprType match {
      case t: TStruct => structEntry(w, typ.tag, t, typ.doc)
      case t: TEnum => enumEntry(w, typ.tag, t, typ.doc)
      case t: TUnion => unionEntry(w, typ.tag, t, typ.doc)
      case t: BasicValueType => aliasEntry(w, typ.tag, t)
      case _ =>
    }
  }

  def unionEntry(w: PrintWriter, tag: String, t: TUnion, doc: String): Unit = {
    w.println()
    w.println(s"### $tag")
    if (doc.trim.length > 0) {
      w.println()
      w.println(doc)
    }
    w.println()
    w.println(s"Union type: ")
    w.println()
    t.unionTypes.toSeq.map(fieldDefName).sorted.foreach { typ =>
      w.println(s" * $typ")
    }
    w.println()
  }

  def aliasEntry(w: PrintWriter, tag: String, t: BasicValueType): Unit = {
    w.println()
    w.println(s"### $tag")
    w.println()
    w.println(s"Alias: ${fieldDefName(t)}")
    w.println()
  }

  def enumEntry(w: PrintWriter, tag: String, enum: TEnum, doc: String): Unit = {

    w.println()
    w.println(s"### $tag")
    if (doc.trim.length > 0) {
      w.println()
      w.println(doc)
    }
    w.println()
    w.println("Values:")
    w.println()

    val table = enum.enumDefs.map { d =>
      Vector(s"`${d.label}`", d.value.toString, d.doc)
    }

    printTable(w, Vector("Label", "Value", "Description"), table)
    w.println()
  }

  def structEntry(w: PrintWriter, tag: String, struct: TStruct, doc: String): Unit = {

    w.println()
    w.println(s"### $tag")
    if (doc.trim.length > 0) {
      w.println()
      w.println(doc)
    }
    w.println()
    w.println("Fields:")
    w.println()

    val table = struct.fields.map { s =>
      Vector(s"`${s.name}`", fieldDefName(s.typ), s.doc)
    }

    printTable(w, Vector("Name", "Type", "Description"), table)
    w.println()
  }

  def fieldDefName(fieldType: VTValueElem): String = {
    fieldType match {
      case ext: TExt => ext.tag
      case opt: TOption => fieldDefName(opt.paramType) + " (optional)"
      case list: TList => s"List[${fieldDefName(list.paramType)}]"
      case map: TMap => s"Map[${fieldDefName(map.keyType)}, ${fieldDefName(map.valueType)}]"
      case TByte => "byte"
      case TBool => "boolean"
      case TInt32 => "int32"
      case TInt64 => "int64"
      case TUInt32 => "uint32"
      case TUInt64 => "uint64"
      case TFloat => "float"
      case TDouble => "double"
      case TString => "string"
      case _ => "unknown"
    }
  }

  def printTable(w: PrintWriter, headers: Vector[String], rows: Seq[Vector[String]]): Unit = {
    val tableWidth = headers.size

    val widths: Array[Int] = Array.ofDim[Int](tableWidth)
    Range(0, tableWidth).foreach { i => widths.update(i, 0) }

    (Seq(headers) ++ rows).foreach { row =>
      row.zipWithIndex.take(tableWidth).foreach {
        case (v, i) => widths.update(i, Math.max(v.length, widths(i)))
      }
    }

    def printRow(row: Vector[String]): Unit = {

      val paddedCols = row.zipWithIndex.map {
        case (col, i) =>
          val colSize = widths(i)
          col.padTo(colSize, " ").mkString
      }

      w.println(paddedCols.mkString("| ", " | ", " |"))
    }

    printRow(headers)
    w.println(widths.map(count => stringOf(count + 2, "-")).mkString("|", "|", "|"))
    rows.foreach(printRow)
  }

  def stringOf(n: Int, part: String): String = {
    val b = StringBuilder.newBuilder
    Range(0, n).foreach(_ => b.append(part))
    b.mkString
  }
}
