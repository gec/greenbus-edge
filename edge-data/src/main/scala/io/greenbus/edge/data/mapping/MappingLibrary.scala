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
package io.greenbus.edge.data.mapping

import io.greenbus.edge.data.schema.{ StructFieldDef, TStruct }
import io.greenbus.edge.data._
import io.greenbus.edge.util.EitherUtil

object MappingLibrary {

  def descName(v: Value): String = {
    v.getClass.getSimpleName
  }

  def toFieldMap(map: ValueMap, typ: TStruct): Map[String, Value] = {

    val nameMap: Map[String, StructFieldDef] = {
      typ.fields.map(sfd => (sfd.name, sfd)).toMap
    }

    map.value.flatMap {
      case (keyElem, valueElem) =>
        val nameOpt = keyElem match {
          case ValueString(key) => nameMap.get(key)
          case _ => None
        }

        nameOpt.map(fd => fd.name -> valueElem)
    }
  }

  def getMapField(name: String, map: ValueMap): Either[String, Value] = {
    map.value.get(ValueString(name)).map(r => Right(r)).getOrElse(Left(s"Struct map did not contain field $name"))
  }

  def readFieldSubStruct[A](fieldName: String, element: Value, tag: String, read: (Value, ReaderContext) => Either[String, A], ctx: ReaderContext): Either[String, A] = {

    element match {
      case v: TaggedValue => read(v.value, ctx.structField(tag, fieldName))
      case other => read(other, ctx.structField(tag, fieldName))
    }
  }

  def readBool(elem: Value, ctx: ReaderContext): Either[String, Boolean] = {
    elem match {
      case v: ValueBool => Right(v.value)
      case _ => Left(s"${ctx.context} error: expected boolean value, saw: ${descName(elem)}")
    }
  }
  def readByte(elem: Value, ctx: ReaderContext): Either[String, Byte] = {
    elem match {
      case v: ValueByte => Right(v.value)
      case _ => Left(s"${ctx.context} error: expected byte value, saw: ${descName(elem)}")
    }
  }
  def readInt(elem: Value, ctx: ReaderContext): Either[String, Int] = {
    elem match {
      case v: IntegerValue => Right(v.toInt)
      case _ => Left(s"${ctx.context} error: expected integer value, saw: ${descName(elem)}")
    }
  }
  def readLong(elem: Value, ctx: ReaderContext): Either[String, Long] = {
    elem match {
      case v: IntegerValue => Right(v.toLong)
      case _ => Left(s"${ctx.context} error: expected long value, saw: ${descName(elem)}")
    }
  }
  def readFloat(elem: Value, ctx: ReaderContext): Either[String, Float] = {
    elem match {
      case v: FloatingPointValue => Right(v.toFloat)
      case _ => Left(s"${ctx.context} error: expected float value, saw: ${descName(elem)}")
    }
  }
  def readDouble(elem: Value, ctx: ReaderContext): Either[String, Double] = {
    elem match {
      case v: FloatingPointValue => Right(v.toDouble)
      case _ => Left(s"${ctx.context} error: expected double value, saw: ${descName(elem)}")
    }
  }
  def readString(elem: Value, ctx: ReaderContext): Either[String, String] = {
    elem match {
      case v: ValueString => Right(v.value)
      case _ => Left(s"${ctx.context} error: expected string value, saw: ${descName(elem)}")
    }
  }

  def readList[A](elem: Value, readContained: (Value, ReaderContext) => Either[String, A], ctx: ReaderContext): Either[String, Seq[A]] = {
    elem match {
      case v: ValueList =>
        EitherUtil.rightSequence(v.value.map(readContained(_, ctx)))
      case _ => Left(s"${ctx.context} error: expected string value, saw: ${descName(elem)}")
    }
  }

  def readTup[A](elem: Value, ctx: ReaderContext, read: (Value, ReaderContext) => Either[String, A]): Either[String, A] = {
    elem match {
      case v: TaggedValue => readTup(v.value, ctx, read)
      case v: ValueMap => read(v, ctx)
      case _ => Left(s"${ctx.context} error: expected tuple value, saw ${descName(elem)}")
    }
  }

  def writeList[A](list: Seq[A], write: A => Value): ValueList = {
    ValueList(list.toIndexedSeq.map(write))
  }
}