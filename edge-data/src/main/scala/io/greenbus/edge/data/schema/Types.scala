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
package io.greenbus.edge.data.schema

case class TypeNamespace(name: String, options: Map[String, String])

sealed trait ValueType
case class VTField(fieldName: String, fieldType: VTValueElem) extends ValueType

sealed trait VTValueElem extends ValueType
case class TExt(ns: TypeNamespace, tag: String, reprType: BasicValueType, doc: String = "") extends VTValueElem

sealed trait BasicValueType extends VTValueElem
sealed trait PrimitiveValueType extends BasicValueType

case object TByte extends PrimitiveValueType
case object TBool extends PrimitiveValueType
case object TInt32 extends PrimitiveValueType
case object TInt64 extends PrimitiveValueType
case object TUInt32 extends PrimitiveValueType
case object TUInt64 extends PrimitiveValueType
case object TFloat extends PrimitiveValueType
case object TDouble extends PrimitiveValueType

case object TString extends BasicValueType

//case object TAbstract extends BasicValueType

case class TUnion(unionTypes: Set[VTValueElem]) extends BasicValueType
case class TOption(paramType: VTValueElem) extends BasicValueType
case class TEither(leftType: VTValueElem, rightType: VTValueElem) extends BasicValueType

case class StructFieldDef(name: String, typ: VTValueElem, number: Int, doc: String = "")
case class TStruct(fields: Seq[StructFieldDef]) extends BasicValueType
case class TList(paramType: VTValueElem) extends BasicValueType
case class TMap(keyType: VTValueElem, valueType: VTValueElem) extends BasicValueType

case class EnumDef(label: String, value: Int, doc: String = "")
case class TEnum(enumDefs: Seq[EnumDef]) extends BasicValueType