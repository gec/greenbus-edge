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
package io.greenbus.edge.api

import io.greenbus.edge.colset._

trait EdgeDataKeyCodec {
  def dataKeyToRow(endpointPath: EndpointPath): RowId
}

trait AppendDataKeyCodec extends EdgeDataKeyCodec {
  def fromTypeValue(v: TypeValue): Either[String, EdgeSequenceDataKeyValue]
}
trait KeyedSetDataKeyCodec extends EdgeDataKeyCodec {
  def fromTypeValue(v: KeyedSetUpdated): Either[String, ActiveSetUpdate]
}

trait EdgeCodec {
  def endpointIdToRow(id: EndpointId): RowId
  def fromTypeValue(v: TypeValue): Either[String, EndpointDescriptor]
}

object EdgeCodecCommon {

  def endpointIdToRow(id: EndpointId): RowId = ???

  def fromTypeValue(v: TypeValue): Either[String, EndpointDescriptor] = {
    ???
  }
}

object ColsetCodec {

  def encodeBoolSeries(obj: (Boolean, Long)): TypeValue = {
    TupleVal(Seq(BoolVal(obj._1), Int64Val(obj._2)))
  }
  def encodeLongSeries(obj: (Long, Long)): TypeValue = {
    TupleVal(Seq(Int64Val(obj._1), Int64Val(obj._2)))
  }
  def encodeDoubleSeries(obj: (Double, Long)): TypeValue = {
    TupleVal(Seq(DoubleVal(obj._1), Int64Val(obj._2)))
  }

  def encodeTopicEvent(obj: (Path, Value, Long)): TypeValue = {
    TupleVal(Seq(encodePath(obj._1), encodeValue(obj._2), Int64Val(obj._3)))
  }

  def encodePath(path: Path): TypeValue = {
    TupleVal(path.parts.map(s => SymbolVal(s)))
  }

  def encodeMap(map: Map[IndexableValue, Value]): Map[TypeValue, TypeValue] = {
    ???
  }

  def encodeValue(value: Value): TypeValue = {
    ???
  }
}
