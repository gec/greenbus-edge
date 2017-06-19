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
package io.greenbus.edge.edm.core

import io.greenbus.edge.api.Path
import io.greenbus.edge.data._

object EdgeCoreModel {

  val seriesTypeKey = Path(Seq("edm", "core", "series_type"))
  val unitKey = Path(Seq("edm", "core", "unit"))
  val integerLabelKey = Path(Seq("edm", "core", "integer_label"))
  val booleanLabelKey = Path(Seq("edm", "core", "boolean_label"))
  val analogDecimalPointsKey = Path(Seq("edm", "core", "decimal_points"))

  val outputTypeKey = Path(Seq("edm", "core", "output_type"))
  val requestBooleansLabelsKey = Path(Seq("edm", "core", "request_boolean_labels"))
  val requestIntegerLabelsKey = Path(Seq("edm", "core", "request_integer_labels"))
  val requestScaleKey = Path(Seq("edm", "core", "request_scale"))
  val requestOffsetKey = Path(Seq("edm", "core", "request_offset"))

  val assocDataKeysKey = Path(Seq("edm", "core", "assoc_data_keys"))

  sealed abstract class SeriesType(val value: String)
  object SeriesType {
    case object AnalogStatus extends SeriesType("analog_status")
    case object AnalogSample extends SeriesType("analog_sample")
    case object CounterStatus extends SeriesType("counter_status")
    case object CounterSample extends SeriesType("counter_sample")
    case object BooleanStatus extends SeriesType("boolean_status")
    case object IntegerEnum extends SeriesType("integer_enum")
  }

  def seriesTypeValue(seriesType: SeriesType): Value = {
    ValueString(seriesType.value)
  }
  def seriesType(seriesType: SeriesType): (Path, Value) = {
    (seriesTypeKey, seriesTypeValue(seriesType))
  }

  def unitValue(unit: String): Value = {
    ValueString(unit)
  }
  def unitMetadata(unit: String): (Path, Value) = {
    (unitKey, unitValue(unit))
  }

  def labeledBooleanValue(truthLabel: String, falseLabel: String): Value = {
    ValueMap(Map(
      ValueBool(true) -> ValueString(truthLabel),
      ValueBool(false) -> ValueString(falseLabel)))
  }
  def labeledBooleanMetadata(truthLabel: String, falseLabel: String): (Path, Value) = {
    (booleanLabelKey, labeledBooleanValue(truthLabel, falseLabel))
  }

  def labeledIntegerValue(map: Map[Long, String]): Value = {
    val vmap: Map[Value, Value] = map.map {
      case (k, v) => (ValueInt64(k), ValueString(v))
    }
    ValueMap(vmap)
  }
  def labeledIntegerMetadata(map: Map[Long, String]): (Path, Value) = {
    (integerLabelKey, labeledIntegerValue(map))
  }

  def analogDecimalPointsValue(decimalPoints: Int): Value = {
    ValueUInt32(decimalPoints)
  }
  def analogDecimalPoints(decimalPoints: Int): (Path, Value) = {
    (analogDecimalPointsKey, analogDecimalPointsValue(decimalPoints))
  }

  sealed abstract class OutputType(val value: String)
  object OutputType {
    case object SimpleIndication extends OutputType("simple_indication")
    case object AnalogSetpoint extends OutputType("analog_setpoint")
    case object BooleanSetpoint extends OutputType("boolean_setpoint")
    case object EnumerationSetpoint extends OutputType("enumeration_setpoint")
  }

  def outputTypeValue(outputType: OutputType): Value = {
    ValueString(outputType.value)
  }
  def outputType(outputType: OutputType): (Path, Value) = {
    (outputTypeKey, outputTypeValue(outputType))
  }

  def requestBooleanLabelsValue(truthLabel: String, falseLabel: String): Value = {
    ValueMap(Map(
      ValueBool(true) -> ValueString(truthLabel),
      ValueBool(false) -> ValueString(falseLabel)))
  }
  def requestBooleanLabels(truthLabel: String, falseLabel: String): (Path, Value) = {
    (requestBooleansLabelsKey, requestBooleanLabelsValue(truthLabel, falseLabel))
  }

  def requestIntegerLabelsValue(map: Map[Long, String]): Value = {
    val vmap: Map[Value, Value] = map.map {
      case (k, v) => (ValueInt64(k), ValueString(v))
    }
    ValueMap(vmap)
  }
  def requestIntegerLabels(map: Map[Long, String]): (Path, Value) = {
    (requestIntegerLabelsKey, requestIntegerLabelsValue(map))
  }

  def requestScaleValue(scale: Double): Value = {
    ValueDouble(scale)
  }
  def requestScale(scale: Double): (Path, Value) = {
    (requestScaleKey, requestScaleValue(scale))
  }

  def requestOffsetValue(offset: Double): Value = {
    ValueDouble(offset)
  }
  def requestOffset(offset: Double): (Path, Value) = {
    (requestOffsetKey, requestOffsetValue(offset))
  }
}