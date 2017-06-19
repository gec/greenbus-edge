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
package io.greenbus.edge.edm.core.japi

import io.greenbus.edge.data.japi.impl.DataConversions
import io.greenbus.edge.edm.core.EdgeCoreModel
import io.greenbus.edge.japi.impl.Conversions

object JavaCoreModel {
  import io.greenbus.edge.api
  import io.greenbus.edge.japi
  import io.greenbus.edge.data

  def seriesTypeKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.seriesTypeKey)
  }
  def seriesTypeValue(seriesType: SeriesType): data.japi.Value = {
    val t = seriesType match {
      case SeriesType.AnalogSample => EdgeCoreModel.SeriesType.AnalogSample
      case SeriesType.AnalogStatus => EdgeCoreModel.SeriesType.AnalogStatus
      case SeriesType.CounterSample => EdgeCoreModel.SeriesType.CounterSample
      case SeriesType.CounterStatus => EdgeCoreModel.SeriesType.CounterStatus
      case SeriesType.BooleanStatus => EdgeCoreModel.SeriesType.BooleanStatus
      case SeriesType.IntegerEnum => EdgeCoreModel.SeriesType.IntegerEnum
    }
    DataConversions.convertValueToJava(EdgeCoreModel.seriesTypeValue(t))
  }

  def unitKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.unitKey)
  }
  def unitValue(unit: String): data.japi.Value = {
    DataConversions.convertValueToJava(EdgeCoreModel.unitValue(unit))
  }

  def integerLabelKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.integerLabelKey)
  }
  def integerLabelValue(jmap: java.util.Map[java.lang.Long, String]): data.japi.Value = {
    import scala.collection.JavaConverters._
    val smap = jmap.asScala.map {
      case (l, s) => (l: Long, s)
    }.toMap
    DataConversions.convertValueToJava(EdgeCoreModel.labeledIntegerValue(smap))
  }

  def labeledBooleanKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.booleanLabelKey)
  }
  def labeledBooleanValue(truthLabel: String, falseLabel: String): data.japi.Value = {
    DataConversions.convertValueToJava(EdgeCoreModel.labeledBooleanValue(truthLabel, falseLabel))
  }

  def analogDecimalPointsKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.analogDecimalPointsKey)
  }
  def analogDecimalPointsValue(decimalPoints: Int): data.japi.Value = {
    DataConversions.convertValueToJava(EdgeCoreModel.analogDecimalPointsValue(decimalPoints))
  }

  def outputTypeKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.outputTypeKey)
  }
  def outputTypeValue(outputType: OutputType): data.japi.Value = {
    val t = outputType match {
      case OutputType.SimpleIndication => EdgeCoreModel.OutputType.SimpleIndication
      case OutputType.BooleanSetpoint => EdgeCoreModel.OutputType.BooleanSetpoint
      case OutputType.AnalogSetpoint => EdgeCoreModel.OutputType.AnalogSetpoint
      case OutputType.EnumerationSetpoint => EdgeCoreModel.OutputType.EnumerationSetpoint
    }
    DataConversions.convertValueToJava(EdgeCoreModel.outputTypeValue(t))
  }

  def requestBooleanLabelsKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.requestBooleansLabelsKey)
  }
  def requestBooleanLabelsValue(truthLabel: String, falseLabel: String): data.japi.Value = {
    DataConversions.convertValueToJava(EdgeCoreModel.requestBooleanLabelsValue(truthLabel, falseLabel))
  }

  def requestIntegerLabelsKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.requestIntegerLabelsKey)
  }
  def requestIntegerLabelsValue(jmap: java.util.Map[java.lang.Long, String]): data.japi.Value = {
    import scala.collection.JavaConverters._
    val smap = jmap.asScala.map {
      case (l, s) => (l: Long, s)
    }.toMap
    DataConversions.convertValueToJava(EdgeCoreModel.requestIntegerLabelsValue(smap))
  }

  def requestScaleKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.requestScaleKey)
  }
  def requestScaleValue(scale: Double): data.japi.Value = {
    DataConversions.convertValueToJava(EdgeCoreModel.requestScaleValue(scale))
  }

  def requestOffsetKey(): japi.Path = {
    Conversions.convertPathToJava(EdgeCoreModel.requestOffsetKey)
  }
  def requestOffsetValue(scale: Double): data.japi.Value = {
    DataConversions.convertValueToJava(EdgeCoreModel.requestOffsetValue(scale))
  }
}
