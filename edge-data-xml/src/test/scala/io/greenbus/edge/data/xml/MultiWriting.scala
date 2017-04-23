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

import java.io.{ File, FileOutputStream, PrintWriter }

import com.google.common.io.Files
import io.greenbus.edge.data.schema._

object MultiWriting {

}

/*
object FepXmlSchemaWriter {

  def main(args: Array[String]): Unit = {

    val f = new File("testschemas/fep.xsd")
    Files.createParentDirs(f)
    if (!f.exists()) {
      f.createNewFile()
    }

    SchemaWriter.write(FrontendSchema.all, Seq(FrontendSchema.frontendConfiguration), "io.greenbus.edge.fep.config.model", new FileOutputStream(f))
  }
}

object DnpXmlSchemaWriter {

  def main(args: Array[String]): Unit = {

    val f = new File("testschemas/dnp.xsd")
    Files.createParentDirs(f)
    if (!f.exists()) {
      f.createNewFile()
    }

    SchemaWriter.write(DnpGatewaySchema.all ++ Schema.all, Seq(DnpGatewaySchema.gateway), "io.greenbus.edge.fep.dnp3.config.model", new FileOutputStream(f))
  }
}

object FrontendSchema {

  val sampleTypeEnum: TExt = {
    TExt("SampleType", TEnum(Seq(
      EnumDef("Float", 0),
      EnumDef("Double", 1),
      EnumDef("Int32", 2),
      EnumDef("UInt32", 3),
      EnumDef("Int64", 4),
      EnumDef("UInt64", 5),
      EnumDef("Bool", 6),
      EnumDef("Byte", 7))))
  }
  val seriesTypeEnum: TExt = {
    TExt("SeriesType", TEnum(Seq(
      EnumDef("AnalogStatus", 0),
      EnumDef("AnalogSample", 1),
      EnumDef("CounterStatus", 2),
      EnumDef("CounterSample", 3),
      EnumDef("BooleanStatus", 4),
      EnumDef("IntegerEnum", 5))))
  }
  val outputTypeEnum: TExt = {
    TExt("OutputType", TEnum(Seq(
      EnumDef("SimpleIndication", 0),
      EnumDef("AnalogSetpoint", 1),
      EnumDef("BooleanSetpoint", 2),
      EnumDef("EnumerationSetpoint", 3))))
  }

  val path: TExt = {
    TExt("Path", TList(TString))
  }

  val typeCast: TExt = {
    TExt("TypeCast", TStruct(Vector(
      StructFieldDef("target", sampleTypeEnum, 0))))
  }

  val linearTransform: TExt = {
    TExt("LinearTransform", TStruct(Vector(
      StructFieldDef("scale", TDouble, 0),
      StructFieldDef("offset", TDouble, 1))))
  }

  val simpleTransformTypeEnum: TExt = {
    TExt("SimpleTransformType", TEnum(Seq(
      EnumDef("Negate", 0))))
  }

  val simpleTransform: TExt = {
    TExt("SimpleTransform", TStruct(Vector(
      StructFieldDef("transformType", simpleTransformTypeEnum, 0))))
  }

  val integerLabel: TExt = {
    TExt("IntegerLabel", TStruct(Vector(
      StructFieldDef("value", TInt32, 0),
      StructFieldDef("label", TString, 1))))
  }

  val booleanLabels: TExt = {
    TExt("BooleanLabels", TStruct(Vector(
      StructFieldDef("trueLabel", TString, 0),
      StructFieldDef("falseLabel", TString, 1))))
  }

  val integerLabelSet: TExt = {
    TExt("IntegerLabelSet", TList(integerLabel))
  }

  val seriesDescriptor: TExt = {
    TExt("SeriesDescriptor", TStruct(Vector(
      StructFieldDef("seriesType", seriesTypeEnum, 0),
      StructFieldDef("unit", TOption(TString), 1),
      StructFieldDef("decimalPoints", TOption(TUInt32), 2),
      StructFieldDef("integerLabels", TOption(integerLabelSet), 3),
      StructFieldDef("booleanLabels", TOption(booleanLabels), 4))))
  }

  val transformDescriptor: TExt = {
    TExt("TransformDescriptor", TUnion(Set(typeCast, linearTransform, simpleTransform)))
  }

  val filterDescriptor: TExt = {
    TExt("FilterDescriptor", TStruct(Vector(
      StructFieldDef("suppressDuplicates", TOption(TBool), 0),
      StructFieldDef("deadband", TOption(TDouble), 1))))
  }

  val frontendDataKey: TExt = {
    TExt("DataKeyConfig", TStruct(Vector(
      StructFieldDef("gatewayKey", TString, 0),
      StructFieldDef("path", path, 1),
      StructFieldDef("descriptor", seriesDescriptor, 2),
      StructFieldDef("transforms", TList(transformDescriptor), 3),
      StructFieldDef("filter", TOption(filterDescriptor), 4))))
  }

  val outputDescriptor: TExt = {
    TExt("OutputDescriptor", TStruct(Vector(
      StructFieldDef("outputType", outputTypeEnum, 0),
      StructFieldDef("requestScale", TOption(TDouble), 1),
      StructFieldDef("requestOffset", TOption(TDouble), 2),
      StructFieldDef("requestIntegerLabels", TOption(integerLabelSet), 3),
      StructFieldDef("requestBooleanLabels", TOption(booleanLabels), 4))))
  }

  val frontendOutputKey: TExt = {
    TExt("OutputKeyConfig", TStruct(Vector(
      StructFieldDef("gatewayKey", TString, 0),
      StructFieldDef("path", path, 1),
      StructFieldDef("descriptor", outputDescriptor, 2))))
  }

  val frontendConfiguration: TExt = {
    TExt("FrontendConfiguration", TStruct(Vector(
      StructFieldDef("endpointId", path, 0),
      StructFieldDef("dataKeys", TList(frontendDataKey), 1),
      StructFieldDef("outputKeys", TList(frontendOutputKey), 2))))
  }

  val all = Seq(
    sampleTypeEnum,
    seriesTypeEnum,
    outputTypeEnum,
    path,
    typeCast,
    linearTransform,
    simpleTransformTypeEnum,
    simpleTransform,
    integerLabel,
    integerLabelSet,
    booleanLabels,
    seriesDescriptor,
    transformDescriptor,
    filterDescriptor,
    frontendDataKey,
    outputDescriptor,
    frontendOutputKey,
    frontendConfiguration)
}

object Schema {

  val linkLayer: TExt = {
    TExt("LinkLayer", TStruct(Vector(
      StructFieldDef("isMaster", TBool, 0),
      StructFieldDef("localAddress", TUInt32, 1),
      StructFieldDef("remoteAddress", TUInt32, 2),
      StructFieldDef("userConfirmations", TBool, 3),
      StructFieldDef("ackTimeoutMs", TUInt64, 4),
      StructFieldDef("numRetries", TUInt32, 5))))
  }
  val appLayer: TExt = {
    TExt("AppLayer", TStruct(Vector(
      StructFieldDef("timeoutMs", TUInt64, 0),
      StructFieldDef("maxFragSize", TUInt32, 1),
      StructFieldDef("numRetries", TUInt32, 2))))
  }
  val stackConfig: TExt = {
    TExt("StackConfig", TStruct(Vector(
      StructFieldDef("linkLayer", linkLayer, 0),
      StructFieldDef("appLayer", appLayer, 1))))
  }
  val masterSettings: TExt = {
    TExt("MasterSettings", TStruct(Vector(
      StructFieldDef("allowTimeSync", TBool, 0),
      StructFieldDef("taskRetryMs", TUInt64, 1),
      StructFieldDef("integrityPeriodMs", TUInt64, 2))))
  }
  val scan: TExt = {
    TExt("Scan", TStruct(Vector(
      StructFieldDef("enableClass1", TBool, 0),
      StructFieldDef("enableClass2", TBool, 1),
      StructFieldDef("enableClass3", TBool, 2),
      StructFieldDef("periodMs", TUInt64, 3))))
  }
  val unsol: TExt = {
    TExt("Unsol", TStruct(Vector(
      StructFieldDef("doTask", TBool, 0),
      StructFieldDef("enable", TBool, 1),
      StructFieldDef("enableClass1", TBool, 2),
      StructFieldDef("enableClass2", TBool, 3),
      StructFieldDef("enableClass3", TBool, 4))))
  }

  val master: TExt = {
    TExt("Master", TStruct(Vector(
      StructFieldDef("stack", stackConfig, 0),
      StructFieldDef("masterSettings", masterSettings, 1),
      StructFieldDef("scanList", TList(scan), 2),
      StructFieldDef("unsol", unsol, 3))))
  }

  def all = Seq(linkLayer, appLayer, stackConfig, masterSettings, scan, unsol, master)
}

object DnpGatewaySchema {

  val tcpClient: TExt = {
    TExt("TCPClient", TStruct(Vector(
      StructFieldDef("host", TString, 0),
      StructFieldDef("port", TUInt32, 1),
      StructFieldDef("retryMs", TUInt64, 2))))
  }
  val selectIndex: TExt = {
    TExt("IndexSelect", TUInt32)
  }
  val selectRange: TExt = {
    TExt("IndexRange", TStruct(Vector(
      StructFieldDef("start", TUInt32, 0),
      StructFieldDef("count", TUInt32, 1))))
  }
  val indexSet: TExt = {
    //TExt("IndexSet", TList(TUnion(Set(selectIndex, selectRange))))
    TExt("IndexSet", TList(selectRange))
  }

  val inputModel: TExt = {
    TExt("InputModel", TStruct(Vector(
      StructFieldDef("binaryInputs", indexSet, 0),
      StructFieldDef("analogInputs", indexSet, 1),
      StructFieldDef("counterInputs", indexSet, 2),
      StructFieldDef("binaryOutputs", indexSet, 3),
      StructFieldDef("analogOutputs", indexSet, 4))))
  }

  val controlTypeEnum: TExt = {
    TExt("ControlType", TEnum(Seq(
      EnumDef("PULSE", 0),
      EnumDef("PULSE_CLOSE", 1),
      EnumDef("PULSE_TRIP", 2),
      EnumDef("LATCH_ON", 3),
      EnumDef("LATCH_OFF", 4))))
  }

  val functionTypeEnum: TExt = {
    TExt("FunctionType", TEnum(Seq(
      EnumDef("SelectBeforeOperate", 0),
      EnumDef("DirectOperate", 1))))
  }

  val controlOptions: TExt = {
    TExt("ControlOptions", TStruct(Vector(
      StructFieldDef("controlType", controlTypeEnum, 0),
      StructFieldDef("onTime", TOption(TUInt32), 1),
      StructFieldDef("offTime", TOption(TUInt32), 2),
      StructFieldDef("count", TOption(TUInt32), 3))))
  }

  val controlItem: TExt = {
    TExt("Control", TStruct(Vector(
      StructFieldDef("name", TString, 0),
      StructFieldDef("index", TUInt32, 1),
      StructFieldDef("function", functionTypeEnum, 2),
      StructFieldDef("controlOptions", controlOptions, 3))))
  }
  val setpointItem: TExt = {
    TExt("Setpoint", TStruct(Vector(
      StructFieldDef("name", TString, 0),
      StructFieldDef("index", TUInt32, 1),
      StructFieldDef("function", functionTypeEnum, 2))))
  }

  val outputModel: TExt = {
    TExt("OutputModel", TStruct(Vector(
      StructFieldDef("controls", TList(controlItem), 0),
      StructFieldDef("setpoints", TList(setpointItem), 1))))
  }

  val gateway: TExt = {
    TExt("DNP3Gateway", TStruct(Vector(
      StructFieldDef("master", Schema.master, 0),
      StructFieldDef("client", tcpClient, 1),
      StructFieldDef("inputModel", inputModel, 2),
      StructFieldDef("outputModel", outputModel, 3))))
  }

  def all = Seq(
    tcpClient,
    selectIndex,
    selectRange,
    indexSet,
    inputModel,
    controlTypeEnum,
    functionTypeEnum,
    controlOptions,
    controlItem,
    setpointItem,
    outputModel,
    gateway)
}
*/ 