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

import java.io._

/*
import com.google.common.io.Files
import io.greenbus.edge.data.mapping.{RootCtx, SimpleReaderContext}
import io.greenbus.edge.fep.config.model._
import io.greenbus.edge.fep.dnp3.config.model._

object XmlWriterTester {

  def main(args: Array[String]): Unit = {
    runGateway()
  }

  def runGateway(): Unit = {

    val example = PostGen.buildGateway
    val obj = DNP3Gateway.write(example)

    println(obj)
    //val stringOut = new ByteArrayOutputStream()

    val filename = "testschemas/dnp.xml"

    val f = new File(filename)
    Files.createParentDirs(f)
    if (!f.exists()) {
      f.createNewFile()
    }

    val stringOut = new FileOutputStream(f)
    //XmlWriter.write(obj, stringOut, Some("io.greenbus.edge.fep.dnp3.config.model"))

    val xmlNs = XmlNamespaceInfo(DnpGatewaySchema.gateway.ns.name,
      Map(
        (DnpGatewaySchema.ns.name, XmlNsDecl("dnp3", DnpGatewaySchema.ns.name)),
        (FrontendSchema.ns.name, XmlNsDecl("fep", FrontendSchema.ns.name))))

    SchemaGuidedXmlWriter.write(obj, DnpGatewaySchema.gateway, stringOut, xmlNs)
    stringOut.close()

    val stringIn = new FileInputStream(f)

    val readOpt = XmlReader.read(stringIn, DnpGatewaySchema.gateway)

    val xmlRead = readOpt.get
    println(obj)
    println(xmlRead)

    val theyMatch = obj == xmlRead
    println("match? " + theyMatch)

    val result = DNP3Gateway.read(xmlRead, SimpleReaderContext(Vector(RootCtx("DNP3Gateway"))))

    println(result.right.get)
    println(example)
    println(result.right.get == example)
  }
}

object PostGen {

  def buildMaster: Master = {
    Master(
      StackConfig(
        LinkLayer(isMaster = true, localAddress = 100, remoteAddress = 1, userConfirmations = false, ackTimeoutMs = 1000, numRetries = 3),
        AppLayer(timeoutMs = 5000, maxFragSize = 2048, numRetries = 0)),
      MasterSettings(allowTimeSync = true, integrityPeriodMs = 300000, taskRetryMs = 5000),
      Seq(Scan(
        enableClass1 = true,
        enableClass2 = true,
        enableClass3 = true,
        periodMs = 2000)),
      Unsol(doTask = true, enable = true, enableClass1 = true, enableClass2 = true, enableClass3 = true))
  }

  def buildGateway: DNP3Gateway = {

    DNP3Gateway(buildMaster,
      TCPClient("127.0.0.1", 20000, 5000),
      InputModel(
        binaryInputs = IndexSet(Seq(IndexRange(0, 10))),
        analogInputs = IndexSet(Seq(IndexRange(0, 10))),
        counterInputs = IndexSet(Seq(IndexRange(0, 10))),
        binaryOutputs = IndexSet(Seq(IndexRange(0, 10))),
        analogOutputs = IndexSet(Seq(IndexRange(0, 10)))),
      OutputModel(
        controls = Seq(Control(
          "control_0",
          index = 0,
          function = FunctionType.SelectBeforeOperate,
          controlOptions = ControlOptions(
            controlType = ControlType.PULSE,
            onTime = Some(100),
            offTime = Some(100),
            count = Some(1))), Control(
          "control_1_on",
          index = 1,
          function = FunctionType.SelectBeforeOperate,
          controlOptions = ControlOptions(
            controlType = ControlType.LATCH_ON,
            onTime = None,
            offTime = None,
            count = None)), Control(
          "control_1_off",
          index = 1,
          function = FunctionType.SelectBeforeOperate,
          controlOptions = ControlOptions(
            controlType = ControlType.LATCH_OFF,
            onTime = None,
            offTime = None,
            count = None))),
        setpoints = Seq(Setpoint(
          "setpoint_0",
          index = 0,
          function = FunctionType.SelectBeforeOperate),
          Setpoint(
            "setpoint_1",
            index = 1,
            function = FunctionType.SelectBeforeOperate))),
      buildFep)
  }

  def buildFep: FrontendConfiguration = {
    FrontendConfiguration(
      endpointId = Path(Seq("mthy", "mgrid", "ess01")),
      dataKeys = Seq(
        DataKeyConfig(
          gatewayKey = "analog_0",
          path = Path(Seq("outputPower")),
          descriptor = SeriesDescriptor(
            seriesType = SeriesType.AnalogStatus,
            unit = Some("kW"),
            decimalPoints = Some(2),
            integerLabels = None,
            booleanLabels = None),
          transforms = Seq(
            LinearTransform(2.0, 5.0)),
          filter = Some(FilterDescriptor(suppressDuplicates = None, deadband = Some(0.01)))),
        DataKeyConfig(
          gatewayKey = "analog_1",
          path = Path(Seq("mode")),
          descriptor = SeriesDescriptor(
            seriesType = SeriesType.IntegerEnum,
            unit = None,
            decimalPoints = None,
            integerLabels = Some(IntegerLabelSet(Seq(
              IntegerLabel(0, "Constant"),
              IntegerLabel(1, "Smoothing"),
              IntegerLabel(2, "GridForming")))),
            booleanLabels = None),
          transforms = Seq(),
          filter = None),
        DataKeyConfig(
          gatewayKey = "binary_0",
          path = Path(Seq("faultStatus")),
          descriptor = SeriesDescriptor(
            seriesType = SeriesType.BooleanStatus,
            unit = None,
            decimalPoints = None,
            integerLabels = None,
            booleanLabels = Some(BooleanLabels(trueLabel = "Fault", falseLabel = "Normal"))),
          transforms = Seq(),
          filter = None)),
      outputKeys = Seq(
        OutputKeyConfig(
          gatewayKey = "control_0",
          path = Path(Seq("Clear")),
          descriptor = OutputDescriptor(
            OutputType.AnalogSetpoint,
            requestScale = Some(100),
            requestOffset = None,
            requestIntegerLabels = None,
            requestBooleanLabels = None)),
        OutputKeyConfig(
          gatewayKey = "setpoint_1",
          path = Path(Seq("SetMode")),
          descriptor = OutputDescriptor(
            OutputType.EnumerationSetpoint,
            requestScale = None,
            requestOffset = None,
            requestIntegerLabels = Some(IntegerLabelSet(Seq(
              IntegerLabel(0, "Constant"),
              IntegerLabel(1, "Smoothing"),
              IntegerLabel(2, "GridForming")))),
            requestBooleanLabels = None))))
  }
}
*/
