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
package io.greenbus.edge.data.codegen

import java.io.{ File, FileOutputStream, PrintWriter }

import com.google.common.io.Files
import io.greenbus.edge.data.{ TaggedValue, ValueMap }
import io.greenbus.edge.data.mapping.{ RootCtx, SimpleReaderContext }
import io.greenbus.edge.data.schema._

/*
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

  val outputModel: TExt = {
    TExt("OutputModel", TStruct(Vector(
      StructFieldDef("binaries", indexSet, 0),
      StructFieldDef("setpoints", indexSet, 1))))
  }

  val gateway: TExt = {
    TExt("DNP3Gateway", TStruct(Vector(
      StructFieldDef("master", Schema.master, 0),
      StructFieldDef("client", tcpClient, 1),
      StructFieldDef("inputModel", inputModel, 2),
      StructFieldDef("outputModel", outputModel, 3))))
  }

  def all = Seq(tcpClient, selectIndex, selectRange, indexSet, inputModel, outputModel, gateway)
}

 */
object OptionTest {

  val typeCast = TExt("TypeCast", TStruct(Vector(
    StructFieldDef("target", TString, 0))))

  val linearTransform = TExt("LinearTransform", TStruct(Vector(
    StructFieldDef("scale", TDouble, 0),
    StructFieldDef("offset", TDouble, 1))))

  val transformDescriptor = TExt("TransformDescriptor", TUnion(Set(typeCast, linearTransform)))

  val frontendDataKey = TExt("FrontendDataKey", TStruct(Vector(
    StructFieldDef("gatewayKey", TString, 0),
    StructFieldDef("transforms", transformDescriptor, 1))))

  def main(args: Array[String]): Unit = {
    val all = Gen.collectObjDefs(frontendDataKey, Map())

    println(all)

    val f = new File("edge-data/target/generated-sources/scala/io/greenbus/edge/data/codegen/test/Model.scala")
    Files.createParentDirs(f)
    if (!f.exists()) {
      f.createNewFile()
    }

    //val fw = new PrintWriter("dnp3-gateway/fakesrc/testfile.scala" /*new FileOutputStream(new File("testdir/testfile.scala"))*/ )
    val fw = new PrintWriter(new FileOutputStream(f))
    Gen.output("io.greenbus.edge.data.codegen.test", all, fw)
    fw.flush()

  }
}

/*
object ReadWriteTest {

  val ex = FrontendDataKey("myGateway", TypeCast("myTarget"))

  def main(args: Array[String]): Unit = {

    val written = FrontendDataKey.write(ex)
    println(written)

    val readEither = written match {
      case t: TaggedValue =>
        t.value match {
          case tt: ValueMap => FrontendDataKey.read(tt, SimpleReaderContext(Vector(RootCtx("DNP3Gateway"))))
          case _ => throw new IllegalArgumentException(s"Written was not a tagged tuple type")
        }
      case _ => throw new IllegalArgumentException(s"Written was not a tagged tuple type")
    }

    readEither match {
      case Left(err) => println("ERROR: " + err)
      case Right(read) =>
        println(ex)
        println(read)
        println(ex == read)
    }

  }
}
*/

/*


case class IndexDescriptor(key: Path, value: IndexableValue)
case class MetadataDescriptor(key: Path, value: IndexableValue)

case class FrontendDataKey(
  gatewayKey: String,
  path: Path,
  seriesDescriptor: SeriesDescriptor,
  transforms: Seq[TransformDescriptor],
  filter: FilterDescriptor,
  indexes: Map[Path, IndexableValue],
  metadata: Map[Path, Value])

case class FrontendEndpointConfiguration(
  endpointId: EndpointId,
  dataKeys: Seq[FrontendDataKey])

sealed trait SampleType
object SampleType {
  case object Float extends SampleType
  case object Double extends SampleType
  case object Int32 extends SampleType
  case object UInt32 extends SampleType
  case object Int64 extends SampleType
  case object UInt64 extends SampleType
  case object Bool extends SampleType
  case object Byte extends SampleType
}

sealed trait SeriesType
object SeriesType {
  case object AnalogStatus extends SeriesType
  case object AnalogSample extends SeriesType
  case object CounterStatus extends SeriesType
  case object CounterSample extends SeriesType
  case object BooleanStatus extends SeriesType
  case object IntegerEnum extends SeriesType
}

sealed trait TransformDescriptor
case class TypeCast(target: SampleType) extends TransformDescriptor
case class LinearTransform(scale: Double, offset: Double) extends TransformDescriptor
case object Negate extends TransformDescriptor

case class FilterDescriptor(suppressDuplicates: Option[Boolean], deadband: Option[Double])

case class BooleanLabels(trueLabel: String, falseLabel: String)

case class SeriesDescriptor(
  seriesType: SeriesType,
  unit: Option[String],
  decimalPoints: Option[Int],
  labeledInteger: Option[Map[Long, String]],
  labeledBoolean: Option[BooleanLabels])

 */ 