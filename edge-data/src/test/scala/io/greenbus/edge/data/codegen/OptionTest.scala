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

import java.io._

import com.google.common.io.Files
import io.greenbus.edge.data.schema._

object OptionTest {

  val selectRange: TExt = {
    TExt("IndexRange", TStruct(Vector(
      StructFieldDef("start", TUInt32, 0),
      StructFieldDef("count", TUInt32, 1))))
  }
  val indexSet: TExt = {
    //TExt("IndexSet", TList(TUnion(Set(selectIndex, selectRange))))
    TExt("IndexSet", TList(selectRange))
  }

  val typeCast = TExt("TypeCast", TStruct(Vector(
    StructFieldDef("target", TString, 0))))

  val linearTransform = TExt("LinearTransform", TStruct(Vector(
    StructFieldDef("scale", TDouble, 0),
    StructFieldDef("offset", TDouble, 1))))

  val transformDescriptor = TExt("TransformDescriptor", TUnion(Set(typeCast, linearTransform)))

  val frontendDataKey = TExt("FrontendDataKey", TStruct(Vector(
    StructFieldDef("gatewayKey", TString, 0),
    StructFieldDef("transforms", transformDescriptor, 1),
    StructFieldDef("indexSet", indexSet, 2))))

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

/*object ReadWriteTest {

  val ex = FrontendDataKey("myGateway", TypeCast("myTarget"), IndexSet(Seq(IndexRange(0, 10), IndexRange(20, 29))))

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
}*/

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