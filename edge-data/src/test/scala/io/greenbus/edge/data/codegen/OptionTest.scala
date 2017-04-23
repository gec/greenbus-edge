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

  val ns = TypeNamespace("my.fake.namespace.first", options = Map("scalaPackage" -> "io.greenbus.edge.data.codegen.test"))

  val selectRange: TExt = {
    TExt(ns, "IndexRange", TStruct(Vector(
      StructFieldDef("start", TUInt32, 0),
      StructFieldDef("count", TUInt32, 1))))
  }
  val indexSet: TExt = {
    //TExt("IndexSet", TList(TUnion(Set(selectIndex, selectRange))))
    TExt(ns, "IndexSet", TList(selectRange))
  }

  val typeCast = TExt(ns, "TypeCast", TStruct(Vector(
    StructFieldDef("target", TString, 0))))

  val linearTransform = TExt(ns, "LinearTransform", TStruct(Vector(
    StructFieldDef("scale", TDouble, 0),
    StructFieldDef("offset", TDouble, 1))))

  val transformDescriptor = TExt(ns, "TransformDescriptor", TUnion(Set(typeCast, linearTransform)))

  val seriesType = TExt(ns, "SeriesType", TEnum(Seq(
    EnumDef("AnalogStatus", 0),
    EnumDef("AnalogSample", 1),
    EnumDef("CounterStatus", 2),
    EnumDef("CounterSample", 3),
    EnumDef("BooleanStatus", 4),
    EnumDef("IntegerEnum", 5))))

  val frontendDataKey = TExt(ns, "FrontendDataKey", TStruct(Vector(
    StructFieldDef("gatewayKey", TString, 0),
    StructFieldDef("transforms", transformDescriptor, 1),
    StructFieldDef("indexSet", indexSet, 2),
    StructFieldDef("unit", TOption(TString), 3),
    StructFieldDef("seriesType", seriesType, 4))))

  def main(args: Array[String]): Unit = {
    val all = Gen.collectObjDefs(ns.name, frontendDataKey, Map())

    println(all)

    val f = new File("edge-data/target/generated-sources/scala/io/greenbus/edge/data/codegen/test/Model.scala")
    Files.createParentDirs(f)
    if (!f.exists()) {
      f.createNewFile()
    }

    val fw = new PrintWriter(new FileOutputStream(f))
    Gen.output("io.greenbus.edge.data.codegen.test", ns.name, all, fw)
    fw.flush()
  }
}

object SecondSchema {

  val ns = TypeNamespace("my.fake.namespace.second", options = Map("scalaPackage" -> "io.greenbus.edge.data.codegen.test2"))

  val secondSchemaStruct = TExt(ns, "SecondSchemaStruct", TStruct(Vector(
    StructFieldDef("localKey", TString, 0),
    StructFieldDef("unit", TOption(TString), 1))))

  val masterStruct = TExt(ns, "MasterStruct", TStruct(Vector(
    StructFieldDef("secondSchema", secondSchemaStruct, 0),
    StructFieldDef("originalSchema", OptionTest.frontendDataKey, 1))))

  def main(args: Array[String]): Unit = {
    val all = Gen.collectObjDefs(ns.name, masterStruct, Map())

    println(all)

    val f = new File("edge-data/target/generated-sources/scala/io/greenbus/edge/data/codegen/test2/Model.scala")
    Files.createParentDirs(f)
    if (!f.exists()) {
      f.createNewFile()
    }

    val fw = new PrintWriter(new FileOutputStream(f))
    Gen.output("io.greenbus.edge.data.codegen.test2", ns.name, all, fw)
    fw.flush()
  }
}

/*
object ReadWriteTest {

  val ex = FrontendDataKey("myGateway", TypeCast("myTarget"), IndexSet(Seq(IndexRange(0, 10), IndexRange(20, 29))), None, SeriesType.IntegerEnum)

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
