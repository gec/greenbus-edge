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
package io.greenbus.edge.api.stream

import io.greenbus.edge.OutputValueStatus
import io.greenbus.edge.api._
import io.greenbus.edge.api.proto.convert.{ Conversions, ValueConversions }
import io.greenbus.edge.colset._
import io.greenbus.edge.api.proto
import io.greenbus.edge.colset.subscribe.KeyedSetUpdated
import io.greenbus.edge.util.EitherUtil

object EdgeTables {

  val endpointDescTable = "edm.endpointdesc"

  val latestKeyValueTable = "edm.lkv"
  val timeSeriesValueTable = "edm.tsv"
  val eventTopicValueTable = "edm.events"
  val activeSetValueTable = "edm.set"

  val outputTable = "edm.output"
}

trait EdgeDataKeyCodec {
  def dataKeyToRow(endpointPath: EndpointPath): RowId
}

object AppendDataKeyCodec {

  def readTimestamp(value: TypeValue): Either[String, Long] = {
    value match {
      case v: Int64Val => Right(v.v)
      case _ => Left("Incorrect value type for timestamp: " + value)
    }
  }

  object SeriesCodec extends AppendDataKeyCodec {

    def dataKeyToRow(endpointPath: EndpointPath): RowId = {
      EdgeCodecCommon.dataKeyRowId(endpointPath, EdgeTables.timeSeriesValueTable)
    }

    def fromTypeValue(value: TypeValue): Either[String, EdgeSequenceDataKeyValue] = {

      value match {
        case TupleVal(elems) => {
          if (elems.size >= 2) {
            for {
              sv <- EdgeCodecCommon.readSampleValue(elems(0))
              time <- readTimestamp(elems(1))
            } yield {
              SeriesUpdate(sv, time)
            }
          } else {
            Left("Insufficient values in tuple for series")
          }
        }
        case _ => Left("Incorrect value type for timestamp: " + value)
      }
    }
  }

  object KeyValueCodec extends AppendDataKeyCodec {

    def dataKeyToRow(endpointPath: EndpointPath): RowId = {
      EdgeCodecCommon.dataKeyRowId(endpointPath, EdgeTables.latestKeyValueTable)
    }

    def fromTypeValue(value: TypeValue): Either[String, EdgeSequenceDataKeyValue] = {
      EdgeCodecCommon.readValue(value)
        .map(v => KeyValueUpdate(v))
    }
  }

  object TopicEventCodec extends AppendDataKeyCodec {

    def dataKeyToRow(endpointPath: EndpointPath): RowId = {
      EdgeCodecCommon.dataKeyRowId(endpointPath, EdgeTables.eventTopicValueTable)
    }

    def fromTypeValue(value: TypeValue): Either[String, EdgeSequenceDataKeyValue] = {
      value match {
        case TupleVal(elems) => {
          if (elems.size >= 2) {
            for {
              topic <- EdgeCodecCommon.readPath(elems(0))
              edgeValue <- EdgeCodecCommon.readValue(elems(1))
              time <- readTimestamp(elems(2))
            } yield {
              TopicEventUpdate(topic, edgeValue, time)
            }
          } else {
            Left("Insufficient values in tuple for series")
          }
        }
        case _ => Left("Incorrect value type for timestamp: " + value)
      }
    }
  }
}
trait AppendDataKeyCodec extends EdgeDataKeyCodec {
  def fromTypeValue(v: TypeValue): Either[String, EdgeSequenceDataKeyValue]
}

/*object AppendOutputKeyCodec {

}
trait AppendOutputKeyCodec extends EdgeDataKeyCodec {
  def fromTypeValue(v: TypeValue): Either[String, OutputValueStatus]
}*/

object KeyedSetDataKeyCodec {

  object ActiveSetCodec extends KeyedSetDataKeyCodec {

    def readMapTuple(tup: (TypeValue, TypeValue)): Either[String, (IndexableValue, Value)] = {
      for {
        key <- EdgeCodecCommon.readIndexableValue(tup._1)
        v <- EdgeCodecCommon.readValue(tup._2)
      } yield {
        (key, v)
      }
    }

    def dataKeyToRow(endpointPath: EndpointPath): RowId = {
      EdgeCodecCommon.dataKeyRowId(endpointPath, EdgeTables.activeSetValueTable)
    }

    def fromTypeValue(v: KeyedSetUpdated): Either[String, ActiveSetUpdate] = {
      for {
        map <- EitherUtil.rightSequence(v.value.toVector.map(readMapTuple)).map(_.toMap)
        removes <- EitherUtil.rightSequence(v.removed.toVector.map(EdgeCodecCommon.readIndexableValue))
        adds <- EitherUtil.rightSequence(v.added.toVector.map(readMapTuple)).map(_.toMap)
        modifies <- EitherUtil.rightSequence(v.modified.toVector.map(readMapTuple)).map(_.toMap)
      } yield {
        ActiveSetUpdate(map, removes.toSet, adds.toSet, modifies.toSet)
      }
    }
  }
}
trait KeyedSetDataKeyCodec extends EdgeDataKeyCodec {
  def fromTypeValue(v: KeyedSetUpdated): Either[String, ActiveSetUpdate]
}

trait EdgeCodec {
  def endpointIdToEndpointDescriptorRow(id: EndpointId): RowId
  def readEndpointDescriptor(v: TypeValue): Either[String, EndpointDescriptor]
}

object EdgeCodecCommon {

  def parse[A](bytes: Array[Byte], f: Array[Byte] => A): Either[String, A] = {
    try {
      Right(f(bytes))
    } catch {
      case ex: Throwable => Left("Error parsing: " + ex.toString)
    }
  }
  def parseAndConvert[A, B](bytes: Array[Byte], p: Array[Byte] => A, c: A => Either[String, B]): Either[String, B] = {
    parse(bytes, p).flatMap(c)
  }

  def readSampleValue(v: TypeValue): Either[String, SampleValue] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.SampleValue.parseFrom).flatMap { protoValue =>
          ValueConversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for edge value: " + v)
    }
  }
  def writeSampleValue(v: SampleValue): TypeValue = {
    BytesVal(ValueConversions.toProto(v).toByteArray)
  }

  def readIndexableValue(v: TypeValue): Either[String, IndexableValue] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.IndexableValue.parseFrom).flatMap { protoValue =>
          ValueConversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for edge value: " + v)
    }
  }
  def writeIndexableValue(v: IndexableValue): TypeValue = {
    BytesVal(ValueConversions.toProto(v).toByteArray)
  }

  def readValue(v: TypeValue): Either[String, Value] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.Value.parseFrom).flatMap { protoValue =>
          ValueConversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for edge value: " + v)
    }
  }
  def writeValue(v: Value): TypeValue = {
    BytesVal(ValueConversions.toProto(v).toByteArray)
  }

  def readPath(v: TypeValue): Either[String, Path] = {
    v match {
      case tup: TupleVal => {
        val elems = tup.elements.map {
          case SymbolVal(s) => Right(s)
          case other => Left(s"Wrong value type in path tuple: $other")
        }

        EitherUtil.rightSequence(elems).map(seq => Path(seq))
      }
      case _ => Left(s"Wrong value type for path: $v")
    }
  }

  def writePath(path: Path): TupleVal = {
    TupleVal(path.parts.map(SymbolVal))
  }

  def writeEndpointId(id: EndpointId): TupleVal = {
    writePath(id.path)
  }
  def endpointIdToRoute(id: EndpointId): TypeValue = writeEndpointId(id)

  def endpointIdToEndpointDescriptorTableRow(id: EndpointId): TableRow = {
    TableRow(EdgeTables.endpointDescTable, writeEndpointId(id))
  }
  def endpointIdToEndpointDescriptorRow(id: EndpointId): RowId = {
    val route = endpointIdToRoute(id)
    RowId(route, EdgeTables.endpointDescTable, writeEndpointId(id))
  }

  def readEndpointDescriptor(v: TypeValue): Either[String, EndpointDescriptor] = {
    v match {
      case b: BytesVal =>
        val protoValue = proto.EndpointDescriptor.parseFrom(b.v)
        Conversions.fromProto(protoValue)
      case _ => Left(s"Wrong value type for endpoint descriptor: " + v)
    }
  }
  def writeEndpointDescriptor(desc: EndpointDescriptor): TypeValue = {
    BytesVal(Conversions.toProto(desc).toByteArray)
  }

  def dataKeyRowId(endpointPath: EndpointPath, table: String): RowId = {
    val route = endpointIdToRoute(endpointPath.endpoint)
    val key = writePath(endpointPath.key)

    RowId(route, table, key)
  }

  def writeSampleValueSeries(obj: (SampleValue, Long)): TypeValue = {
    TupleVal(Seq(writeSampleValue(obj._1), Int64Val(obj._2)))
  }

  def writeTopicEvent(obj: (Path, Value, Long)): TypeValue = {
    TupleVal(Seq(writePath(obj._1), writeValue(obj._2), Int64Val(obj._3)))
  }

  def writeMap(obj: Map[IndexableValue, Value]): Map[TypeValue, TypeValue] = {
    obj.map {
      case (key, value) =>
        (BytesVal(ValueConversions.toProto(key).toByteArray), BytesVal(ValueConversions.toProto(value).toByteArray))
    }
  }
}
