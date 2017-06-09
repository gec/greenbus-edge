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

import io.greenbus.edge.api.{ proto, _ }
import io.greenbus.edge.api.proto.convert.{ Conversions, OutputConversions }
import io.greenbus.edge.data.proto.convert.ValueConversions
import io.greenbus.edge.data.{ IndexableValue, SampleValue, Value, proto => vproto }
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.consume.{ MapUpdated, SetUpdated }
import io.greenbus.edge.util.EitherUtil

object EdgeTables {

  val endpointDescTable = "edm.endpointdesc"

  val latestKeyValueTable = "edm.lkv"
  val timeSeriesValueTable = "edm.tsv"
  val eventTopicValueTable = "edm.events"
  val activeSetValueTable = "edm.set"

  val dataKeyTable = "edm.data"
  val outputTable = "edm.output"

  val endpointPrefixTable = "edm.prefix.endpoint"
  val endpointIndexTable = "edm.index.endpoint"
  val dataKeyIndexTable = "edm.index.datakey"
  val outputKeyIndexTable = "edm.index.outputkey"
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

    def latest: Boolean = false

    def dataKeyToRow(endpointPath: EndpointPath): RowId = {
      EdgeCodecCommon.keyRowId(endpointPath, EdgeTables.timeSeriesValueTable)
    }

    def fromTypeValue(value: TypeValue): Either[String, SequenceDataKeyValueUpdate] = {

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

  object LatestKeyValueCodec extends AppendDataKeyCodec {

    def latest: Boolean = true

    def dataKeyToRow(endpointPath: EndpointPath): RowId = {
      EdgeCodecCommon.keyRowId(endpointPath, EdgeTables.latestKeyValueTable)
    }

    def fromTypeValue(value: TypeValue): Either[String, SequenceDataKeyValueUpdate] = {
      EdgeCodecCommon.readValue(value)
        .map(v => KeyValueUpdate(v))
    }
  }

  object TopicEventCodec extends AppendDataKeyCodec {

    def latest: Boolean = false

    def dataKeyToRow(endpointPath: EndpointPath): RowId = {
      EdgeCodecCommon.keyRowId(endpointPath, EdgeTables.eventTopicValueTable)
    }

    def fromTypeValue(value: TypeValue): Either[String, SequenceDataKeyValueUpdate] = {
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
  def fromTypeValue(v: TypeValue): Either[String, SequenceDataKeyValueUpdate]
  def latest: Boolean
}

object AppendOutputKeyCodec extends AppendOutputKeyCodec {

  def dataKeyToRow(endpointPath: EndpointPath): RowId = {
    EdgeCodecCommon.keyRowId(endpointPath, EdgeTables.outputTable)
  }

  def fromTypeValue(v: TypeValue): Either[String, OutputKeyStatus] = {
    EdgeCodecCommon.readOutputKeyStatus(v)
  }

}
trait AppendOutputKeyCodec extends EdgeDataKeyCodec {
  def fromTypeValue(v: TypeValue): Either[String, OutputKeyStatus]
}

object SetCodec {

  object EndpointIdSetCodec extends SetCodec[EndpointSetUpdate] {
    def fromTypeValue(v: SetUpdated): Either[String, EndpointSetUpdate] = {
      for {
        set <- EitherUtil.rightSequence(v.value.map(EdgeCodecCommon.readEndpointId).toVector)
        removes <- EitherUtil.rightSequence(v.removed.map(EdgeCodecCommon.readEndpointId).toVector)
        adds <- EitherUtil.rightSequence(v.added.map(EdgeCodecCommon.readEndpointId).toVector)
      } yield {
        EndpointSetUpdate(set.toSet, removes.toSet, adds.toSet)
      }
    }
  }

  object EndpointPathSetCodec extends SetCodec[KeySetUpdate] {
    def fromTypeValue(v: SetUpdated): Either[String, KeySetUpdate] = {
      for {
        set <- EitherUtil.rightSequence(v.value.map(EdgeCodecCommon.readEndpointPath).toVector)
        removes <- EitherUtil.rightSequence(v.removed.map(EdgeCodecCommon.readEndpointPath).toVector)
        adds <- EitherUtil.rightSequence(v.added.map(EdgeCodecCommon.readEndpointPath).toVector)
      } yield {
        KeySetUpdate(set.toSet, removes.toSet, adds.toSet)
      }
    }
  }

}
trait SetCodec[A] {
  def fromTypeValue(v: SetUpdated): Either[String, A]
}

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
      EdgeCodecCommon.keyRowId(endpointPath, EdgeTables.activeSetValueTable)
    }

    def fromTypeValue(v: MapUpdated): Either[String, ActiveSetUpdate] = {
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
  def fromTypeValue(v: MapUpdated): Either[String, ActiveSetUpdate]
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
        parse(b.v, vproto.SampleValue.parseFrom).flatMap { protoValue =>
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
        parse(b.v, vproto.IndexableValue.parseFrom).flatMap { protoValue =>
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
        parse(b.v, vproto.Value.parseFrom).flatMap { protoValue =>
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

  def convertSession(session: PeerSessionId): SessionId = {
    SessionId(session.persistenceId, session.instanceId)
  }

  def writeEndpointId(id: EndpointId): TupleVal = {
    writePath(id.path)
  }
  def readEndpointId(v: TypeValue): Either[String, EndpointId] = {
    readPath(v).map(p => EndpointId(p))
  }
  def endpointIdToRoute(id: EndpointId): TypeValue = writeEndpointId(id)
  def routeToEndpointId(v: TypeValue): Either[String, EndpointId] = readEndpointId(v)

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

  def dataKeyRowId(endpointPath: EndpointPath): RowId = {
    keyRowId(endpointPath, EdgeTables.dataKeyTable)
  }

  def outputKeyRowId(endpointPath: EndpointPath): RowId = {
    keyRowId(endpointPath, EdgeTables.outputTable)
  }

  def keyRowId(endpointPath: EndpointPath, table: String): RowId = {
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

  def writeOutputKeyStatus(v: OutputKeyStatus): TypeValue = {
    BytesVal(OutputConversions.toProto(v).toByteArray)
  }
  def readOutputKeyStatus(v: TypeValue): Either[String, OutputKeyStatus] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.OutputKeyStatus.parseFrom).flatMap { protoValue =>
          OutputConversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for edge output result: " + v)
    }
  }

  def writeOutputResult(v: OutputResult): TypeValue = {
    BytesVal(OutputConversions.toProto(v).toByteArray)
  }
  def readOutputResult(v: TypeValue): Either[String, OutputResult] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.OutputResult.parseFrom).flatMap { protoValue =>
          OutputConversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for edge output result: " + v)
    }
  }

  def writeIndexSpecifier(v: OutputParams): TypeValue = {
    BytesVal(OutputConversions.toProto(v).toByteArray)
  }
  def readOutputRequest(v: TypeValue): Either[String, OutputParams] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.OutputParams.parseFrom).flatMap { protoValue =>
          OutputConversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for edge output request: " + v)
    }
  }

  def writeMap(obj: Map[IndexableValue, Value]): Map[TypeValue, TypeValue] = {
    obj.map {
      case (key, value) =>
        (BytesVal(ValueConversions.toProto(key).toByteArray), BytesVal(ValueConversions.toProto(value).toByteArray))
    }
  }

  def writeIndexSpecifier(v: IndexSpecifier): TypeValue = {
    BytesVal(Conversions.toProto(v).toByteArray)
  }
  def readIndexSpecifier(v: TypeValue): Either[String, IndexSpecifier] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.IndexSpecifier.parseFrom).flatMap { protoValue =>
          Conversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for index specifier: " + v)
    }
  }

  def writeEndpointPath(id: EndpointPath): TypeValue = {
    BytesVal(Conversions.toProto(id).toByteArray)
  }
  def readEndpointPath(v: TypeValue): Either[String, EndpointPath] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.EndpointPath.parseFrom).flatMap { protoValue =>
          Conversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for EndpointPath: " + v)
    }
  }

  def writeEndpointIdProto(id: EndpointId): TypeValue = {
    BytesVal(Conversions.toProto(id).toByteArray)
  }
  def readEndpointIdProto(v: TypeValue): Either[String, EndpointId] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.EndpointId.parseFrom).flatMap { protoValue =>
          Conversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for EndpointId: " + v)
    }
  }

  /*case class IndexSubKey(spec: IndexSpecifier, table: String) extends PeerBasedSubKey {
    def row(session: PeerSessionId): RowId = {
      RowId(IndexProducer.routeForSession(session), table, writeIndexSpecifier(spec))
    }
  }

  case class EndpointPrefixSubKey(path: Path) extends PeerBasedSubKey {
    def row(session: PeerSessionId): RowId = {
      RowId(IndexProducer.routeForSession(session), EdgeTables.endpointPrefixTable, writePath(path))
    }
  }

  private def indexSubKey(spec: IndexSpecifier, table: String): SubscriptionKey = {
    IndexSubKey(spec, table)
  }

  def endpointPrefixToSubKey(path: Path): SubscriptionKey = {
    EndpointPrefixSubKey(path)
  }
  def endpointIndexSpecToSubKey(spec: IndexSpecifier): SubscriptionKey = {
    indexSubKey(spec, EdgeTables.endpointIndexTable)
  }
  def dataKeyIndexSpecToSubKey(spec: IndexSpecifier): SubscriptionKey = {
    indexSubKey(spec, EdgeTables.dataKeyIndexTable)
  }
  def outputKeyIndexSpecToSubKey(spec: IndexSpecifier): SubscriptionKey = {
    indexSubKey(spec, EdgeTables.outputKeyIndexTable)
  }
*/
  def writeDataKeyDescriptor(desc: DataKeyDescriptor): TypeValue = {
    BytesVal(Conversions.toProto(desc).toByteArray)
  }
  def readDataKeyDescriptor(v: TypeValue): Either[String, DataKeyDescriptor] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.DataKeyDescriptor.parseFrom).flatMap { protoValue =>
          Conversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for DataKeyDescriptor: " + v)
    }
  }

  def writeOutputKeyDescriptor(desc: OutputKeyDescriptor): TypeValue = {
    BytesVal(Conversions.toProto(desc).toByteArray)
  }
  def readOutputKeyDescriptor(v: TypeValue): Either[String, OutputKeyDescriptor] = {
    v match {
      case b: BytesVal =>
        parse(b.v, proto.OutputKeyDescriptor.parseFrom).flatMap { protoValue =>
          Conversions.fromProto(protoValue)
        }
      case _ => Left(s"Wrong value type for OutputKeyDescriptor: " + v)
    }
  }
}
