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
package io.greenbus.edge.api.stream.subscribe

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.api.stream.AppendDataKeyCodec.{ LatestKeyValueCodec, SeriesCodec, TopicEventCodec }
import io.greenbus.edge.api.stream.KeyedSetDataKeyCodec.ActiveSetCodec
import io.greenbus.edge.api.stream.{ AppendDataKeyCodec, EdgeCodecCommon, EdgeSubCodec, KeyedSetDataKeyCodec }
import io.greenbus.edge.stream.TypeValue
import io.greenbus.edge.stream.consume.{ Appended, DataValueUpdate, MapUpdated }

trait DataKeyCodec {
  def updateFor(dataValueUpdate: DataValueUpdate, descOpt: Option[DataKeyDescriptor]): Seq[IdentifiedEdgeUpdate]
}

class AppendDataCodec(id: EndpointPath, valueCodec: AppendDataKeyCodec) extends DataKeyCodec with LazyLogging {
  def updateFor(dataValueUpdate: DataValueUpdate, descOpt: Option[DataKeyDescriptor]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: Appended => {
        val readValues: Seq[SequenceDataKeyValueUpdate] = up.values.flatMap { ap =>
          valueCodec.fromTypeValue(ap.value) match {
            case Left(str) =>
              logger.warn(s"Could not extract data value for $id: $str")
              None
            case Right(value) =>
              Some(value)
          }
        }

        if (readValues.nonEmpty) {
          val head = readValues.head
          val headUp = IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(descOpt, head)))
          Seq(headUp) ++ readValues.tail.map(v => IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(None, v))))
        } else {
          Seq()
        }
      }
      case _ => Seq()
    }
  }
}

class MapDataKeySubCodec(id: EndpointPath, codec: KeyedSetDataKeyCodec) extends DataKeyCodec with LazyLogging {

  def updateFor(dataValueUpdate: DataValueUpdate, descOpt: Option[DataKeyDescriptor]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: MapUpdated => {

        val vOpt = codec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data value for $id: $str")
            None
          case Right(value) =>
            Some(value)
        }

        vOpt.map(v => IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(descOpt, v))))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class DynamicDataKeyCodec(logId: String, id: EndpointPath) extends EdgeSubCodec with LazyLogging {

  private var activeCodec = Option.empty[DataKeyCodec]

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {

    val descUpdateOpt = metaOpt.flatMap { tv =>
      EdgeCodecCommon.readDataKeyDescriptor(tv) match {
        case Left(str) =>
          logger.warn(s"Could not extract descriptor for $logId: $str")
          None
        case Right(value) => Some(value)
      }
    }

    descUpdateOpt.foreach {
      case _: TimeSeriesValueDescriptor =>
        activeCodec = Some(new AppendDataCodec(id, SeriesCodec))
      case _: LatestKeyValueDescriptor =>
        activeCodec = Some(new AppendDataCodec(id, LatestKeyValueCodec))
      case _: EventTopicValueDescriptor =>
        activeCodec = Some(new AppendDataCodec(id, TopicEventCodec))
      case _: ActiveSetValueDescriptor =>
        activeCodec = Some(new MapDataKeySubCodec(id, ActiveSetCodec))
      case _ =>
    }

    activeCodec match {
      case Some(codec) => codec.updateFor(dataValueUpdate, descUpdateOpt)
      case None => Seq()
    }
  }
}
