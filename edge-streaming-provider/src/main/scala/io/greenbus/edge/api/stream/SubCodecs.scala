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

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.api.stream.SetCodec.EndpointIdSetCodec
import io.greenbus.edge.stream.TypeValue
import io.greenbus.edge.stream.subscribe.{ Appended, DataValueUpdate, MapUpdated, SetUpdated }
import io.greenbus.edge.util.EitherUtil

class AppendDataKeySubCodec(logId: String, id: EndpointPath, codec: AppendDataKeyCodec) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {

    dataValueUpdate match {
      case up: Appended => {
        val readValues: Seq[SequenceDataKeyValueUpdate] = up.values.flatMap { ap =>
          codec.fromTypeValue(ap.value) match {
            case Left(str) =>
              logger.warn(s"Could not extract data value for $logId: $str")
              None
            case Right(value) =>
              Some(value)
          }
        }

        val descUpdateOpt = metaOpt.flatMap { tv =>
          EdgeCodecCommon.readDataKeyDescriptor(tv) match {
            case Left(str) =>
              logger.warn(s"Could not extract descriptor for $logId: $str")
              None
            case Right(value) => Some(value)
          }
        }

        if (readValues.nonEmpty) {
          val head = readValues.head
          val headUp = IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(descUpdateOpt, head)))
          Seq(headUp) ++ readValues.tail.map(v => IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(None, v))))
        } else {
          Seq()
        }
      }
      case _ =>
        Seq()
    }
  }
}

class EndpointDescSubCodec(logId: String, id: EndpointId) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdEndpointUpdate(id, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {

    dataValueUpdate match {
      case up: Appended => {
        val descOpt = up.values.lastOption.flatMap { av =>
          EdgeCodecCommon.readEndpointDescriptor(av.value) match {
            case Left(str) =>
              logger.warn(s"Could not extract data value for $logId: $str")
              None
            case Right(value) =>
              Some(value)
          }

        }

        descOpt.map(desc => IdEndpointUpdate(id, ResolvedValue(desc)))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class MapDataKeySubCodec(logId: String, id: EndpointPath, codec: KeyedSetDataKeyCodec) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdDataKeyUpdate(id, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: MapUpdated => {

        val vOpt = codec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data value for $logId: $str")
            None
          case Right(value) =>
            Some(value)
        }

        val descOpt = metaOpt.flatMap { tv =>
          EdgeCodecCommon.readDataKeyDescriptor(tv) match {
            case Left(str) =>
              logger.warn(s"Could not extract descriptor for $logId: $str")
              None
            case Right(value) => Some(value)
          }
        }

        vOpt.map(v => IdDataKeyUpdate(id, ResolvedValue(DataKeyUpdate(descOpt, v))))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class AppendOutputKeySubCodec(logId: String, id: EndpointPath, codec: AppendOutputKeyCodec) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdOutputKeyUpdate(id, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {

    dataValueUpdate match {
      case up: Appended => {
        val valueOpt = up.values.lastOption.flatMap { av =>
          codec.fromTypeValue(av.value) match {
            case Left(str) =>
              logger.warn(s"Could not extract data value for $logId: $str")
              None
            case Right(value) =>
              Some(value)
          }
        }

        val descOpt = metaOpt.flatMap { tv =>
          EdgeCodecCommon.readOutputKeyDescriptor(tv) match {
            case Left(str) =>
              logger.warn(s"Could not extract descriptor for $logId: $str")
              None
            case Right(value) => Some(value)
          }
        }

        valueOpt.map(v => IdOutputKeyUpdate(id, ResolvedValue(OutputKeyUpdate(descOpt, v))))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class ManifestRowToEndpointSetCodec(logId: String, path: Path) extends EdgeSubCodec with LazyLogging {
  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    IdEndpointPrefixUpdate(path, v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: MapUpdated =>
        if (up.added.nonEmpty || up.removed.nonEmpty) {
          val current = up.value.keySet.map(EdgeCodecCommon.readEndpointId).flatMap(_.toOption)
          val removes = up.removed.map(EdgeCodecCommon.readEndpointId).flatMap(_.toOption)
          val adds = up.added.map(_._1).map(EdgeCodecCommon.readEndpointId).flatMap(_.toOption)
          Seq(IdEndpointPrefixUpdate(path, ResolvedValue(EndpointSetUpdate(current, removes, adds))))
        } else {
          Seq()
        }
      case _ => Seq()
    }
  }
}

class EndpointSetSubCodec(logId: String, identify: EdgeDataStatus[EndpointSetUpdate] => IdentifiedEdgeUpdate) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    identify(v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: SetUpdated => {
        val vOpt = EndpointIdSetCodec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data value for $logId: $str")
            None
          case Right(data) =>
            Some(data)
        }

        vOpt.map(v => identify(ResolvedValue(v)))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

class KeySetSubCodec(logId: String, identify: EdgeDataStatus[KeySetUpdate] => IdentifiedEdgeUpdate) extends EdgeSubCodec with LazyLogging {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate = {
    identify(v)
  }

  def updateFor(dataValueUpdate: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate] = {
    dataValueUpdate match {
      case up: SetUpdated => {
        val vOpt = SetCodec.EndpointPathSetCodec.fromTypeValue(up) match {
          case Left(str) =>
            logger.warn(s"Could not extract data value for $logId: $str")
            None
          case Right(data) =>
            Some(data)
        }

        vOpt.map(v => identify(ResolvedValue(v)))
          .map(v => Seq(v)).getOrElse(Seq())
      }
      case _ =>
        Seq()
    }
  }
}

object SubscriptionManagers {

  def subEndpointDesc(id: EndpointId): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(id.toString, endpointDesc(id.toString, id))
  }
  def endpointDesc(logId: String, id: EndpointId): EdgeSubCodec = {
    new EndpointDescSubCodec(logId, id)
  }

  def subAppendDataKey(id: EndpointPath, codec: AppendDataKeyCodec): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(id.toString, appendDataKey(id, codec))
  }
  def appendDataKey(id: EndpointPath, codec: AppendDataKeyCodec): EdgeSubCodec = {
    new AppendDataKeySubCodec(id.toString, id, codec)
  }

  def subMapDataKey(id: EndpointPath, codec: KeyedSetDataKeyCodec): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(id.toString, mapDataKey(id, codec))
  }
  def mapDataKey(id: EndpointPath, codec: KeyedSetDataKeyCodec): EdgeSubCodec = {
    new MapDataKeySubCodec(id.toString, id, codec)
  }

  def subOutputStatus(id: EndpointPath, codec: AppendOutputKeyCodec): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(id.toString, outputStatus(id, codec))
  }
  def outputStatus(id: EndpointPath, codec: AppendOutputKeyCodec): EdgeSubCodec = {
    new AppendOutputKeySubCodec(id.toString, id, codec)
  }

  def subPrefixSet(prefix: Path): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(prefix.toString, prefixSet(prefix))
  }
  def prefixSet(prefix: Path): EdgeSubCodec = {

    def identify(status: EdgeDataStatus[EndpointSetUpdate]): IdentifiedEdgeUpdate = IdEndpointPrefixUpdate(prefix, status)

    new EndpointSetSubCodec(prefix.toString, identify)
  }

  def subEndpointIndexSet(spec: IndexSpecifier): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(spec.toString, endpointIndexSet(spec))
  }
  def endpointIndexSet(spec: IndexSpecifier): EdgeSubCodec = {

    def identify(status: EdgeDataStatus[EndpointSetUpdate]): IdentifiedEdgeUpdate = IdEndpointIndexUpdate(spec, status)

    new EndpointSetSubCodec(spec.toString, identify)
  }

  def subDataKeyIndexSet(spec: IndexSpecifier): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(spec.toString, dataKeyIndexSet(spec))
  }
  def dataKeyIndexSet(spec: IndexSpecifier): EdgeSubCodec = {

    def identify(status: EdgeDataStatus[KeySetUpdate]): IdentifiedEdgeUpdate = IdDataKeyIndexUpdate(spec, status)

    new KeySetSubCodec(spec.toString, identify)
  }

  def subOutputKeyIndexSet(spec: IndexSpecifier): EdgeTypeSubMgr = {
    new GenEdgeTypeSubMgrComp(spec.toString, outputKeyIndexSet(spec))
  }
  def outputKeyIndexSet(spec: IndexSpecifier): EdgeSubCodec = {

    def identify(status: EdgeDataStatus[KeySetUpdate]): IdentifiedEdgeUpdate = IdOutputKeyIndexUpdate(spec, status)

    new KeySetSubCodec(spec.toString, identify)
  }
}
