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
package io.greenbus.edge.proto.convert

import java.io.ByteArrayInputStream
import java.util.UUID

import com.google.protobuf.{ ByteString, Message }
import com.google.protobuf.util.JsonFormat
import io.greenbus.edge._
import io.greenbus.edge
import io.greenbus.edge.proto.ClientToServerMessage

import scala.collection.JavaConversions._

object MessageConversions {
  import Conversions._

  private def toProtoBytes[A, B <: Message](toProto: A => B, toByteArray: B => Array[Byte]): A => Array[Byte] = {
    def serialize(obj: A): Array[Byte] = toByteArray(toProto(obj))

    serialize
  }

  private def stream(array: Array[Byte], length: Int): ByteArrayInputStream = {
    new ByteArrayInputStream(array, 0, length)
  }

  def clientSubscriptionParamsToBytes(obj: edge.ClientSubscriptionParamsMessage): Array[Byte] = {
    toProto(obj).toByteArray
  }
  def clientSubscriptionParamsFromBytes(array: Array[Byte], length: Int): Either[String, edge.ClientSubscriptionParamsMessage] = {
    fromProto(proto.ClientSubscriptionParamsMessage.parseFrom(stream(array, length)))
  }

  def clientSubscriptionNotificationToBytes(obj: edge.ClientSubscriptionNotificationMessage): Array[Byte] = {
    toProto(obj).toByteArray
  }
  def clientSubscriptionNotificationFromBytes(array: Array[Byte], length: Int): Either[String, edge.ClientSubscriptionNotificationMessage] = {
    fromProto(proto.ClientSubscriptionNotificationMessage.parseFrom(stream(array, length)))
  }

  def endpointPublishMessageToBytes(obj: edge.EndpointPublishMessage): Array[Byte] = {
    toProto(obj).toByteArray
  }
  def endpointPublishMessageFromBytes(array: Array[Byte], length: Int): Either[String, edge.EndpointPublishMessage] = {
    fromProto(proto.EndpointPublishMessage.parseFrom(stream(array, length)))
  }

  def publisherOutputRequestMessageToBytes(obj: edge.PublisherOutputRequestMessage): Array[Byte] = {
    toProto(obj).toByteArray
  }
  def publisherOutputRequestMessageFromBytes(array: Array[Byte], length: Int): Either[String, edge.PublisherOutputRequestMessage] = {
    fromProto(proto.PublisherOutputRequestMessage.parseFrom(stream(array, length)))
  }

  def publisherOutputResponseMessageToBytes(obj: edge.PublisherOutputResponseMessage): Array[Byte] = {
    toProto(obj).toByteArray
  }
  def publisherOutputResponseMessageFromBytes(array: Array[Byte], length: Int): Either[String, edge.PublisherOutputResponseMessage] = {
    fromProto(proto.PublisherOutputResponseMessage.parseFrom(stream(array, length)))
  }

  def clientOutputRequestMessageToBytes(obj: edge.ClientOutputRequestMessage): Array[Byte] = {
    toProto(obj).toByteArray
  }
  def clientOutputRequestMessageFromBytes(array: Array[Byte], length: Int): Either[String, edge.ClientOutputRequestMessage] = {
    fromProto(proto.ClientOutputRequestMessage.parseFrom(stream(array, length)))
  }

  def clientOutputResponseMessageToBytes(obj: edge.ClientOutputResponseMessage): Array[Byte] = {
    toProto(obj).toByteArray
  }
  def clientOutputResponseMessageFromBytes(array: Array[Byte], length: Int): Either[String, edge.ClientOutputResponseMessage] = {
    fromProto(proto.ClientOutputResponseMessage.parseFrom(stream(array, length)))
  }
}

object Conversions {
  import ConversionUtil._
  import ValueConversions._

  // ============= DATA

  def toProto(obj: edge.SequencedValue): proto.SequencedValue = {
    proto.SequencedValue.newBuilder()
      .setSequence(obj.sequence)
      .setValue(ValueConversions.toProto(obj.value))
      .build()
  }
  def fromProto(msg: proto.SequencedValue): Either[String, edge.SequencedValue] = {
    if (msg.hasValue) {
      for { v <- ValueConversions.fromProto(msg.getValue) } yield {
        edge.SequencedValue(msg.getSequence, v)
      }
    } else {
      Left("SequencedValue did not contain value")
    }
  }

  def toProto(obj: edge.TimeSeriesSample): proto.TimeSeriesSample = {
    val b = proto.TimeSeriesSample.newBuilder()
    b.setValue(ValueConversions.toProto(obj.value))
    b.setTime(obj.time)
    b.build()
  }
  def fromProto(msg: proto.TimeSeriesSample): Either[String, edge.TimeSeriesSample] = {
    if (msg.hasValue) {
      for { v <- ValueConversions.fromProto(msg.getValue) } yield {
        edge.TimeSeriesSample(msg.getTime, v)
      }
    } else {
      Left("TimeSeriesSample did not contain value")
    }
  }

  def toProto(obj: edge.TimeSeriesSequenced): proto.SequencedTimeSeriesValue = {
    val b = proto.SequencedTimeSeriesValue.newBuilder()
    b.setSample(toProto(obj.sample))
    b.setSequence(obj.sequence)
    b.build()
  }
  def fromProto(msg: proto.SequencedTimeSeriesValue): Either[String, edge.TimeSeriesSequenced] = {
    if (msg.hasSample) {
      for { v <- fromProto(msg.getSample) } yield {
        edge.TimeSeriesSequenced(msg.getSequence, v)
      }
    } else {
      Left("TimeSeriesSample did not contain value")
    }
  }

  def toProto(obj: edge.TimeSeriesState): proto.TimeSeriesState = {
    val b = proto.TimeSeriesState.newBuilder()
    obj.values.map(toProto).foreach(b.addValues)
    b.build()
  }
  def fromProto(msg: proto.TimeSeriesState): Either[String, edge.TimeSeriesState] = {
    rightSequence(msg.getValuesList.map(fromProto))
      .map(seq => edge.TimeSeriesState(seq))
  }

  def toProto(obj: edge.TimeSeriesUpdate): proto.TimeSeriesUpdate = {
    val b = proto.TimeSeriesUpdate.newBuilder()
    obj.values.map(toProto).foreach(b.addValues)
    b.build()
  }
  def fromProto(msg: proto.TimeSeriesUpdate): Either[String, edge.TimeSeriesUpdate] = {
    rightSequence(msg.getValuesList.map(fromProto))
      .map(seq => edge.TimeSeriesUpdate(seq))
  }

  def toProto(obj: edge.DataValueState): proto.DataValueState = {
    val b = proto.DataValueState.newBuilder()
    obj match {
      case v: edge.SequencedValue => b.setSequencedValueState(toProto(v))
      case v: edge.TimeSeriesState => b.setTimeSeriesState(toProto(v))
      case _ => throw new IllegalArgumentException("Data value state unrecognized: " + obj)
    }
    b.build()
  }
  def fromProto(msg: proto.DataValueState): Either[String, edge.DataValueState] = {
    msg.getStateCase match {
      case proto.DataValueState.StateCase.SEQUENCED_VALUE_STATE =>
        fromProto(msg.getSequencedValueState)
      case proto.DataValueState.StateCase.TIME_SERIES_STATE =>
        fromProto(msg.getTimeSeriesState)
      case proto.DataValueState.StateCase.STATE_NOT_SET =>
        Left("DataValueState type unrecognized")
    }
  }
  def toProto(obj: edge.DataValueUpdate): proto.DataValueUpdate = {
    val b = proto.DataValueUpdate.newBuilder()
    obj match {
      case v: edge.SequencedValue => b.setSequencedValueUpdate(toProto(v))
      case v: edge.TimeSeriesUpdate => b.setTimeSeriesUpdate(toProto(v))
      case _ => throw new IllegalArgumentException("Data value state unrecognized: " + obj)
    }
    b.build()
  }
  def fromProto(msg: proto.DataValueUpdate): Either[String, edge.DataValueUpdate] = {
    msg.getUpdateCase match {
      case proto.DataValueUpdate.UpdateCase.SEQUENCED_VALUE_UPDATE =>
        fromProto(msg.getSequencedValueUpdate)
      case proto.DataValueUpdate.UpdateCase.TIME_SERIES_UPDATE =>
        fromProto(msg.getTimeSeriesUpdate)
      case proto.DataValueUpdate.UpdateCase.UPDATE_NOT_SET =>
        Left("DataValueState type unrecognized")
    }
  }

  def toProto(obj: edge.DataKeyDescriptor): proto.DataKeyDescriptor = {
    val b = proto.DataKeyDescriptor.newBuilder()
    obj.indexes.map(toProto).foreach(b.addIndexes)
    obj.metadata.map(toProto).foreach(b.addMetadata)

    obj match {
      case d: edge.LatestKeyValueDescriptor => b.setLatestKeyValue(proto.LatestKeyValueDescriptor.newBuilder().build())
      case d: edge.TimeSeriesValueDescriptor => b.setTimeSeriesValue(proto.TimeSeriesValueDescriptor.newBuilder().build())
    }
    b.build()
  }
  def fromProto(msg: proto.DataKeyDescriptor): Either[String, edge.DataKeyDescriptor] = {

    for {
      indexes <- rightSequence(msg.getIndexesList.map(fromProto))
      metadata <- rightSequence(msg.getMetadataList.map(fromProto))
    } yield {
      msg.getValueTypesCase match {
        case proto.DataKeyDescriptor.ValueTypesCase.LATEST_KEY_VALUE =>
          edge.LatestKeyValueDescriptor(indexes.toMap, metadata.toMap)
        case proto.DataKeyDescriptor.ValueTypesCase.TIME_SERIES_VALUE =>
          edge.TimeSeriesValueDescriptor(indexes.toMap, metadata.toMap)
        case _ =>
          edge.UnrecognizedValueDescriptor(indexes.toMap, metadata.toMap)
      }
    }
  }

  def toProto(obj: edge.OutputKeyDescriptor): proto.OutputKeyDescriptor = {
    val b = proto.OutputKeyDescriptor.newBuilder()
    obj.indexes.map(toProto).foreach(b.addIndexes)
    obj.metadata.map(toProto).foreach(b.addMetadata)
    b.build()
  }
  def fromProto(msg: proto.OutputKeyDescriptor): Either[String, edge.OutputKeyDescriptor] = {
    for {
      indexes <- rightSequence(msg.getIndexesList.map(fromProto))
      metadata <- rightSequence(msg.getMetadataList.map(fromProto))
    } yield {
      edge.OutputKeyDescriptor(indexes.toMap, metadata.toMap)
    }
  }

  // =============== /DATA

  def toProto(obj: edge.Path): proto.Path = {
    val b = proto.Path.newBuilder()
    obj.parts.foreach(b.addPart)
    b.build()
  }

  def fromProtoSimple(msg: proto.Path): edge.Path = {
    edge.Path(msg.getPartList.toVector)
  }

  def fromProto(msg: proto.Path): Either[String, edge.Path] = {
    Right(fromProtoSimple(msg))
  }

  def toProto(obj: edge.EndpointId): proto.EndpointId = {
    val b = proto.EndpointId.newBuilder()

    obj match {
      case id: NamedEndpointId => b.setNamedId(proto.NamedEndpointId.newBuilder().setName(id.name))
      case other => throw new IllegalArgumentException(s"Protobuf conversion did not recognize EndpointId $other")
    }

    b.build()
  }

  def fromProto(msg: proto.EndpointId): Either[String, edge.EndpointId] = {
    if (msg.hasNamedId) {
      val id = msg.getNamedId
      Right(edge.NamedEndpointId(id.getName))
    } else {
      Left("EndpointId unrecognized")
    }
  }

  def toProto(obj: edge.EndpointPath): proto.EndpointPath = {
    val b = proto.EndpointPath.newBuilder()
    b.setEndpointId(toProto(obj.endpoint))
    b.setKey(toProto(obj.key))
    b.build()
  }

  def fromProto(msg: proto.EndpointPath): Either[String, edge.EndpointPath] = {
    if (msg.hasEndpointId && msg.hasKey) {
      for {
        id <- fromProto(msg.getEndpointId)
        key <- fromProto(msg.getKey)
      } yield {
        edge.EndpointPath(id, key)
      }
    } else {
      Left("EndpointPath missing endpoint id or key")
    }
  }

  def toProto(obj: edge.ClientSubscriptionParams): proto.ClientSubscriptionParams = {
    val b = proto.ClientSubscriptionParams.newBuilder()

    obj.endpointSetPrefixes.map(toProto).foreach(b.addEndpointSetPrefix)
    obj.infoSubscriptions.map(toProto).foreach(b.addInfoSubscription)
    obj.dataSubscriptions.map(toProto).foreach(b.addDataSubscription)
    obj.outputSubscriptions.map(toProto).foreach(b.addOutputSubscription)

    b.build()
  }

  def fromProto(msg: proto.ClientSubscriptionParams): Either[String, edge.ClientSubscriptionParams] = {
    for {
      endpointSetPrefixes <- rightSequence(msg.getEndpointSetPrefixList.map(fromProto))
      infoSubscriptions <- rightSequence(msg.getInfoSubscriptionList.map(fromProto))
      dataSubscriptions <- rightSequence(msg.getDataSubscriptionList.map(fromProto))
      outputSubscriptions <- rightSequence(msg.getOutputSubscriptionList.map(fromProto))
    } yield {
      edge.ClientSubscriptionParams(
        endpointSetPrefixes = endpointSetPrefixes,
        infoSubscriptions = infoSubscriptions,
        dataSubscriptions = dataSubscriptions,
        outputSubscriptions = outputSubscriptions)
    }
  }

  def toProto(obj: edge.EndpointSetEntry): proto.EndpointSetEntry = {
    val b = proto.EndpointSetEntry.newBuilder()
    b.setEndpointId(toProto(obj.endpointId))
    obj.indexes.map(toProto).foreach(b.addIndexes)
    b.build()
  }

  def fromProto(msg: proto.EndpointSetEntry): Either[String, edge.EndpointSetEntry] = {
    if (msg.hasEndpointId) {
      for {
        id <- fromProto(msg.getEndpointId)
        indexes <- rightSequence(msg.getIndexesList.map(fromProto))
      } yield {
        edge.EndpointSetEntry(id, indexes.toMap)
      }
    } else {
      Left("EndpointSetEntry missing endpoint id")
    }
  }

  def toProto(obj: edge.EndpointSetSnapshot): proto.EndpointSetSnapshot = {
    val b = proto.EndpointSetSnapshot.newBuilder()
    obj.entries.map(toProto).foreach(b.addEntries)
    b.build()
  }
  def fromProto(msg: proto.EndpointSetSnapshot): Either[String, edge.EndpointSetSnapshot] = {
    rightSequence(msg.getEntriesList.map(fromProto)).map(r => edge.EndpointSetSnapshot(r))
  }

  def toProto(obj: edge.EndpointSetNotification): proto.EndpointSetNotification = {
    val b = proto.EndpointSetNotification.newBuilder()
    b.setPrefix(toProto(obj.prefix))
    obj.snapshotOpt.map(toProto).foreach(b.setSnapshot)
    obj.added.map(toProto).foreach(b.addAdded)
    obj.modified.map(toProto).foreach(b.addModified)
    obj.removed.map(toProto).foreach(b.addRemoved)
    b.build()
  }
  def fromProto(msg: proto.EndpointSetNotification): Either[String, edge.EndpointSetNotification] = {
    if (msg.hasPrefix) {
      val snapEither = if (msg.hasSnapshot) fromProto(msg.getSnapshot).map(r => Some(r)) else Right(None)

      for {
        prefix <- fromProto(msg.getPrefix)
        snap <- snapEither
        added <- rightSequence(msg.getAddedList.map(fromProto))
        modified <- rightSequence(msg.getModifiedList.map(fromProto))
        removed <- rightSequence(msg.getRemovedList.map(fromProto))
      } yield {
        edge.EndpointSetNotification(prefix, snap, added, modified, removed)
      }
    } else {
      Left("EndpointSetNotification missing prefix")
    }
  }

  def toProto(obj: (edge.Path, edge.IndexableValue)): proto.IndexKeyValue = {
    proto.IndexKeyValue.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(ValueConversions.toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.IndexKeyValue): Either[String, (edge.Path, edge.IndexableValue)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: (edge.Path, edge.Value)): proto.MetadataKeyValue = {
    proto.MetadataKeyValue.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(ValueConversions.toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.MetadataKeyValue): Either[String, (edge.Path, edge.Value)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- ValueConversions.fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: (edge.Path, edge.DataKeyDescriptor)): proto.DataKeyValue = {
    proto.DataKeyValue.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.DataKeyValue): Either[String, (edge.Path, edge.DataKeyDescriptor)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: (edge.Path, edge.OutputKeyDescriptor)): proto.OutputKeyValue = {
    proto.OutputKeyValue.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.OutputKeyValue): Either[String, (edge.Path, edge.OutputKeyDescriptor)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("IndexKeyValue missing key or value")
    }
  }

  def toProto(obj: edge.EndpointDescriptor): proto.EndpointDescriptor = {
    val b = proto.EndpointDescriptor.newBuilder()
    obj.indexes.map(toProto).foreach(b.addIndexes)
    obj.metadata.map(toProto).foreach(b.addMetadata)
    obj.dataKeySet.map(toProto).foreach(b.addDataKeySet)
    obj.outputKeySet.map(toProto).foreach(b.addOutputKeySet)
    b.build()
  }
  def fromProto(msg: proto.EndpointDescriptor): Either[String, edge.EndpointDescriptor] = {
    for {
      indexes <- rightSequence(msg.getIndexesList.map(fromProto))
      metadata <- rightSequence(msg.getMetadataList.map(fromProto))
      dataKeys <- rightSequence(msg.getDataKeySetList.map(fromProto))
      outputKeys <- rightSequence(msg.getOutputKeySetList.map(fromProto))
    } yield {
      edge.EndpointDescriptor(
        indexes = indexes.toMap,
        metadata = metadata.toMap,
        dataKeySet = dataKeys.toMap,
        outputKeySet = outputKeys.toMap)
    }
  }

  def toProto(obj: edge.EndpointDescriptorNotification): proto.EndpointDescriptorNotification = {
    val b = proto.EndpointDescriptorNotification.newBuilder()
    b.setEndpointId(toProto(obj.endpointId))
    b.setEndpointDescriptor(toProto(obj.descriptor))
    b.setSequence(obj.sequence)
    b.build()
  }

  def fromProto(msg: proto.EndpointDescriptorNotification): Either[String, edge.EndpointDescriptorNotification] = {
    if (msg.hasEndpointId && msg.hasEndpointDescriptor) {
      for {
        id <- fromProto(msg.getEndpointId)
        desc <- fromProto(msg.getEndpointDescriptor)
      } yield {
        edge.EndpointDescriptorNotification(id, desc, msg.getSequence)
      }
    } else {
      Left("EndpointDescriptorNotification missing id or descriptor")
    }
  }

  def toProto(obj: edge.DataValueNotification): proto.DataValueNotification = {
    val b = proto.DataValueNotification.newBuilder()
    b.build()
  }
  def fromProto(msg: proto.DataValueNotification): Either[String, edge.DataValueNotification] = {
    Right(SequencedValue(0, ValueString("fixme")))
  }

  def toProto(obj: edge.OutputValueStatus): proto.OutputValueStatus = {
    val b = proto.OutputValueStatus.newBuilder()
    b.build()
  }

  def persistenceSessionIdToProto(obj: edge.PersistenceSessionId): proto.PersistenceSessionId = {
    proto.PersistenceSessionId.newBuilder()
      .setPersistenceId(ValueConversions.toProto(obj.persistenceId))
      .setPersistenceSequence(obj.sequence)
      .build()
  }
  def fromProto(msg: proto.PersistenceSessionId): Either[String, edge.PersistenceSessionId] = {
    if (msg.hasPersistenceId) {
      Right(PersistenceSessionId(ValueConversions.fromProtoSimple(msg.getPersistenceId), msg.getPersistenceSequence))
    } else {
      Left("PersistenceSessionId missing persistence id")
    }
  }

  def toProto(obj: edge.SessionId): proto.SessionId = {
    obj match {
      case id: edge.PersistenceSessionId => proto.SessionId.newBuilder().setPersistenceId(persistenceSessionIdToProto(id)).build()
      case _ => throw new IllegalArgumentException("SessionId type unrecognized: " + obj)
    }
  }
  def fromProto(msg: proto.SessionId): Either[String, edge.SessionId] = {
    if (msg.hasPersistenceId) {
      fromProto(msg.getPersistenceId)
    } else {
      Left("PersistenceSessionId missing persistence id")
    }
  }

  /*def eitherOpt[L, ROpt](obj: Option[ROpt], f: ROpt => Either[L, ROpt]): Either[L, Option[ROpt]] = {
    obj match {
      case Some(v) => f(v).map(r => Some(r))
      case None => Right(None)
    }
  }*/

  def fromProto(msg: proto.OutputValueStatus): Either[String, edge.OutputValueStatus] = {
    if (msg.hasSessionId) {
      for {
        sessId <- fromProto(msg.getSessionId)
        valueOpt <- if (msg.hasValue) ValueConversions.fromProto(msg.getValue).map(r => Some(r)) else Right(None)
      } yield {
        OutputValueStatus(sessId, msg.getSequence, valueOpt)
      }
    } else {
      Left("Output value status missing session id")
    }
  }

  def toProto(obj: edge.EndpointDataNotification): proto.EndpointDataNotification = {
    val b = proto.EndpointDataNotification.newBuilder()
    b.setKey(toProto(obj.key))
    b.setValue(toProto(obj.value))
    b.build()
  }
  def fromProto(msg: proto.EndpointDataNotification): Either[String, edge.EndpointDataNotification] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        edge.EndpointDataNotification(key, value)
      }
    } else {
      Left("EndpointDescriptorNotification missing id or descriptor")
    }
  }

  def toProto(obj: edge.EndpointOutputStatusNotification): proto.EndpointOutputStatusNotification = {
    val b = proto.EndpointOutputStatusNotification.newBuilder()
    b.setKey(toProto(obj.key))
    b.setValue(toProto(obj.status))
    b.build()
  }
  def fromProto(msg: proto.EndpointOutputStatusNotification): Either[String, edge.EndpointOutputStatusNotification] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        edge.EndpointOutputStatusNotification(key, value)
      }
    } else {
      Left("EndpointOutputStatusNotification missing id or descriptor")
    }
  }

  def toProto(obj: edge.ClientSubscriptionNotification): proto.ClientSubscriptionNotification = {
    val b = proto.ClientSubscriptionNotification.newBuilder()
    obj.setNotifications.map(toProto).foreach(b.addEndpointSetNotification)
    obj.descriptorNotifications.map(toProto).foreach(b.addDescriptorNotification)
    obj.dataNotifications.map(toProto).foreach(b.addDataNotification)
    obj.outputNotifications.map(toProto).foreach(b.addOutputNotification)
    b.build()
  }

  def fromProto(msg: proto.ClientSubscriptionNotification): Either[String, edge.ClientSubscriptionNotification] = {
    for {
      endpointSets <- rightSequence(msg.getEndpointSetNotificationList.map(fromProto))
      descriptors <- rightSequence(msg.getDescriptorNotificationList.map(fromProto))
      datas <- rightSequence(msg.getDataNotificationList.map(fromProto))
      outputs <- rightSequence(msg.getOutputNotificationList.map(fromProto))
    } yield {
      edge.ClientSubscriptionNotification(
        setNotifications = endpointSets,
        descriptorNotifications = descriptors,
        dataNotifications = datas,
        outputNotifications = outputs)
    }
  }

  def toProto(obj: edge.ClientSubscriptionParamsMessage): proto.ClientSubscriptionParamsMessage = {
    proto.ClientSubscriptionParamsMessage.newBuilder()
      .setContent(toProto(obj.params))
      .build()
  }
  def fromProto(msg: proto.ClientSubscriptionParamsMessage): Either[String, edge.ClientSubscriptionParamsMessage] = {
    if (msg.hasContent) {
      fromProto(msg.getContent).map(content => edge.ClientSubscriptionParamsMessage(content))
    } else {
      Left("No content in subscription params message")
    }
  }

  def toProto(obj: edge.ClientSubscriptionNotificationMessage): proto.ClientSubscriptionNotificationMessage = {
    proto.ClientSubscriptionNotificationMessage.newBuilder()
      .setContent(toProto(obj.notification))
      .build()
  }
  def fromProto(msg: proto.ClientSubscriptionNotificationMessage): Either[String, edge.ClientSubscriptionNotificationMessage] = {
    if (msg.hasContent) {
      fromProto(msg.getContent).map(content => edge.ClientSubscriptionNotificationMessage(content))
    } else {
      Left("No content in subscription notification message")
    }
  }

  def toProto(obj: edge.EndpointDescriptorRecord): proto.EndpointDescriptorRecord = {
    val b = proto.EndpointDescriptorRecord.newBuilder()
    b.setEndpointId(toProto(obj.endpointId))
    b.setEndpointDescriptor(toProto(obj.descriptor))
    b.setSequence(obj.sequence)
    b.build()
  }
  def fromProto(msg: proto.EndpointDescriptorRecord): Either[String, edge.EndpointDescriptorRecord] = {
    if (msg.hasEndpointId && msg.hasEndpointDescriptor) {
      for {
        id <- fromProto(msg.getEndpointId)
        desc <- fromProto(msg.getEndpointDescriptor)
      } yield {
        edge.EndpointDescriptorRecord(msg.getSequence, id, desc)
      }
    } else {
      Left("EndpointDescriptorRecord missing id or descriptor")
    }
  }

  def toProto(obj: edge.PublisherOutputValueStatus): proto.PublisherOutputValueStatus = {
    proto.PublisherOutputValueStatus.newBuilder().build()
  }
  def fromProto(msg: proto.PublisherOutputValueStatus): Either[String, edge.PublisherOutputValueStatus] = {
    if (msg.hasValue) {
      ValueConversions.fromProto(msg.getValue).map(v => edge.PublisherOutputValueStatus(msg.getSequence, Some(v)))
    } else {
      Right(edge.PublisherOutputValueStatus(msg.getSequence, None))
    }
  }

  def toProto(obj: (edge.Path, edge.DataValueState)): proto.KeyedDataValueState = {
    proto.KeyedDataValueState.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.KeyedDataValueState): Either[String, (edge.Path, edge.DataValueState)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("KeyedDataValueState missing key or value")
    }
  }

  def toProto(obj: (edge.Path, edge.PublisherOutputValueStatus)): proto.KeyedPublisherOutputValueStatus = {
    proto.KeyedPublisherOutputValueStatus.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.KeyedPublisherOutputValueStatus): Either[String, (edge.Path, edge.PublisherOutputValueStatus)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("KeyedPublisherOutputValueStatus missing key or value")
    }
  }

  def toProto(obj: (edge.Path, edge.DataValueUpdate)): proto.KeyedDataValueUpdate = {
    proto.KeyedDataValueUpdate.newBuilder()
      .setKey(toProto(obj._1))
      .setValue(toProto(obj._2))
      .build()
  }
  def fromProto(msg: proto.KeyedDataValueUpdate): Either[String, (edge.Path, edge.DataValueUpdate)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("KeyedDataValueState missing key or value")
    }
  }

  def toProto(obj: edge.EndpointPublishSnapshot): proto.EndpointPublishSnapshot = {
    val b = proto.EndpointPublishSnapshot.newBuilder()
    b.setEndpointDescriptor(toProto(obj.endpoint))
    obj.data.map(toProto).foreach(b.addDataValues)
    obj.outputStatus.map(toProto).foreach(b.addOutputValues)
    b.build()
  }
  def fromProto(msg: proto.EndpointPublishSnapshot): Either[String, edge.EndpointPublishSnapshot] = {
    if (msg.hasEndpointDescriptor) {
      for {
        desc <- fromProto(msg.getEndpointDescriptor)
        dataStates <- rightSequence(msg.getDataValuesList.map(fromProto))
        outputStatuses <- rightSequence(msg.getOutputValuesList.map(fromProto))
      } yield {
        edge.EndpointPublishSnapshot(desc, dataStates.toMap, outputStatuses.toMap)
      }
    } else {
      Left("EndpointPublishSnapshot missing descriptor")
    }
  }

  def toProto(obj: edge.EndpointPublishMessage): proto.EndpointPublishMessage = {
    val b = proto.EndpointPublishMessage.newBuilder()
    obj.snapshotOpt.map(toProto).foreach(b.setSnapshot)
    obj.infoUpdateOpt.map(toProto).foreach(b.setDescriptorUpdate)
    obj.dataUpdates.map(toProto).foreach(b.addDataUpdates)
    obj.outputStatusUpdates.map(toProto).foreach(b.addOutputUpdates)
    b.build()
  }
  def fromProto(msg: proto.EndpointPublishMessage): Either[String, edge.EndpointPublishMessage] = {

    val snapEither = if (msg.hasSnapshot) fromProto(msg.getSnapshot).map(r => Some(r)) else Right(None)
    val recordEither = if (msg.hasDescriptorUpdate) fromProto(msg.getDescriptorUpdate).map(r => Some(r)) else Right(None)

    for {
      snapOpt <- snapEither
      recordOpt <- recordEither
      datas <- rightSequence(msg.getDataUpdatesList.map(fromProto))
      outputs <- rightSequence(msg.getOutputUpdatesList.map(fromProto))
    } yield {
      edge.EndpointPublishMessage(snapOpt, recordOpt, datas, outputs)
    }
  }

  def toProto(obj: edge.PublisherOutputParams): proto.PublisherOutputParams = {
    val b = proto.PublisherOutputParams.newBuilder()
    obj.sequenceOpt.map(toOptionUInt64).foreach(b.setSequence)
    obj.compareValueOpt.map(ValueConversions.toProto).foreach(b.setCompareValue)
    obj.outputValueOpt.map(ValueConversions.toProto).foreach(b.setOutputValue)
    b.build()
  }
  def fromProto(msg: proto.PublisherOutputParams): Either[String, edge.PublisherOutputParams] = {
    val compareEither = if (msg.hasCompareValue) ValueConversions.fromProto(msg.getCompareValue).map(r => Some(r)) else Right(None)
    val outputEither = if (msg.hasOutputValue) ValueConversions.fromProto(msg.getOutputValue).map(r => Some(r)) else Right(None)
    val seqOpt = if (msg.hasSequence) Some(msg.getSequence.getValue) else None
    for {
      compareOpt <- compareEither
      outOpt <- outputEither
    } yield {
      edge.PublisherOutputParams(seqOpt, compareOpt, outOpt)
    }
  }

  def toProto(obj: edge.PublisherOutputRequest): proto.PublisherOutputRequest = {
    val b = proto.PublisherOutputRequest.newBuilder()
    b.setKey(toProto(obj.key))
    b.setParams(toProto(obj.value))
    b.setCorrelation(obj.correlation)
    b.build()
  }
  def fromProto(msg: proto.PublisherOutputRequest): Either[String, edge.PublisherOutputRequest] = {
    if (msg.hasKey && msg.hasParams) {
      for {
        key <- fromProto(msg.getKey)
        params <- fromProto(msg.getParams)
      } yield {
        edge.PublisherOutputRequest(key, params, msg.getCorrelation)
      }
    } else {
      Left("PublisherOutputRequest missing key or params")
    }
  }

  def toProto(obj: edge.ClientOutputParams): proto.ClientOutputParams = {
    val b = proto.ClientOutputParams.newBuilder()
    obj.sessionOpt.map(toProto).foreach(b.setSession)
    obj.sequenceOpt.map(toOptionUInt64).foreach(b.setSequence)
    obj.compareValueOpt.map(ValueConversions.toProto).foreach(b.setCompareValue)
    obj.outputValueOpt.map(ValueConversions.toProto).foreach(b.setOutputValue)
    b.build()
  }
  def fromProto(msg: proto.ClientOutputParams): Either[String, edge.ClientOutputParams] = {
    val sessionEither = if (msg.hasSession) fromProto(msg.getSession).map(r => Some(r)) else Right(None)
    val compareEither = if (msg.hasCompareValue) ValueConversions.fromProto(msg.getCompareValue).map(r => Some(r)) else Right(None)
    val outputEither = if (msg.hasOutputValue) ValueConversions.fromProto(msg.getOutputValue).map(r => Some(r)) else Right(None)
    val seqOpt = if (msg.hasSequence) Some(msg.getSequence.getValue) else None
    for {
      sessOpt <- sessionEither
      compareOpt <- compareEither
      outOpt <- outputEither
    } yield {
      edge.ClientOutputParams(sessOpt, seqOpt, compareOpt, outOpt)
    }
  }

  def toProto(obj: edge.ClientOutputRequest): proto.ClientOutputRequest = {
    val b = proto.ClientOutputRequest.newBuilder()
    b.setKey(toProto(obj.key))
    b.setParams(toProto(obj.value))
    b.setCorrelation(obj.correlation)
    b.build()
  }
  def fromProto(msg: proto.ClientOutputRequest): Either[String, edge.ClientOutputRequest] = {
    if (msg.hasKey && msg.hasParams) {
      for {
        key <- fromProto(msg.getKey)
        params <- fromProto(msg.getParams)
      } yield {
        edge.ClientOutputRequest(key, params, msg.getCorrelation)
      }
    } else {
      Left("ClientOutputParams missing key or params")
    }
  }

  def toProto(obj: edge.OutputSuccess): proto.OutputSuccess = {
    val b = proto.OutputSuccess.newBuilder()
    obj.valueOpt.map(ValueConversions.toProto)
    b.build()
  }
  def fromProto(msg: proto.OutputSuccess): Either[String, edge.OutputSuccess] = {
    if (msg.hasResult) {
      ValueConversions.fromProto(msg.getResult).map(r => edge.OutputSuccess(Some(r)))
    } else {
      Right(edge.OutputSuccess(None))
    }
  }
  def toProto(obj: edge.OutputFailure): proto.OutputFailure = {
    proto.OutputFailure.newBuilder().setMessage(obj.reason).build()
  }

  def toProto(obj: edge.OutputResult): proto.OutputResult = {
    obj match {
      case r: edge.OutputSuccess => proto.OutputResult.newBuilder().setSuccess(toProto(r)).build()
      case r: edge.OutputFailure => proto.OutputResult.newBuilder().setFailure(toProto(r)).build()
      case _ => throw new IllegalArgumentException("Unrecognized OutputResult: " + obj)
    }
  }
  def fromProto(msg: proto.OutputResult): Either[String, edge.OutputResult] = {
    msg.getResultCase match {
      case proto.OutputResult.ResultCase.FAILURE => Right(edge.OutputFailure(msg.getFailure.getMessage))
      case proto.OutputResult.ResultCase.SUCCESS => fromProto(msg.getSuccess)
      case _ => Left("OutputResult type unrecognized")
    }
  }

  def toProto(obj: edge.PublisherOutputRequestMessage): proto.PublisherOutputRequestMessage = {
    val b = proto.PublisherOutputRequestMessage.newBuilder()
    obj.requests.map(toProto).foreach(b.addRequests)
    b.build()
  }
  def fromProto(msg: proto.PublisherOutputRequestMessage): Either[String, edge.PublisherOutputRequestMessage] = {
    rightSequence(msg.getRequestsList.map(fromProto)).map(seq => edge.PublisherOutputRequestMessage(seq))
  }

  def toProto(obj: edge.PublisherOutputResponseMessage): proto.PublisherOutputResponseMessage = {
    val b = proto.PublisherOutputResponseMessage.newBuilder()
    obj.results.foreach { case (corr, res) => b.putResults(corr, toProto(res)) }
    b.build()
  }
  def fromProto(msg: proto.PublisherOutputResponseMessage): Either[String, edge.PublisherOutputResponseMessage] = {
    val mapped = msg.getResultsMap.map { case (corr, res) => fromProto(res).map(r => (corr.toLong, r)) }.toSeq
    rightSequence(mapped).map(r => edge.PublisherOutputResponseMessage(r))
  }

  def toProto(obj: edge.ClientOutputRequestMessage): proto.ClientOutputRequestMessage = {
    val b = proto.ClientOutputRequestMessage.newBuilder()
    obj.requests.map(toProto).foreach(b.addRequests)
    b.build()
  }
  def fromProto(msg: proto.ClientOutputRequestMessage): Either[String, edge.ClientOutputRequestMessage] = {
    rightSequence(msg.getRequestsList.map(fromProto)).map(seq => edge.ClientOutputRequestMessage(seq))
  }

  def toProto(obj: edge.ClientOutputResponseMessage): proto.ClientOutputResponseMessage = {
    val b = proto.ClientOutputResponseMessage.newBuilder()
    obj.results.foreach { case (corr, res) => b.putResults(corr, toProto(res)) }
    b.build()
  }
  def fromProto(msg: proto.ClientOutputResponseMessage): Either[String, edge.ClientOutputResponseMessage] = {
    val mapped = msg.getResultsMap.map { case (corr, res) => fromProto(res).map(r => (corr.toLong, r)) }.toSeq
    rightSequence(mapped).map(r => edge.ClientOutputResponseMessage(r))
  }

}

object ConversionUtil {

  def rightSequence[L, R](seq: Seq[Either[L, R]]): Either[L, Seq[R]] = {
    var continue = true
    var leftOpt = Option.empty[L]
    val results = Vector.newBuilder[R]
    val iter = seq.iterator
    while (continue && iter.hasNext) {
      val obj = iter.next()
      obj match {
        case Left(l) =>
          leftOpt = Some(l)
          continue = false
        case Right(r) =>
          results += r
      }
    }
    leftOpt match {
      case None => Right(results.result())
      case Some(l) => Left(l)
    }
  }
}

object ValueConversions {
  import ConversionUtil._

  def fromProtoSimple(msg: proto.UUID): java.util.UUID = {
    new java.util.UUID(msg.getHigh, msg.getLow)
  }
  def toProto(uuid: java.util.UUID): proto.UUID = {
    proto.UUID.newBuilder().setLow(uuid.getLeastSignificantBits).setHigh(uuid.getMostSignificantBits).build()
  }

  def fromProto(msg: proto.ArrayValue): Either[String, edge.ValueArray] = {
    rightSequence(msg.getElementList.map(fromProto).toVector).map(s => edge.ValueArray(s.toIndexedSeq))
  }
  def toProto(obj: edge.ValueArray): proto.ArrayValue = {
    val b = proto.ArrayValue.newBuilder()
    obj.seq.foreach(v => b.addElement(toProto(v)))
    b.build()
  }

  def fromProto(msg: proto.ObjectValue): Either[String, edge.ValueObject] = {
    val fields = msg.getFieldsMap.toVector.map { case (k, v) => fromProto(v).map(ve => (k, ve)) }
    rightSequence(fields).map { all =>
      edge.ValueObject(all.toMap)
    }
  }
  def toProto(obj: edge.ValueObject): proto.ObjectValue = {
    val b = proto.ObjectValue.newBuilder()
    obj.map.foreach {
      case (k, v) =>
        b.putFields(k, toProto(v))
    }
    b.build()
  }

  def toOptionUInt64(v: Long): proto.OptionalUInt64 = {
    proto.OptionalUInt64.newBuilder().setValue(v).build()
  }
  def toOptionString(s: String): proto.OptionalString = {
    proto.OptionalString.newBuilder().setValue(s).build()
  }
  def toOptionBool(s: Boolean): proto.OptionalBool = {
    proto.OptionalBool.newBuilder().setValue(s).build()
  }

  def fromProtoSimple(msg: proto.StringValue): edge.ValueString = {
    val mimeOpt = if (msg.hasMimeType) Some(msg.getMimeType.getValue) else None
    edge.ValueString(msg.getValue, mimeOpt)
  }

  def toProto(obj: edge.ValueString): proto.StringValue = {
    val b = proto.StringValue.newBuilder().setValue(obj.v)
    obj.mimeType.foreach(s => b.setMimeType(toOptionString(s)))
    b.build()
  }

  def fromProtoSimple(msg: proto.BytesValue): edge.ValueBytes = {
    val mimeOpt = if (msg.hasMimeType) Some(msg.getMimeType.getValue) else None
    val textOpt = if (msg.hasIsText) Some(msg.getIsText.getValue) else None
    edge.ValueBytes(msg.getValue.toByteArray, mimeOpt, textOpt)
  }

  def toProto(obj: edge.ValueBytes): proto.BytesValue = {
    val b = proto.BytesValue.newBuilder().setValue(ByteString.copyFrom(obj.v))
    obj.mimeType.foreach(v => b.setMimeType(toOptionString(v)))
    obj.isText.foreach(v => b.setIsText(toOptionBool(v)))
    b.build()
  }

  def fromProto(msg: proto.Value): Either[String, edge.Value] = {
    import proto.Value.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(edge.ValueBool(msg.getBoolValue))
      case ValueTypesCase.FLOAT_VALUE => Right(edge.ValueFloat(msg.getFloatValue))
      case ValueTypesCase.DOUBLE_VALUE => Right(edge.ValueDouble(msg.getDoubleValue))
      case ValueTypesCase.SINT32_VALUE => Right(edge.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(edge.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(edge.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(edge.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.STRING_VALUE => Right(fromProtoSimple(msg.getStringValue))
      case ValueTypesCase.BYTES_VALUE => Right(fromProtoSimple(msg.getBytesValue))
      case ValueTypesCase.UUID_VALUE => Right(edge.ValueUuid(fromProtoSimple(msg.getUuidValue)))
      case ValueTypesCase.ARRAY_VALUE => fromProto(msg.getArrayValue)
      case ValueTypesCase.OBJECT_VALUE => fromProto(msg.getObjectValue)
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: edge.Value): proto.Value = {
    obj match {
      case edge.ValueBool(v) => proto.Value.newBuilder().setBoolValue(v).build()
      case edge.ValueFloat(v) => proto.Value.newBuilder().setDoubleValue(v).build()
      case edge.ValueDouble(v) => proto.Value.newBuilder().setDoubleValue(v).build()
      case edge.ValueInt32(v) => proto.Value.newBuilder().setSint32Value(v).build()
      case edge.ValueUInt32(v) => proto.Value.newBuilder().setUint32Value(v.toInt).build()
      case edge.ValueInt64(v) => proto.Value.newBuilder().setSint64Value(v).build()
      case edge.ValueUInt64(v) => proto.Value.newBuilder().setUint64Value(v).build()
      case v: edge.ValueString => proto.Value.newBuilder().setStringValue(toProto(v)).build()
      case v: edge.ValueBytes => proto.Value.newBuilder().setBytesValue(toProto(v)).build()
      case v: edge.ValueUuid => proto.Value.newBuilder().setUuidValue(toProto(v.v)).build()
      case v: edge.ValueArray => proto.Value.newBuilder().setArrayValue(toProto(v)).build()
      case v: edge.ValueObject => proto.Value.newBuilder().setObjectValue(toProto(v)).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }

  def fromProto(msg: proto.IndexableValue): Either[String, edge.IndexableValue] = {
    import proto.IndexableValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(edge.ValueBool(msg.getBoolValue))
      case ValueTypesCase.FLOAT_VALUE => Right(edge.ValueFloat(msg.getFloatValue))
      case ValueTypesCase.DOUBLE_VALUE => Right(edge.ValueDouble(msg.getDoubleValue))
      case ValueTypesCase.SINT32_VALUE => Right(edge.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(edge.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(edge.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(edge.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.STRING_VALUE => Right(fromProtoSimple(msg.getStringValue))
      case ValueTypesCase.BYTES_VALUE => Right(fromProtoSimple(msg.getBytesValue))
      case ValueTypesCase.UUID_VALUE => Right(edge.ValueUuid(fromProtoSimple(msg.getUuidValue)))
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: edge.IndexableValue): proto.IndexableValue = {
    obj match {
      case edge.ValueBool(v) => proto.IndexableValue.newBuilder().setBoolValue(v).build()
      case edge.ValueFloat(v) => proto.IndexableValue.newBuilder().setDoubleValue(v).build()
      case edge.ValueDouble(v) => proto.IndexableValue.newBuilder().setDoubleValue(v).build()
      case edge.ValueInt32(v) => proto.IndexableValue.newBuilder().setSint32Value(v).build()
      case edge.ValueUInt32(v) => proto.IndexableValue.newBuilder().setUint32Value(v.toInt).build()
      case edge.ValueInt64(v) => proto.IndexableValue.newBuilder().setSint64Value(v).build()
      case edge.ValueUInt64(v) => proto.IndexableValue.newBuilder().setUint64Value(v).build()
      case v: edge.ValueString => proto.IndexableValue.newBuilder().setStringValue(toProto(v)).build()
      case v: edge.ValueBytes => proto.IndexableValue.newBuilder().setBytesValue(toProto(v)).build()
      case v: edge.ValueUuid => proto.IndexableValue.newBuilder().setUuidValue(toProto(v.v)).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }

  def fromProto(msg: proto.SampleValue): Either[String, edge.SampleValue] = {
    import proto.SampleValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.BOOL_VALUE => Right(edge.ValueBool(msg.getBoolValue))
      case ValueTypesCase.FLOAT_VALUE => Right(edge.ValueFloat(msg.getFloatValue))
      case ValueTypesCase.DOUBLE_VALUE => Right(edge.ValueDouble(msg.getDoubleValue))
      case ValueTypesCase.SINT32_VALUE => Right(edge.ValueInt32(msg.getSint32Value))
      case ValueTypesCase.UINT32_VALUE => Right(edge.ValueUInt32(msg.getUint32Value))
      case ValueTypesCase.SINT64_VALUE => Right(edge.ValueInt64(msg.getSint64Value))
      case ValueTypesCase.UINT64_VALUE => Right(edge.ValueUInt64(msg.getUint64Value))
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: edge.SampleValue): proto.SampleValue = {
    obj match {
      case edge.ValueBool(v) => proto.SampleValue.newBuilder().setBoolValue(v).build()
      case edge.ValueFloat(v) => proto.SampleValue.newBuilder().setDoubleValue(v).build()
      case edge.ValueDouble(v) => proto.SampleValue.newBuilder().setDoubleValue(v).build()
      case edge.ValueInt32(v) => proto.SampleValue.newBuilder().setSint32Value(v).build()
      case edge.ValueUInt32(v) => proto.SampleValue.newBuilder().setUint32Value(v.toInt).build()
      case edge.ValueInt64(v) => proto.SampleValue.newBuilder().setSint64Value(v).build()
      case edge.ValueUInt64(v) => proto.SampleValue.newBuilder().setUint64Value(v).build()
      case other => throw new IllegalArgumentException(s"Conversion not implemented for $other")
    }
  }
}

object Test {
  def main(args: Array[String]): Unit = {

    val jdouble = proto.Value.newBuilder()
      .setDoubleValue(3.44)
      .build()

    val jbool = proto.Value.newBuilder()
      .setBoolValue(false)
      .build()

    val objBuilder = proto.ObjectValue.newBuilder()
      .putFields("field1", jdouble)
      .putFields("field2", jbool)

    val obj = objBuilder.build()

    val jmap = proto.Value.newBuilder().setObjectValue(obj).build()

    val obj2Builder = obj.toBuilder.putFields("fieldrecur", jmap)

    val jmap2 = proto.Value.newBuilder().setObjectValue(obj2Builder).build()

    /*val cdouble = proto.Value.newBuilder()
      .setDoubleValue(proto.ValueDouble.newBuilder().setValue(3.3))
      .build

    val cbool = proto.Value.newBuilder()
      .setBoolValue(proto.ValueBool.newBuilder().setValue(false))
      .build*/

    val printer = JsonFormat.printer()
    //val printer = new Printer()

    println(printer.print(jdouble))
    println(printer.print(jbool))
    println(printer.print(jmap))
    println(printer.print(jmap2))
    //    println(printer.print(cdouble))
    //    println(printer.print(cbool))

    val msg = ClientToServerMessage.newBuilder().putSubscriptionsAdded(0, proto.ClientSubscriptionParams.newBuilder().addEndpointSetPrefix(proto.Path.newBuilder().addPart("part")).build())
    println(msg)
    println(printer.print(msg))
  }
}