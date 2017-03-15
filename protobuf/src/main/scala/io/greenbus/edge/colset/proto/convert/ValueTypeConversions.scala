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
package io.greenbus.edge.colset.proto.convert

import io.greenbus.edge.colset
import io.greenbus.edge.colset._

import scala.collection.JavaConverters._

object StreamConversions {
  import io.greenbus.edge.proto.convert.ConversionUtil._
  import ValueTypeConversions._

  def sessionFromProto(msg: proto.PeerSessionId): Either[String, colset.PeerSessionId] = {
    if (msg.hasPersistenceId) {
      Right(colset.PeerSessionId(ValueTypeConversions.fromProtoSimple(msg.getPersistenceId), msg.getInstanceId))
    } else {
      Left("PeerSessionId missing persistence id")
    }
  }
  def sessionToProto(obj: colset.PeerSessionId): proto.PeerSessionId = {
    val b = proto.PeerSessionId.newBuilder()
    b.setPersistenceId(ValueTypeConversions.toProtoSimple(obj.persistenceId))
    b.setInstanceId(obj.instanceId)
    b.build()
  }

  def rowIdFromProto(msg: proto.RowId): Either[String, colset.RowId] = {
    if (msg.hasRoutingKey && msg.hasRowKey) {
      for {
        route <- fromProto(msg.getRoutingKey)
        row <- fromProto(msg.getRowKey)
      } yield {
        colset.RowId(route, msg.getTable, row)
      }
    } else {
      Left("RowId missing routing key or row key")
    }
  }
  def rowIdToProto(obj: colset.RowId): proto.RowId = {
    val b = proto.RowId.newBuilder()
    b.setRoutingKey(toProto(obj.routingKey))
    b.setTable(obj.table)
    b.setRowKey(toProto(obj.routingKey))
    b.build()
  }

  def modSetDeltaFromProto(msg: proto.ModifiedSetDelta): Either[String, colset.ModifiedSetDelta] = {
    if (msg.hasSequence) {
      for {
        sequence <- sequencedFromProto(msg.getSequence)
        removes <- rightSequence(msg.getRemovesList.asScala.map(fromProto))
        adds <- rightSequence(msg.getAddsList.asScala.map(fromProto))
      } yield {
        colset.ModifiedSetDelta(sequence, removes.toSet, adds.toSet)
      }
    } else {
      Left("ModifiedSetDelta missing sequence")
    }
  }
  def modSetDeltaToProto(obj: colset.ModifiedSetDelta): proto.ModifiedSetDelta = {
    val b = proto.ModifiedSetDelta.newBuilder()
    b.setSequence(sequencedToProto(obj.sequence))
    obj.removes.map(toProto).foreach(b.addRemoves)
    obj.adds.map(toProto).foreach(b.addAdds)
    b.build()
  }

  def modSetSnapFromProto(msg: proto.ModifiedSetSnapshot): Either[String, colset.ModifiedSetSnapshot] = {
    if (msg.hasSequence) {
      for {
        sequence <- sequencedFromProto(msg.getSequence)
        elements <- rightSequence(msg.getElementsList.asScala.map(fromProto))
      } yield {
        colset.ModifiedSetSnapshot(sequence, elements.toSet)
      }
    } else {
      Left("ModifiedSetSnapshot missing sequence")
    }
  }
  def modSetSnapToProto(obj: colset.ModifiedSetSnapshot): proto.ModifiedSetSnapshot = {
    val b = proto.ModifiedSetSnapshot.newBuilder()
    b.setSequence(sequencedToProto(obj.sequence))
    obj.snapshot.map(toProto).foreach(b.addElements)
    b.build()
  }

  def keyPairFromProto(msg: proto.TypeValueKeyPair): Either[String, (colset.TypeValue, colset.TypeValue)] = {
    if (msg.hasKey && msg.hasValue) {
      for {
        key <- fromProto(msg.getKey)
        value <- fromProto(msg.getValue)
      } yield {
        (key, value)
      }
    } else {
      Left("RowId missing routing key or row key")
    }
  }
  def keyPairToProto(obj: (colset.TypeValue, colset.TypeValue)): proto.TypeValueKeyPair = {
    val b = proto.TypeValueKeyPair.newBuilder()
    b.setKey(toProto(obj._1))
    b.setValue(toProto(obj._2))
    b.build()
  }

  def modKeySetDeltaFromProto(msg: proto.ModifiedKeyedSetDelta): Either[String, colset.ModifiedKeyedSetDelta] = {
    if (msg.hasSequence) {
      for {
        sequence <- sequencedFromProto(msg.getSequence)
        removes <- rightSequence(msg.getRemovesList.asScala.map(fromProto))
        adds <- rightSequence(msg.getAddsList.asScala.map(keyPairFromProto))
        modifies <- rightSequence(msg.getModifiesList.asScala.map(keyPairFromProto))
      } yield {
        colset.ModifiedKeyedSetDelta(sequence, removes.toSet, adds.toSet, modifies.toSet)
      }
    } else {
      Left("ModifiedKeyedSetDelta missing sequence")
    }
  }
  def modKeySetDeltaToProto(obj: colset.ModifiedKeyedSetDelta): proto.ModifiedKeyedSetDelta = {
    val b = proto.ModifiedKeyedSetDelta.newBuilder()
    b.setSequence(sequencedToProto(obj.sequence))
    obj.removes.map(toProto).foreach(b.addRemoves)
    obj.adds.map(keyPairToProto).foreach(b.addAdds)
    obj.modifies.map(keyPairToProto).foreach(b.addModifies)
    b.build()
  }

  def modKeySetSnapFromProto(msg: proto.ModifiedKeyedSetSnapshot): Either[String, colset.ModifiedKeyedSetSnapshot] = {
    if (msg.hasSequence) {
      for {
        sequence <- sequencedFromProto(msg.getSequence)
        elements <- rightSequence(msg.getElementsList.asScala.map(keyPairFromProto))
      } yield {
        colset.ModifiedKeyedSetSnapshot(sequence, elements.toMap)
      }
    } else {
      Left("ModifiedKeyedSetSnapshot missing sequence")
    }
  }
  def modKeySetSnapToProto(obj: colset.ModifiedKeyedSetSnapshot): proto.ModifiedKeyedSetSnapshot = {
    val b = proto.ModifiedKeyedSetSnapshot.newBuilder()
    b.setSequence(sequencedToProto(obj.sequence))
    obj.snapshot.map(keyPairToProto).foreach(b.addElements)
    b.build()
  }

  def appSetFromProto(msg: proto.AppendSetValue): Either[String, colset.AppendSetValue] = {
    if (msg.hasSequence && msg.hasValue) {
      for {
        sequence <- sequencedFromProto(msg.getSequence)
        value <- fromProto(msg.getValue)
      } yield {
        colset.AppendSetValue(sequence, value)
      }
    } else {
      Left("AppendSetValue missing sequence or value")
    }
  }
  def appSetToProto(obj: colset.AppendSetValue): proto.AppendSetValue = {
    val b = proto.AppendSetValue.newBuilder()
    b.setSequence(sequencedToProto(obj.sequence))
    b.setValue(toProto(obj.value))
    b.build()
  }

  def appSetSeqFromProto(msg: proto.AppendSetSequence): Either[String, colset.AppendSetSequence] = {
    rightSequence(msg.getValuesList.asScala.map(appSetFromProto))
      .map(seq => colset.AppendSetSequence(seq))
  }
  def appSetSeqToProto(obj: colset.AppendSetSequence): proto.AppendSetSequence = {
    val b = proto.AppendSetSequence.newBuilder()
    obj.appends.map(appSetToProto).foreach(b.addValues)
    b.build()
  }

  def setDeltaFromProto(msg: proto.SetDelta): Either[String, colset.SetDelta] = {
    import proto.SetDelta.SetTypesCase
    msg.getSetTypesCase match {
      case SetTypesCase.MODIFIED_SET_DELTA => modSetDeltaFromProto(msg.getModifiedSetDelta)
      case SetTypesCase.MODIFIED_KEYED_SET_DELTA => modKeySetDeltaFromProto(msg.getModifiedKeyedSetDelta)
      case SetTypesCase.APPEND_SET_SEQUENCE => appSetSeqFromProto(msg.getAppendSetSequence)
      case _ => Left("Unrecognizable SetDelta type")
    }
  }
  def setDeltaToProto(obj: colset.SetDelta): proto.SetDelta = {
    val b = proto.SetDelta.newBuilder()
    obj match {
      case v: colset.ModifiedSetDelta => b.setModifiedSetDelta(modSetDeltaToProto(v))
      case v: colset.ModifiedKeyedSetDelta => b.setModifiedKeyedSetDelta(modKeySetDeltaToProto(v))
      case v: colset.AppendSetSequence => b.setAppendSetSequence(appSetSeqToProto(v))
      case _ => throw new IllegalArgumentException("Unrecognized SetDelta type")
    }
    b.build()
  }

  def setSnapshotFromProto(msg: proto.SetSnapshot): Either[String, colset.SetSnapshot] = {
    import proto.SetSnapshot.SetTypesCase
    msg.getSetTypesCase match {
      case SetTypesCase.MODIFIED_SET_SNAPSHOT => modSetSnapFromProto(msg.getModifiedSetSnapshot)
      case SetTypesCase.MODIFIED_KEYED_SET_SNAPSHOT => modKeySetSnapFromProto(msg.getModifiedKeyedSetSnapshot)
      case SetTypesCase.APPEND_SET_SEQUENCE => appSetSeqFromProto(msg.getAppendSetSequence)
      case _ => Left("Unrecognizable SetSnapshot type")
    }
  }
  def setSnapshotToProto(obj: colset.SetSnapshot): proto.SetSnapshot = {
    val b = proto.SetSnapshot.newBuilder()
    obj match {
      case v: colset.ModifiedSetSnapshot => b.setModifiedSetSnapshot(modSetSnapToProto(v))
      case v: colset.ModifiedKeyedSetSnapshot => b.setModifiedKeyedSetSnapshot(modKeySetSnapToProto(v))
      case v: colset.AppendSetSequence => b.setAppendSetSequence(appSetSeqToProto(v))
      case _ => throw new IllegalArgumentException("Unrecognized SetSnapshot type")
    }
    b.build()
  }

  def streamDeltaFromProto(msg: proto.StreamDelta): Either[String, colset.StreamDelta] = {
    if (msg.hasUpdate) {
      for {
        update <- setDeltaFromProto(msg.getUpdate)
      } yield {
        colset.StreamDelta(update)
      }
    } else {
      Left("StreamDelta missing update")
    }
  }
  def streamDeltaToProto(obj: colset.StreamDelta): proto.StreamDelta = {
    val b = proto.StreamDelta.newBuilder()
    b.setUpdate(setDeltaToProto(obj.update))
    b.build()
  }

  def resyncSnapshotFromProto(msg: proto.ResyncSnapshot): Either[String, colset.ResyncSnapshot] = {
    if (msg.hasSnapshot) {
      for {
        update <- setSnapshotFromProto(msg.getSnapshot)
      } yield {
        colset.ResyncSnapshot(update)
      }
    } else {
      Left("ResyncSnapshot missing update")
    }
  }
  def resyncSnapshotToProto(obj: colset.ResyncSnapshot): proto.ResyncSnapshot = {
    val b = proto.ResyncSnapshot.newBuilder()
    b.setSnapshot(setSnapshotToProto(obj.snapshot))
    b.build()
  }

  def resyncSessionFromProto(msg: proto.ResyncSession): Either[String, colset.ResyncSession] = {
    if (msg.hasSessionId && msg.hasSnapshot) {
      for {
        session <- sessionFromProto(msg.getSessionId)
        update <- setSnapshotFromProto(msg.getSnapshot)
      } yield {
        colset.ResyncSession(session, update)
      }
    } else {
      Left("ResyncSession missing update")
    }
  }
  def resyncSessionToProto(obj: colset.ResyncSession): proto.ResyncSession = {
    val b = proto.ResyncSession.newBuilder()
    b.setSessionId(sessionToProto(obj.sessionId))
    b.setSnapshot(setSnapshotToProto(obj.snapshot))
    b.build()
  }

  def appendFromProto(msg: proto.AppendEvent): Either[String, colset.AppendEvent] = {
    import proto.AppendEvent.AppendTypesCase
    msg.getAppendTypesCase match {
      case AppendTypesCase.STREAM_DELTA => streamDeltaFromProto(msg.getStreamDelta)
      case AppendTypesCase.RESYNC_SNAPSHOT => resyncSnapshotFromProto(msg.getResyncSnapshot)
      case AppendTypesCase.RESYNC_SESSION => resyncSessionFromProto(msg.getResyncSession)
      case _ => Left("Unrecognizable AppendEvent type")
    }
  }
  def appendToProto(obj: colset.AppendEvent): proto.AppendEvent = {
    val b = proto.AppendEvent.newBuilder()
    obj match {
      case v: colset.StreamDelta => b.setStreamDelta(streamDeltaToProto(v))
      case v: colset.ResyncSnapshot => b.setResyncSnapshot(resyncSnapshotToProto(v))
      case v: colset.ResyncSession => b.setResyncSession(resyncSessionToProto(v))
      case _ => throw new IllegalArgumentException("Unrecognized AppendEvent type")
    }
    b.build()
  }

  def rowAppendFromProto(msg: proto.RowAppendEvent): Either[String, colset.RowAppendEvent] = {
    if (msg.hasRow && msg.hasAppend) {
      for {
        row <- rowIdFromProto(msg.getRow)
        append <- appendFromProto(msg.getAppend)
      } yield {
        colset.RowAppendEvent(row, append)
      }
    } else {
      Left("RowAppendEvent missing update")
    }
  }
  def rowAppendToProto(obj: colset.RowAppendEvent): proto.RowAppendEvent = {
    val b = proto.RowAppendEvent.newBuilder()
    b.setRow(rowIdToProto(obj.rowId))
    b.setAppend(appendToProto(obj.appendEvent))
    b.build()
  }

  def unresolvedFromProto(msg: proto.RouteUnresolved): Either[String, colset.RouteUnresolved] = {
    if (msg.hasRoutingKey) {
      for {
        routingKey <- fromProto(msg.getRoutingKey)
      } yield {
        colset.RouteUnresolved(routingKey)
      }
    } else {
      Left("RouteUnresolved missing routing key")
    }
  }
  def unresolvedToProto(obj: colset.RouteUnresolved): proto.RouteUnresolved = {
    val b = proto.RouteUnresolved.newBuilder()
    b.setRoutingKey(toProto(obj.routingKey))
    b.build()
  }

  def streamFromProto(msg: proto.StreamEvent): Either[String, colset.StreamEvent] = {
    import proto.StreamEvent.EventTypeCase
    msg.getEventTypeCase match {
      case EventTypeCase.ROW_APPEND => rowAppendFromProto(msg.getRowAppend)
      case EventTypeCase.ROUTE_UNRESOLVED => unresolvedFromProto(msg.getRouteUnresolved)
      case _ => Left("Unrecognizable StreamEvent type")
    }
  }
  def streamToProto(obj: colset.StreamEvent): proto.StreamEvent = {
    val b = proto.StreamEvent.newBuilder()
    obj match {
      case v: colset.RowAppendEvent => b.setRowAppend(rowAppendToProto(v))
      case v: colset.RouteUnresolved => b.setRouteUnresolved(unresolvedToProto(v))
      case _ => throw new IllegalArgumentException("Unrecognized StreamEvent type")
    }
    b.build()
  }
}

object ProtocolConversions {
  import io.greenbus.edge.proto.convert.ConversionUtil._
  import ValueTypeConversions._
  import StreamConversions._

  def servReqFromProto(msg: proto.ServiceRequest): Either[String, colset.ServiceRequest] = {
    if (msg.hasRow && msg.hasValue && msg.hasCorrelation) {
      for {
        row <- rowIdFromProto(msg.getRow)
        value <- fromProto(msg.getValue)
        correlation <- fromProto(msg.getCorrelation)
      } yield {
        colset.ServiceRequest(row, value, correlation)
      }
    } else {
      Left("ServiceRequest missing field(s)")
    }
  }
  def servReqToProto(obj: colset.ServiceRequest): proto.ServiceRequest = {
    val b = proto.ServiceRequest.newBuilder()
    b.setRow(rowIdToProto(obj.row))
    b.setValue(toProto(obj.value))
    b.setCorrelation(toProto(obj.correlation))
    b.build()
  }

  def servRespFromProto(msg: proto.ServiceResponse): Either[String, colset.ServiceResponse] = {
    if (msg.hasRow && msg.hasValue && msg.hasCorrelation) {
      for {
        row <- rowIdFromProto(msg.getRow)
        value <- fromProto(msg.getValue)
        correlation <- fromProto(msg.getCorrelation)
      } yield {
        colset.ServiceResponse(row, value, correlation)
      }
    } else {
      Left("ServiceResponse missing field(s)")
    }
  }
  def servRespToProto(obj: colset.ServiceResponse): proto.ServiceResponse = {
    val b = proto.ServiceResponse.newBuilder()
    b.setRow(rowIdToProto(obj.row))
    b.setValue(toProto(obj.value))
    b.setCorrelation(toProto(obj.correlation))
    b.build()
  }

  def servReqBatchFromProto(msg: proto.ServiceRequestBatch): Either[String, colset.ServiceRequestBatch] = {
    rightSequence(msg.getRequestsList.asScala.map(servReqFromProto))
      .map(seq => colset.ServiceRequestBatch(seq))
  }
  def servReqBatchToProto(obj: colset.ServiceRequestBatch): proto.ServiceRequestBatch = {
    val b = proto.ServiceRequestBatch.newBuilder()
    obj.requests.map(servReqToProto).foreach(b.addRequests)
    b.build()
  }

  def servRespBatchFromProto(msg: proto.ServiceResponseBatch): Either[String, colset.ServiceResponseBatch] = {
    rightSequence(msg.getResponsesList.asScala.map(servRespFromProto))
      .map(seq => colset.ServiceResponseBatch(seq))
  }
  def servRespBatchToProto(obj: colset.ServiceResponseBatch): proto.ServiceResponseBatch = {
    val b = proto.ServiceResponseBatch.newBuilder()
    obj.responses.map(servRespToProto).foreach(b.addResponses)
    b.build()
  }

  def subSetFromProto(msg: proto.SubscriptionSetUpdate): Either[String, colset.SubscriptionSetUpdate] = {
    rightSequence(msg.getRowsList.asScala.map(rowIdFromProto))
      .map(seq => colset.SubscriptionSetUpdate(seq.toSet))
  }
  def subSetToProto(obj: colset.SubscriptionSetUpdate): proto.SubscriptionSetUpdate = {
    val b = proto.SubscriptionSetUpdate.newBuilder()
    obj.rows.map(rowIdToProto).foreach(b.addRows)
    b.build()
  }

  def eventBatchFromProto(msg: proto.EventBatch): Either[String, colset.EventBatch] = {
    rightSequence(msg.getEventsList.asScala.map(streamFromProto))
      .map(seq => colset.EventBatch(seq))
  }
  def eventBatchToProto(obj: colset.EventBatch): proto.EventBatch = {
    val b = proto.EventBatch.newBuilder()
    obj.events.map(streamToProto).foreach(b.addEvents)
    b.build()
  }

  def gatewayEventsFromProto(msg: proto.GatewayClientEvents): Either[String, colset.GatewayClientEvents] = {
    val routesOptEither: Either[String, Option[Set[TypeValue]]] = if (msg.hasRoutesUpdate) {
      rightSequence(msg.getRoutesUpdate.getValuesList.asScala.map(fromProto)).map(r => Some(r.toSet))
    } else {
      Right(None)
    }

    for {
      routesOpt <- routesOptEither
      events <- rightSequence(msg.getEventsList.asScala.map(streamFromProto))
    } yield {
      colset.GatewayClientEvents(routesOpt, events)
    }
  }
  def gatewayEventsToProto(obj: colset.GatewayClientEvents): proto.GatewayClientEvents = {
    val b = proto.GatewayClientEvents.newBuilder()

    obj.routesUpdate.map { set =>
      val tvb = proto.OptionalTypeValueArray.newBuilder()
      set.map(toProto).foreach(tvb.addValues)
      tvb.build()
    }.foreach(b.setRoutesUpdate)

    obj.events.map(streamToProto).foreach(b.addEvents)
    b.build()
  }
}

object ValueTypeConversions {
  import io.greenbus.edge.proto.convert.ConversionUtil._

  def fromProtoSimple(msg: proto.UUID): java.util.UUID = {
    new java.util.UUID(msg.getHigh, msg.getLow)
  }
  def toProtoSimple(uuid: java.util.UUID): proto.UUID = {
    proto.UUID.newBuilder().setLow(uuid.getLeastSignificantBits).setHigh(uuid.getMostSignificantBits).build()
  }

  def tupleFromProto(msg: proto.TupleValue): Either[String, colset.TupleVal] = {
    rightSequence(msg.getElementList.asScala.map(fromProto).toVector).map(s => colset.TupleVal(s.toIndexedSeq))
  }
  def tupleToProto(obj: colset.TupleVal): proto.TupleValue = {
    val b = proto.TupleValue.newBuilder()
    obj.elements.foreach(v => b.addElement(toProto(v)))
    b.build()
  }

  def sequencedToProto(obj: colset.SequencedTypeValue): proto.SequencedTypeValue = {
    val b = proto.SequencedTypeValue.newBuilder()
    obj match {
      case v: Int64Val => b.setSint64Value(v.v)
      //case v: colset.TupleValue => b.setTupleValue(tupleToProto(v))
      case _ => throw new IllegalArgumentException(s"Unrecognized SequencedTypeValue: $obj")
    }
    b.build()
  }
  def sequencedFromProto(msg: proto.SequencedTypeValue): Either[String, colset.SequencedTypeValue] = {
    import proto.SequencedTypeValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.SINT64_VALUE => Right(colset.Int64Val(msg.getSint64Value))
      //case ValueTypesCase.TUPLE_VALUE => tupleFromProto(msg.getTupleValue)
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: colset.TypeValue): proto.TypeValue = {
    val b = proto.TypeValue.newBuilder()
    obj match {
      case v: colset.Int64Val => b.setSint64Value(v.v)
      case v: colset.DoubleVal => b.setDoubleValue(v.v)
      case v: colset.BoolVal => b.setBoolValue(v.v)
      case v: colset.SymbolVal => b.setSymbolValue(v.v)
      case v: colset.TextVal => b.setTextValue(v.v)
      case v: colset.UuidVal => b.setUuidValue(toProtoSimple(v.v))
      case v: colset.TupleVal => b.setTupleValue(tupleToProto(v))
      case _ => throw new IllegalArgumentException(s"Unrecognized TypeValue: $obj")
    }
    b.build()
  }
  def fromProto(msg: proto.TypeValue): Either[String, colset.TypeValue] = {
    import proto.TypeValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.SINT64_VALUE => Right(colset.Int64Val(msg.getSint64Value))
      case ValueTypesCase.DOUBLE_VALUE => Right(colset.DoubleVal(msg.getDoubleValue))
      case ValueTypesCase.BOOL_VALUE => Right(colset.BoolVal(msg.getBoolValue))
      case ValueTypesCase.SYMBOL_VALUE => Right(colset.SymbolVal(msg.getSymbolValue))
      case ValueTypesCase.TEXT_VALUE => Right(colset.TextVal(msg.getTextValue))
      case ValueTypesCase.BYTES_VALUE => Right(colset.BytesVal(msg.getBytesValue.toByteArray))
      case ValueTypesCase.UUID_VALUE => Right(UuidVal(fromProtoSimple(msg.getUuidValue)))
      case ValueTypesCase.TUPLE_VALUE => tupleFromProto(msg.getTupleValue)
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }
}
