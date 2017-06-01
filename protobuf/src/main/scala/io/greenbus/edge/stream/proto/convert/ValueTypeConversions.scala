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
package io.greenbus.edge.stream.proto.convert

import com.google.protobuf.ByteString
import io.greenbus.edge.stream
import io.greenbus.edge.stream._

import scala.collection.JavaConverters._

object StreamConversions {
  import ValueTypeConversions._
  import io.greenbus.edge.util.EitherUtil._

  def sessionFromProto(msg: proto.PeerSessionId): Either[String, stream.PeerSessionId] = {
    if (msg.hasPersistenceId) {
      Right(stream.PeerSessionId(ValueTypeConversions.fromProtoSimple(msg.getPersistenceId), msg.getInstanceId))
    } else {
      Left("PeerSessionId missing persistence id")
    }
  }
  def sessionToProto(obj: stream.PeerSessionId): proto.PeerSessionId = {
    val b = proto.PeerSessionId.newBuilder()
    b.setPersistenceId(ValueTypeConversions.toProtoSimple(obj.persistenceId))
    b.setInstanceId(obj.instanceId)
    b.build()
  }

  def rowIdFromProto(msg: proto.RowId): Either[String, stream.RowId] = {
    if (msg.hasRoutingKey && msg.hasRowKey) {
      for {
        route <- fromProto(msg.getRoutingKey)
        row <- fromProto(msg.getRowKey)
      } yield {
        stream.RowId(route, msg.getTable, row)
      }
    } else {
      Left("RowId missing routing key or row key")
    }
  }
  def rowIdToProto(obj: stream.RowId): proto.RowId = {
    val b = proto.RowId.newBuilder()
    b.setRoutingKey(toProto(obj.routingKey))
    b.setTable(obj.table)
    b.setRowKey(toProto(obj.rowKey))
    b.build()
  }

  def modSetDeltaFromProto(msg: proto.SetDiff): Either[String, stream.SetDiff] = {
    for {
      removes <- rightSequence(msg.getRemovesList.asScala.map(fromProto))
      adds <- rightSequence(msg.getAddsList.asScala.map(fromProto))
    } yield {
      stream.SetDiff(removes.toSet, adds.toSet)
    }
  }
  def modSetDeltaToProto(obj: stream.SetDiff): proto.SetDiff = {
    val b = proto.SetDiff.newBuilder()
    obj.removes.map(toProto).foreach(b.addRemoves)
    obj.adds.map(toProto).foreach(b.addAdds)
    b.build()
  }

  def modSetSnapFromProto(msg: proto.SetSnapshot): Either[String, stream.SetSnapshot] = {
    for {
      elements <- rightSequence(msg.getElementsList.asScala.map(fromProto))
    } yield {
      stream.SetSnapshot(elements.toSet)
    }
  }
  def modSetSnapToProto(obj: stream.SetSnapshot): proto.SetSnapshot = {
    val b = proto.SetSnapshot.newBuilder()
    obj.snapshot.map(toProto).foreach(b.addElements)
    b.build()
  }

  def keyPairFromProto(msg: proto.TypeValueKeyPair): Either[String, (stream.TypeValue, stream.TypeValue)] = {
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
  def keyPairToProto(obj: (stream.TypeValue, stream.TypeValue)): proto.TypeValueKeyPair = {
    val b = proto.TypeValueKeyPair.newBuilder()
    b.setKey(toProto(obj._1))
    b.setValue(toProto(obj._2))
    b.build()
  }

  def modKeySetDeltaFromProto(msg: proto.MapDiff): Either[String, stream.MapDiff] = {
    for {
      removes <- rightSequence(msg.getRemovesList.asScala.map(fromProto))
      adds <- rightSequence(msg.getAddsList.asScala.map(keyPairFromProto))
      modifies <- rightSequence(msg.getModifiesList.asScala.map(keyPairFromProto))
    } yield {
      stream.MapDiff(removes.toSet, adds.toSet, modifies.toSet)
    }
  }
  def modKeySetDeltaToProto(obj: stream.MapDiff): proto.MapDiff = {
    val b = proto.MapDiff.newBuilder()
    obj.removes.map(toProto).foreach(b.addRemoves)
    obj.adds.map(keyPairToProto).foreach(b.addAdds)
    obj.modifies.map(keyPairToProto).foreach(b.addModifies)
    b.build()
  }

  def modKeySetSnapFromProto(msg: proto.MapSnapshot): Either[String, stream.MapSnapshot] = {
    for {
      elements <- rightSequence(msg.getElementsList.asScala.map(keyPairFromProto))
    } yield {
      stream.MapSnapshot(elements.toMap)
    }
  }
  def modKeySetSnapToProto(obj: stream.MapSnapshot): proto.MapSnapshot = {
    val b = proto.MapSnapshot.newBuilder()
    obj.snapshot.map(keyPairToProto).foreach(b.addElements)
    b.build()
  }

  def appSetSeqFromProto(msg: proto.AppendSnapshot): Either[String, stream.AppendSnapshot] = {
    if (msg.hasCurrent) {
      for {
        prev <- rightSequence(msg.getPreviousList.asScala.map(sequencedDiffFromProto))
        current <- sequencedDiffFromProto(msg.getCurrent)
      } yield {
        stream.AppendSnapshot(current, prev)
      }
    } else {
      Left("AppendSnapshot missing current value")
    }
  }
  def appSetSeqToProto(obj: stream.AppendSnapshot): proto.AppendSnapshot = {
    val b = proto.AppendSnapshot.newBuilder()
    b.setCurrent(sequencedDiffToProto(obj.current))
    obj.previous.map(sequencedDiffToProto).foreach(b.addPrevious)
    b.build()
  }

  def setSnapshotFromProto(msg: proto.SeqSnapshot): Either[String, stream.SequenceSnapshot] = {
    import proto.SeqSnapshot.SetTypesCase
    msg.getSetTypesCase match {
      case SetTypesCase.SET_SNAPSHOT => modSetSnapFromProto(msg.getSetSnapshot)
      case SetTypesCase.MAP_SNAPSHOT => modKeySetSnapFromProto(msg.getMapSnapshot)
      case SetTypesCase.APPEND_SET_SEQUENCE => appSetSeqFromProto(msg.getAppendSetSequence)
      case _ => Left("Unrecognizable SetSnapshot type")
    }
  }
  def setSnapshotToProto(obj: stream.SequenceSnapshot): proto.SeqSnapshot = {
    val b = proto.SeqSnapshot.newBuilder()
    obj match {
      case v: SetSnapshot => b.setSetSnapshot(modSetSnapToProto(v))
      case v: MapSnapshot => b.setMapSnapshot(modKeySetSnapToProto(v))
      case v: AppendSnapshot => b.setAppendSetSequence(appSetSeqToProto(v))
      case _ => throw new IllegalArgumentException("Unrecognized SetSnapshot type")
    }
    b.build()
  }

  def seqDiffFromProto(msg: proto.SeqDiff): Either[String, stream.SequenceTypeDiff] = {
    import proto.SeqDiff.SetTypesCase

    msg.getSetTypesCase match {
      case SetTypesCase.SET_DIFF => modSetDeltaFromProto(msg.getSetDiff)
      case SetTypesCase.MAP_DIFF => modKeySetDeltaFromProto(msg.getMapDiff)
      case SetTypesCase.APPEND_DIFF => ValueTypeConversions.fromProto(msg.getAppendDiff).map(stream.AppendValue)
      case _ => Left("Unrecognizable SetSnapshot type")
    }
  }
  def seqDiffToProto(obj: stream.SequenceTypeDiff): proto.SeqDiff = {
    val b = proto.SeqDiff.newBuilder()
    obj match {
      case v: stream.SetDiff => b.setSetDiff(modSetDeltaToProto(v))
      case v: stream.MapDiff => b.setMapDiff(modKeySetDeltaToProto(v))
      case v: stream.AppendValue => b.setAppendDiff(ValueTypeConversions.toProto(v.value))
      case _ => throw new IllegalArgumentException("Unrecognized SetDelta type")
    }
    b.build()
  }

  def sequencedDiffFromProto(msg: proto.SequencedDiff): Either[String, stream.SequencedDiff] = {
    if (msg.hasSequence && msg.hasDiff) {
      for {
        sequence <- sequencedFromProto(msg.getSequence)
        value <- seqDiffFromProto(msg.getDiff)
      } yield {
        stream.SequencedDiff(sequence, value)
      }
    } else {
      Left("AppendSetValue missing sequence or value")
    }
  }
  def sequencedDiffToProto(obj: stream.SequencedDiff): proto.SequencedDiff = {
    val b = proto.SequencedDiff.newBuilder()
    b.setSequence(sequencedToProto(obj.sequence))
    b.setDiff(seqDiffToProto(obj.diff))
    b.build()
  }

  def streamDeltaFromProto(msg: proto.StreamDelta): Either[String, stream.StreamDelta] = {
    rightSequence(msg.getUpdatesList.asScala.map(sequencedDiffFromProto)).map { seqDiffs =>
      stream.StreamDelta(stream.Delta(seqDiffs))
    }

  }
  def streamDeltaToProto(obj: stream.StreamDelta): proto.StreamDelta = {
    val b = proto.StreamDelta.newBuilder()
    obj.update.diffs.map(sequencedDiffToProto).foreach(b.addUpdates)
    b.build()
  }

  def resyncSnapshotFromProto(msg: proto.ResyncSnapshot): Either[String, stream.ResyncSnapshot] = {
    if (msg.hasSnapshot && msg.hasSequence) {
      for {
        seq <- ValueTypeConversions.sequencedFromProto(msg.getSequence)
        update <- setSnapshotFromProto(msg.getSnapshot)
      } yield {
        stream.ResyncSnapshot(Resync(seq, update))
      }
    } else {
      Left("ResyncSnapshot missing update or sequence")
    }
  }
  def resyncSnapshotToProto(obj: stream.ResyncSnapshot): proto.ResyncSnapshot = {
    val b = proto.ResyncSnapshot.newBuilder()
    b.setSnapshot(setSnapshotToProto(obj.resync.snapshot))
    b.build()
  }

  def resyncSessionFromProto(msg: proto.ResyncSession): Either[String, stream.ResyncSession] = {
    if (msg.hasSessionId && msg.hasSnapshot && msg.hasSequence && msg.hasContext) {
      for {
        session <- sessionFromProto(msg.getSessionId)
        ctx <- contextFromProto(msg.getContext)
        seq <- ValueTypeConversions.sequencedFromProto(msg.getSequence)
        update <- setSnapshotFromProto(msg.getSnapshot)
      } yield {
        stream.ResyncSession(session, ctx, Resync(seq, update))
      }
    } else {
      Left("ResyncSession missing session, snapshot, sequence, or context")
    }
  }
  def resyncSessionToProto(obj: stream.ResyncSession): proto.ResyncSession = {
    val b = proto.ResyncSession.newBuilder()
    b.setSessionId(sessionToProto(obj.sessionId))
    b.setSequence(ValueTypeConversions.sequencedToProto(obj.resync.sequence))
    b.setContext(contextToProto(obj.context))
    b.setSnapshot(setSnapshotToProto(obj.resync.snapshot))
    b.build()
  }

  def contextFromProto(msg: proto.SequenceContext): Either[String, stream.SequenceCtx] = {
    for {
      metaOpt <- if (msg.hasUserMetadata) ValueTypeConversions.fromProto(msg.getUserMetadata).map(v => Some(v)) else Right(None)
    } yield {
      stream.SequenceCtx(None, metaOpt)
    }
  }
  def contextToProto(obj: stream.SequenceCtx): proto.SequenceContext = {
    val b = proto.SequenceContext.newBuilder()
    obj.userMetadata.map(ValueTypeConversions.toProto).foreach(b.setUserMetadata)
    b.build()
  }

  def appendFromProto(msg: proto.AppendEvent): Either[String, stream.AppendEvent] = {
    import proto.AppendEvent.AppendTypesCase
    msg.getAppendTypesCase match {
      case AppendTypesCase.STREAM_DELTA => streamDeltaFromProto(msg.getStreamDelta)
      case AppendTypesCase.RESYNC_SNAPSHOT => resyncSnapshotFromProto(msg.getResyncSnapshot)
      case AppendTypesCase.RESYNC_SESSION => resyncSessionFromProto(msg.getResyncSession)
      case AppendTypesCase.STREAM_ABSENT => Right(StreamAbsent)
      case _ => Left("Unrecognizable AppendEvent type")
    }
  }
  def appendToProto(obj: stream.AppendEvent): proto.AppendEvent = {
    val b = proto.AppendEvent.newBuilder()
    obj match {
      case v: stream.StreamDelta => b.setStreamDelta(streamDeltaToProto(v))
      case v: stream.ResyncSnapshot => b.setResyncSnapshot(resyncSnapshotToProto(v))
      case v: stream.ResyncSession => b.setResyncSession(resyncSessionToProto(v))
      case stream.StreamAbsent => b.setStreamAbsent(true)
      case _ => throw new IllegalArgumentException("Unrecognized AppendEvent type")
    }
    b.build()
  }

  def rowAppendFromProto(msg: proto.RowAppendEvent): Either[String, stream.RowAppendEvent] = {
    if (msg.hasRow && msg.hasAppend) {
      for {
        row <- rowIdFromProto(msg.getRow)
        append <- appendFromProto(msg.getAppend)
      } yield {
        stream.RowAppendEvent(row, append)
      }
    } else {
      Left("RowAppendEvent missing update")
    }
  }
  def rowAppendToProto(obj: stream.RowAppendEvent): proto.RowAppendEvent = {
    val b = proto.RowAppendEvent.newBuilder()
    b.setRow(rowIdToProto(obj.rowId))
    b.setAppend(appendToProto(obj.appendEvent))
    b.build()
  }

  def unresolvedFromProto(msg: proto.RouteUnresolved): Either[String, stream.RouteUnresolved] = {
    if (msg.hasRoutingKey) {
      for {
        routingKey <- fromProto(msg.getRoutingKey)
      } yield {
        stream.RouteUnresolved(routingKey)
      }
    } else {
      Left("RouteUnresolved missing routing key")
    }
  }
  def unresolvedToProto(obj: stream.RouteUnresolved): proto.RouteUnresolved = {
    val b = proto.RouteUnresolved.newBuilder()
    b.setRoutingKey(toProto(obj.routingKey))
    b.build()
  }

  def streamFromProto(msg: proto.StreamEvent): Either[String, stream.StreamEvent] = {
    import proto.StreamEvent.EventTypeCase
    msg.getEventTypeCase match {
      case EventTypeCase.ROW_APPEND => rowAppendFromProto(msg.getRowAppend)
      case EventTypeCase.ROUTE_UNRESOLVED => unresolvedFromProto(msg.getRouteUnresolved)
      case _ => Left("Unrecognizable StreamEvent type")
    }
  }
  def streamToProto(obj: stream.StreamEvent): proto.StreamEvent = {
    val b = proto.StreamEvent.newBuilder()
    obj match {
      case v: stream.RowAppendEvent => b.setRowAppend(rowAppendToProto(v))
      case v: stream.RouteUnresolved => b.setRouteUnresolved(unresolvedToProto(v))
      case _ => throw new IllegalArgumentException("Unrecognized StreamEvent type")
    }
    b.build()
  }
}

object ProtocolConversions {
  import ValueTypeConversions._
  import StreamConversions._
  import io.greenbus.edge.util.EitherUtil._

  def servReqFromProto(msg: proto.ServiceRequest): Either[String, stream.ServiceRequest] = {
    if (msg.hasRow && msg.hasValue && msg.hasCorrelation) {
      for {
        row <- rowIdFromProto(msg.getRow)
        value <- fromProto(msg.getValue)
        correlation <- fromProto(msg.getCorrelation)
      } yield {
        stream.ServiceRequest(row, value, correlation)
      }
    } else {
      Left("ServiceRequest missing field(s)")
    }
  }
  def servReqToProto(obj: stream.ServiceRequest): proto.ServiceRequest = {
    val b = proto.ServiceRequest.newBuilder()
    b.setRow(rowIdToProto(obj.row))
    b.setValue(toProto(obj.value))
    b.setCorrelation(toProto(obj.correlation))
    b.build()
  }

  def servRespFromProto(msg: proto.ServiceResponse): Either[String, stream.ServiceResponse] = {
    if (msg.hasRow && msg.hasValue && msg.hasCorrelation) {
      for {
        row <- rowIdFromProto(msg.getRow)
        value <- fromProto(msg.getValue)
        correlation <- fromProto(msg.getCorrelation)
      } yield {
        stream.ServiceResponse(row, value, correlation)
      }
    } else {
      Left("ServiceResponse missing field(s)")
    }
  }
  def servRespToProto(obj: stream.ServiceResponse): proto.ServiceResponse = {
    val b = proto.ServiceResponse.newBuilder()
    b.setRow(rowIdToProto(obj.row))
    b.setValue(toProto(obj.value))
    b.setCorrelation(toProto(obj.correlation))
    b.build()
  }

  def servReqBatchFromProto(msg: proto.ServiceRequestBatch): Either[String, stream.ServiceRequestBatch] = {
    rightSequence(msg.getRequestsList.asScala.map(servReqFromProto))
      .map(seq => stream.ServiceRequestBatch(seq))
  }
  def servReqBatchToProto(obj: stream.ServiceRequestBatch): proto.ServiceRequestBatch = {
    val b = proto.ServiceRequestBatch.newBuilder()
    obj.requests.map(servReqToProto).foreach(b.addRequests)
    b.build()
  }

  def servRespBatchFromProto(msg: proto.ServiceResponseBatch): Either[String, stream.ServiceResponseBatch] = {
    rightSequence(msg.getResponsesList.asScala.map(servRespFromProto))
      .map(seq => stream.ServiceResponseBatch(seq))
  }
  def servRespBatchToProto(obj: stream.ServiceResponseBatch): proto.ServiceResponseBatch = {
    val b = proto.ServiceResponseBatch.newBuilder()
    obj.responses.map(servRespToProto).foreach(b.addResponses)
    b.build()
  }

  def subSetFromProto(msg: proto.SubscriptionSetUpdate): Either[String, stream.SubscriptionSetUpdate] = {
    rightSequence(msg.getRowsList.asScala.map(rowIdFromProto))
      .map(seq => stream.SubscriptionSetUpdate(seq.toSet))
  }
  def subSetToProto(obj: stream.SubscriptionSetUpdate): proto.SubscriptionSetUpdate = {
    val b = proto.SubscriptionSetUpdate.newBuilder()
    obj.rows.map(rowIdToProto).foreach(b.addRows)
    b.build()
  }

  def eventBatchFromProto(msg: proto.EventBatch): Either[String, stream.EventBatch] = {
    rightSequence(msg.getEventsList.asScala.map(streamFromProto))
      .map(seq => stream.EventBatch(seq))
  }
  def eventBatchToProto(obj: stream.EventBatch): proto.EventBatch = {
    val b = proto.EventBatch.newBuilder()
    obj.events.map(streamToProto).foreach(b.addEvents)
    b.build()
  }

  def gatewayEventsFromProto(msg: proto.GatewayClientEvents): Either[String, stream.GatewayClientEvents] = {
    val routesOptEither: Either[String, Option[Set[stream.TypeValue]]] = if (msg.hasRoutesUpdate) {
      rightSequence(msg.getRoutesUpdate.getValuesList.asScala.map(fromProto)).map(r => Some(r.toSet))
    } else {
      Right(None)
    }

    for {
      routesOpt <- routesOptEither
      events <- rightSequence(msg.getEventsList.asScala.map(streamFromProto))
    } yield {
      stream.GatewayClientEvents(routesOpt, events)
    }
  }
  def gatewayEventsToProto(obj: stream.GatewayClientEvents): proto.GatewayClientEvents = {
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
  import io.greenbus.edge.util.EitherUtil._

  def fromProtoSimple(msg: proto.UUID): java.util.UUID = {
    new java.util.UUID(msg.getHigh, msg.getLow)
  }
  def toProtoSimple(uuid: java.util.UUID): proto.UUID = {
    proto.UUID.newBuilder().setLow(uuid.getLeastSignificantBits).setHigh(uuid.getMostSignificantBits).build()
  }

  def tupleFromProto(msg: proto.TupleValue): Either[String, stream.TupleVal] = {
    rightSequence(msg.getElementList.asScala.map(fromProto).toVector).map(s => stream.TupleVal(s.toIndexedSeq))
  }
  def tupleToProto(obj: stream.TupleVal): proto.TupleValue = {
    val b = proto.TupleValue.newBuilder()
    obj.elements.foreach(v => b.addElement(toProto(v)))
    b.build()
  }

  def sequencedToProto(obj: stream.SequencedTypeValue): proto.SequencedTypeValue = {
    val b = proto.SequencedTypeValue.newBuilder()
    obj match {
      case v: stream.Int64Val => b.setSint64Value(v.v)
      case _ => throw new IllegalArgumentException(s"Unrecognized SequencedTypeValue: $obj")
    }
    b.build()
  }
  def sequencedFromProto(msg: proto.SequencedTypeValue): Either[String, stream.SequencedTypeValue] = {
    import proto.SequencedTypeValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.SINT64_VALUE => Right(stream.Int64Val(msg.getSint64Value))
      //case ValueTypesCase.TUPLE_VALUE => tupleFromProto(msg.getTupleValue)
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }

  def toProto(obj: stream.TypeValue): proto.TypeValue = {
    val b = proto.TypeValue.newBuilder()
    obj match {
      case v: stream.Int64Val => b.setSint64Value(v.v)
      case v: stream.DoubleVal => b.setDoubleValue(v.v)
      case v: stream.BoolVal => b.setBoolValue(v.v)
      case v: stream.SymbolVal => b.setSymbolValue(v.v)
      case v: stream.TextVal => b.setTextValue(v.v)
      case v: stream.UuidVal => b.setUuidValue(toProtoSimple(v.v))
      case v: stream.BytesVal => b.setBytesValue(ByteString.copyFrom(v.v))
      case v: stream.TupleVal => b.setTupleValue(tupleToProto(v))
      case _ => throw new IllegalArgumentException(s"Unrecognized TypeValue: $obj")
    }
    b.build()
  }
  def fromProto(msg: proto.TypeValue): Either[String, stream.TypeValue] = {
    import proto.TypeValue.ValueTypesCase
    msg.getValueTypesCase match {
      case ValueTypesCase.SINT64_VALUE => Right(stream.Int64Val(msg.getSint64Value))
      case ValueTypesCase.DOUBLE_VALUE => Right(stream.DoubleVal(msg.getDoubleValue))
      case ValueTypesCase.BOOL_VALUE => Right(stream.BoolVal(msg.getBoolValue))
      case ValueTypesCase.SYMBOL_VALUE => Right(stream.SymbolVal(msg.getSymbolValue))
      case ValueTypesCase.TEXT_VALUE => Right(stream.TextVal(msg.getTextValue))
      case ValueTypesCase.BYTES_VALUE => Right(stream.BytesVal(msg.getBytesValue.toByteArray))
      case ValueTypesCase.UUID_VALUE => Right(stream.UuidVal(fromProtoSimple(msg.getUuidValue)))
      case ValueTypesCase.TUPLE_VALUE => tupleFromProto(msg.getTupleValue)
      case ValueTypesCase.VALUETYPES_NOT_SET => Left("Unrecognizable value type")
      case _ => Left("Unrecognizable value type")
    }
  }
}
