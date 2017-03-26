package io.greenbus.edge.colset


//sealed trait StreamParams

sealed trait SequenceTypeDiff

case class SetDiff(removes: Set[TypeValue], adds: Set[TypeValue]) extends SequenceTypeDiff
case class MapDiff(removes: Set[TypeValue], adds: Set[(TypeValue, TypeValue)], modifies: Set[(TypeValue, TypeValue)]) extends SequenceTypeDiff
case class AppendValue(value: TypeValue) extends SequenceTypeDiff

sealed trait SequenceSnapshot

case class SetSnapshot(snapshot: Set[TypeValue]) extends SequenceSnapshot
case class MapSnapshot(snapshot: Map[TypeValue, TypeValue]) extends SequenceSnapshot

case class AppendSetValue(sequence: SequencedTypeValue, value: AppendValue)
//case class AppendSnapshot(current: AppendSetValue, previous: Seq[AppendSetValue]) extends StreamSnapshot
case class AppendSnapshot(current: SequencedDiff, previous: Seq[SequencedDiff]) extends SequenceSnapshot

case class SequencedDiff(sequence: SequencedTypeValue, diff: SequenceTypeDiff)

sealed trait SequenceEvent
case class Delta(diffs: Seq[SequencedDiff]) extends SequenceEvent
case class Resync(sequence: SequencedTypeValue, snapshot: SequenceSnapshot) extends SequenceEvent

case class StreamParams()

object SequenceCtx {
  def empty: SequenceCtx = SequenceCtx(StreamParams(), TupleVal(Seq()))
}
case class SequenceCtx(params: StreamParams, userMetadata: TypeValue)

/*
CONTROL STREAM VS VALUE SEQUENCE

 */
sealed trait AppendEvent
case class StreamDelta(update: Delta) extends AppendEvent
case class ResyncSnapshot(resync: Resync) extends AppendEvent
//case class Reinitialize(init: SequenceInit) extends AppendEvent
case class ResyncSession(sessionId: PeerSessionId, context: SequenceCtx, resync: Resync) extends AppendEvent

// This is "peer event", append event above is stream event
sealed trait StreamEvent {
  def routingKey: TypeValue // TODO: change to route?
}
sealed trait RowEvent extends StreamEvent {
  def rowId: RowId
}
case class RowAppendEvent(rowId: RowId, appendEvent: AppendEvent) extends RowEvent {
  def routingKey: TypeValue = rowId.routingKey
}
//sealed trait RowAppendEvent extends RowEvent
/*case class DeltaEvent(row: RowId, delta: Delta) extends RowAppendEvent
case class ResyncEvent(row: RowId, resync: Resync) extends RowAppendEvent
case class ResequenceEvent(row: RowId, sessionId: PeerSessionId, resync: Resync) extends RowAppendEvent*/
case class RowResolvedAbsent(rowId: RowId) extends RowEvent {
  def routingKey: TypeValue = rowId.routingKey
}
case class RouteUnresolved(routingKey: TypeValue) extends StreamEvent

/*

trait SetDelta
trait SetSnapshot

case class ModifiedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[TypeValue]) extends SetDelta
case class ModifiedSetSnapshot(sequence: SequencedTypeValue, snapshot: Set[TypeValue]) extends SetSnapshot
case class ModifiedKeyedSetDelta(sequence: SequencedTypeValue, removes: Set[TypeValue], adds: Set[(TypeValue, TypeValue)], modifies: Set[(TypeValue, TypeValue)]) extends SetDelta
case class ModifiedKeyedSetSnapshot(sequence: SequencedTypeValue, snapshot: Map[TypeValue, TypeValue]) extends SetSnapshot
case class AppendSetValue(sequence: SequencedTypeValue, value: TypeValue)
case class AppendSetSequence(appends: Seq[AppendSetValue]) extends SetDelta with SetSnapshot

sealed trait AppendEvent
case class StreamDelta(update: SetDelta) extends AppendEvent
case class ResyncSnapshot(snapshot: SetSnapshot) extends AppendEvent
case class ResyncSession(sessionId: PeerSessionId, snapshot: SetSnapshot) extends AppendEvent


sealed trait StreamEvent {
  def routingKey: TypeValue
}

case class RowAppendEvent(rowId: RowId, appendEvent: AppendEvent) extends StreamEvent {
  def routingKey = rowId.routingKey
}

// TODO: rowunavailable event?
case class RouteUnresolved(routingKey: TypeValue) extends StreamEvent

 */