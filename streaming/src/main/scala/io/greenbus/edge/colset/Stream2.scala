package io.greenbus.edge.colset

object Redux {

  sealed trait StreamParams

  sealed trait StreamDiff

  case class SetDiff(removes: Set[TypeValue], adds: Set[TypeValue])
  case class MapDiff(removes: Set[TypeValue], adds: Set[(TypeValue, TypeValue)], modifies: Set[(TypeValue, TypeValue)])
  case class AppendValue(value: TypeValue)

  sealed trait StreamSnapshot

  case class SetSnapshot(snapshot: Set[TypeValue]) extends StreamSnapshot
  case class MapSnapshot(snapshot: Map[TypeValue, TypeValue]) extends StreamSnapshot

  case class AppendSetValue(sequence: SequencedTypeValue, value: TypeValue)
  case class AppendSnapshot(current: AppendSetValue, previous: Seq[AppendSetValue]) extends StreamSnapshot

  case class SequencedDiff(sequence: SequencedTypeValue, diff: StreamDiff)
  case class Delta(diffs: Seq[SequencedDiff])

  case class Resync(sequence: SequencedTypeValue, params: StreamParams, userMetadata: TypeValue, snapshot: StreamSnapshot)

  sealed trait StreamEvent
  sealed trait RowEvent extends StreamEvent {
    def row: RowId
  }
  sealed trait RowAppendEvent extends RowEvent
  case class DeltaEvent(row: RowId, delta: Delta) extends RowAppendEvent
  case class ResyncEvent(row: RowId, resync: Resync) extends RowAppendEvent
  case class ResequenceEvent(row: RowId, sessionId: PeerSessionId, resync: Resync) extends RowAppendEvent
  case class RowResolvedAbsent(row: RowId) extends RowEvent
  case class RouteUnresolved(route: TypeValue) extends StreamEvent
}