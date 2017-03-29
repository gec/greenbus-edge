package io.greenbus.edge.api

import io.greenbus.edge.flow.Sink

trait EndpointDescSink extends Sink[EndpointDescriptor]

case class EndpointDescSub(endpointId: EndpointId)

class EndpointSubscription(endpointId: EndpointId, descOpt: Option[EndpointDescSub], dataKeys: Set[Path], outputKeys: Path)

case class DataKeyUpdate(descriptor: Option[DataKeyDescriptor], value: DataKeyValueUpdate)
case class OutputKeyUpdate(descriptor: Option[OutputKeyDescriptor], value: OutputKeyStatus)

sealed trait DataKeyValueUpdate
sealed trait SequenceDataKeyValueUpdate extends DataKeyValueUpdate
case class KeyValueUpdate(value: Value) extends SequenceDataKeyValueUpdate
case class SeriesUpdate(value: SampleValue, time: Long) extends SequenceDataKeyValueUpdate
case class TopicEventUpdate(topic: Path, value: Value, time: Long) extends SequenceDataKeyValueUpdate
case class ActiveSetUpdate(value: Map[IndexableValue, Value], removes: Set[IndexableValue], added: Set[(IndexableValue, Value)], modified: Set[(IndexableValue, Value)]) extends DataKeyValueUpdate

case class EndpointSetUpdate(set: Set[EndpointId], removes: Set[EndpointId], adds: Set[EndpointId])
case class KeySetUpdate(set: Set[EndpointPath], removes: Set[EndpointPath], adds: Set[EndpointPath])

sealed trait IdentifiedEdgeUpdate
case class IdEndpointUpdate(id: EndpointId, data: EdgeDataStatus[EndpointDescriptor]) extends IdentifiedEdgeUpdate
case class IdDataKeyUpdate(id: EndpointPath, data: EdgeDataStatus[DataKeyUpdate]) extends IdentifiedEdgeUpdate
case class IdOutputKeyUpdate(id: EndpointPath, data: EdgeDataStatus[OutputKeyUpdate]) extends IdentifiedEdgeUpdate

case class IdEndpointPrefixUpdate(prefix: Path, data: EdgeDataStatus[EndpointSetUpdate]) extends IdentifiedEdgeUpdate
case class IdEndpointIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[EndpointSetUpdate]) extends IdentifiedEdgeUpdate
case class IdDataKeyIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[KeySetUpdate]) extends IdentifiedEdgeUpdate
case class IdOutputKeyIndexUpdate(specifier: IndexSpecifier, data: EdgeDataStatus[KeySetUpdate]) extends IdentifiedEdgeUpdate

/*
Pending
Unresolved
ResolvedAbsent
ResolvedValue
 */
sealed trait EdgeDataStatus[+A]
case object Pending extends EdgeDataStatus[Nothing]
case object DataUnresolved extends EdgeDataStatus[Nothing]
case object ResolvedAbsent extends EdgeDataStatus[Nothing]
case class ResolvedValue[A](value: A) extends EdgeDataStatus[A]
case object Disconnected extends EdgeDataStatus[Nothing]