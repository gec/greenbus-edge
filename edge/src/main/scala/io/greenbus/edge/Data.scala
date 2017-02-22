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
package io.greenbus.edge

import java.util.UUID

import io.greenbus.edge.SessionId.Unrelated

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

sealed trait EndpointId
case class NamedEndpointId(name: Path) extends EndpointId

object SessionId {
  sealed trait SessionRelation
  case object Unrelated extends SessionRelation
  case object Previous extends SessionRelation
  case object Next extends SessionRelation
  case object GreaterThan extends SessionRelation
}
sealed trait SessionId {
  def relatesTo(id: SessionId): SessionId.SessionRelation
}
case class PersistenceSessionId(persistenceId: UUID, sequence: Long) extends SessionId {
  import SessionId._

  def relatesTo(id: SessionId): SessionRelation = {
    id match {
      case PersistenceSessionId(rightId, rightSeq) => //rightId == persistenceId && rightSeq > sequence
        if (rightId == persistenceId) {
          if (sequence == rightSeq + 1) {
            Next
          } else if (sequence > rightSeq + 1) {
            GreaterThan
          } else if (sequence < rightSeq) {
            Previous
          } else {
            Unrelated
          }
        } else {
          Unrelated
        }
      case _ => Unrelated
    }
  }
}

case class EndpointPath(endpoint: EndpointId, key: Path)

case class EndpointSetEntry(endpointId: EndpointId, indexes: Map[Path, IndexableValue])

case class EndpointSetSnapshot(entries: Seq[EndpointSetEntry])

case class EndpointSetNotification(
  prefix: Path,
  snapshotOpt: Option[EndpointSetSnapshot],
  added: Seq[EndpointSetEntry],
  modified: Seq[EndpointSetEntry],
  removed: Seq[EndpointId])

case class EndpointDescriptorNotification(endpointId: EndpointId, descriptor: EndpointDescriptor, sequence: Long)
case class DataKeyDescriptorNotification(endpointPath: EndpointPath, descriptor: DataKeyDescriptor, sequence: Long)
case class OutputKeyDescriptorNotification(endpointPath: EndpointPath, descriptor: OutputKeyDescriptor, sequence: Long)

case class EndpointDataNotification(key: EndpointPath, value: DataValueNotification, descriptorNotification: Option[DataKeyDescriptorNotification])
case class EndpointOutputStatusNotification(key: EndpointPath, status: OutputValueStatus, descriptorNotification: Option[OutputKeyDescriptorNotification])

case class ClientSubscriptionNotification(
  setNotifications: Seq[EndpointSetNotification],
  indexNotification: ClientIndexNotification,
  descriptorNotifications: Seq[EndpointDescriptorNotification],
  dataNotifications: Seq[EndpointDataNotification],
  outputNotifications: Seq[EndpointOutputStatusNotification])

case class IndexSpecifier(key: Path, valueOpt: Option[IndexableValue])

case class ClientIndexSubscriptionParams(
  endpointIndexes: Seq[IndexSpecifier] = Seq(),
  dataKeyIndexes: Seq[IndexSpecifier] = Seq(),
  outputKeyIndexes: Seq[IndexSpecifier] = Seq())

case class EndpointIndexNotification(specifier: IndexSpecifier, snapshot: Option[Set[EndpointId]], added: Set[EndpointId], removed: Set[EndpointId])
case class DataKeyIndexNotification(specifier: IndexSpecifier, snapshot: Option[Set[EndpointPath]], added: Set[EndpointPath], removed: Set[EndpointPath])
case class OutputKeyIndexNotification(specifier: IndexSpecifier, snapshot: Option[Set[EndpointPath]], added: Set[EndpointPath], removed: Set[EndpointPath])

case class ClientIndexNotification(
  endpointNotifications: Seq[EndpointIndexNotification] = Seq(),
  dataKeyNotifications: Seq[DataKeyIndexNotification] = Seq(),
  outputKeyNotifications: Seq[OutputKeyIndexNotification] = Seq())

case class ClientSubscriptionParams(
  endpointSetPrefixes: Seq[Path] = Seq(),
  indexParams: ClientIndexSubscriptionParams = ClientIndexSubscriptionParams(),
  infoSubscriptions: Seq[EndpointId] = Seq(),
  dataSubscriptions: Seq[EndpointPath] = Seq(),
  outputSubscriptions: Seq[EndpointPath] = Seq())

case class ClientSubscriptionParamsMessage(params: ClientSubscriptionParams)
case class ClientSubscriptionNotificationMessage(notification: ClientSubscriptionNotification)

trait DataKeyDescriptor {
  def indexes: Map[Path, IndexableValue]
  def metadata: Map[Path, Value]
}

case class OutputKeyDescriptor(
  indexes: Map[Path, IndexableValue],
  metadata: Map[Path, Value])

case class EndpointDescriptor(
  indexes: Map[Path, IndexableValue],
  metadata: Map[Path, Value],
  dataKeySet: Map[Path, DataKeyDescriptor],
  outputKeySet: Map[Path, OutputKeyDescriptor])

case class ValueUpdate(key: Path, value: Value)

case class EndpointSnapshot(infoUpdate: EndpointDescriptor, data: Map[Path, DataValueState], outputStatuses: Map[Path, PublisherOutputValueStatus])
case class EndpointPublishBatch(
  descriptorUpdate: Option[EndpointDescriptor],
  data: Map[Path, DataValueUpdate],
  outputStatuses: Map[Path, PublisherOutputValueStatus])

case class EndpointDescriptorRecord(sequence: Long, endpointId: EndpointId, descriptor: EndpointDescriptor)
case class EndpointPublishSnapshot(endpoint: EndpointDescriptorRecord, data: Map[Path, DataValueState], outputStatus: Map[Path, PublisherOutputValueStatus])
case class EndpointPublishMessage(
  snapshotOpt: Option[EndpointPublishSnapshot],
  infoUpdateOpt: Option[EndpointDescriptorRecord],
  dataUpdates: Seq[(Path, DataValueUpdate)],
  outputStatusUpdates: Seq[(Path, PublisherOutputValueStatus)])

sealed trait OutputResult
case class OutputSuccess(valueOpt: Option[Value]) extends OutputResult
case class OutputFailure(reason: String) extends OutputResult

case class PublisherOutputRequestMessage(requests: Seq[PublisherOutputRequest])
case class PublisherOutputResponseMessage(results: Seq[(Long, OutputResult)])

case class ClientOutputRequestMessage(requests: Seq[ClientOutputRequest])
case class ClientOutputResponseMessage(results: Seq[(Long, OutputResult)])

