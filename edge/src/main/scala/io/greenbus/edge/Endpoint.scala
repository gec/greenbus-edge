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

object Path {
  def isPrefixOf(l: Path, r: Path): Boolean = {
    val lIter = l.parts.iterator
    val rIter = r.parts.iterator
    var okay = true
    var continue = true
    while (continue) {
      (lIter.hasNext, rIter.hasNext) match {
        case (true, true) =>
          if (lIter.next() != rIter.next()) {
            okay = false
            continue = false
          }
        case (false, true) => continue = false
        case (true, false) =>
          okay = false; continue = false
        case (false, false) => continue = false
      }
    }
    okay
  }

  def apply(part: String): Path = {
    Path(Seq(part))
  }
}
case class Path(parts: Seq[String])

// TODO: one object for multiple sessions, since responding with backlog requires going across sessions
object EndpointDb {

  /*
    - path-to-source update for endpoint manifest
    - optional info update
    - value updates
      - should the abstraction treat updates/current values as equiv?

    can distinguish between publishers and peer replication
    in case where source reaches out to redundant peers we may be replicating and sourcing

   */
  case class ProcessResult(
    sourceRejected: Boolean = false,
    setUpdate: Option[EndpointSetEntry] = None,
    added: Boolean = false,
    infoUpdateOpt: Option[(Long, EndpointDescriptor)] = None,
    valueUpdates: Seq[(Path, DataValueUpdate)] = Seq(),
    outputUpdates: Seq[(Path, OutputValueStatus)] = Seq())
}

class EndpointDb(id: EndpointId, dataDef: DataStreamSource) {
  import EndpointDb._
  //private var active = Option.empty[(SourceId, SessionId, EndpointSessionDb)]

  private var state = Option.empty[(Option[SourceId], SessionId, EndpointSessionDb)]
  private def dbOpt: Option[EndpointSessionDb] = state.map(_._3)

  def activeSession(): Option[SessionId] = {
    state.map(_._2)
  }

  def allowPublisher(source: SourceId, sessionId: SessionId): Boolean = {
    state match {
      case None => true
      case Some((None, _, _)) => true
      case _ => false
    }
  }

  def currentInfo(): Option[EndpointDescriptorRecord] = {
    dbOpt.map(_.currentInfo()).map {
      case (info, seq) => EndpointDescriptorRecord(seq, id, info)
    }
  }

  def currentData(keys: Seq[Path]): Option[Map[Path, DataValueNotification]] = {
    dbOpt.map(_.currentData(keys))
  }
  def currentOutputStatuses(keys: Seq[Path]): Option[Map[Path, OutputValueStatus]] = {
    state.map {
      case (_, sessionId, db) =>
        db.currentOutputStatuses(keys).mapValues { clientStatus =>
          OutputValueStatus(sessionId, clientStatus.sequence, clientStatus.valueOpt)
        }
    }
  }

  def processBatch(sourceId: SourceId, sessionId: SessionId, batch: EndpointPublishMessage): EndpointDb.ProcessResult = {
    state match {
      case None => batch.snapshotOpt.map(snap => processFirst(sourceId, sessionId, snap)).getOrElse(ProcessResult(sourceRejected = true))
      case Some((Some(activeSrc), activeSess, db)) => {
        if (sourceId == activeSrc && sessionId == activeSess) {
          processUpdate(db, activeSess, batch)
        } else {
          ProcessResult(sourceRejected = true)
        }
      }
      case Some((None, activeSess, db)) => {
        if (sessionId == activeSess) {
          state = Some((Some(sourceId), activeSess, db))
          processUpdate(db, activeSess, batch)
        } else {
          batch.snapshotOpt match {
            case None => ProcessResult(sourceRejected = true)
            case Some(snap) =>
              val (nextDb, result) = processSessionTransition(db, sessionId, snap)
              state = Some((Some(sourceId), sessionId, nextDb))
              result
          }
        }
      }
    }
  }

  def sourceClosed(sourceId: SourceId): EndpointDb.ProcessResult = {
    state match {
      case None => ProcessResult()
      case Some((Some(activeSrc), activeSess, db)) =>
        if (activeSrc == sourceId) {
          state = Some((None, activeSess, db))
        }
        ProcessResult()
      case Some(_) => ProcessResult()
    }
  }

  private def processFirst(sourceId: SourceId, sessionId: SessionId, snapshot: EndpointPublishSnapshot): EndpointDb.ProcessResult = {
    val sessionDb = new EndpointSessionDb(snapshot, dataDef)
    val result = sessionDb.processSnapshotUpdate(snapshot)
    val setUpdate = EndpointSetEntry(snapshot.endpoint.endpointId, snapshot.endpoint.descriptor.indexes)
    val outputsWithSession = result.outputUpdates.map { case (path, cl) => (path, OutputValueStatus(sessionId, cl.sequence, cl.valueOpt)) }
    state = Some((Some(sourceId), sessionId, sessionDb))
    ProcessResult(added = true, setUpdate = Some(setUpdate), infoUpdateOpt = result.infoUpdateOpt, valueUpdates = result.valueUpdates, outputUpdates = outputsWithSession)
  }

  private def processUpdate(db: EndpointSessionDb, sessionId: SessionId, batch: EndpointPublishMessage): EndpointDb.ProcessResult = {
    val result = db.processUpdates(batch.infoUpdateOpt, batch.dataUpdates, batch.outputStatusUpdates)
    val outputsWithSession = result.outputUpdates.map { case (path, cl) => (path, OutputValueStatus(sessionId, cl.sequence, cl.valueOpt)) }
    ProcessResult(setUpdate = result.setUpdate, infoUpdateOpt = result.infoUpdateOpt, valueUpdates = result.valueUpdates, outputUpdates = outputsWithSession)
  }

  private def processSessionTransition(oldDb: EndpointSessionDb, nextSessionId: SessionId, snapshot: EndpointPublishSnapshot): (EndpointSessionDb, EndpointDb.ProcessResult) = {
    ???
  }

}

object EndpointSessionDb {
  case class Result(
    setUpdate: Option[EndpointSetEntry],
    infoUpdateOpt: Option[(Long, EndpointDescriptor)],
    valueUpdates: Seq[(Path, DataValueUpdate)],
    outputUpdates: Seq[(Path, PublisherOutputValueStatus)])
}
class EndpointSessionDb(initial: EndpointPublishSnapshot, dataDef: DataStreamSource) {
  import EndpointSessionDb._

  private val endpointId = initial.endpoint.endpointId
  private var infoSequence: Long = initial.endpoint.sequence
  private var info: EndpointDescriptor = initial.endpoint.descriptor
  private var validDataKeys: Set[Path] = initial.endpoint.descriptor.dataKeySet.keySet
  private var dataMap: Map[Path, DataStreamDb] = initial.data.mapValues(dataDef.streamForState)
  private var validOutputKeys: Set[Path] = initial.endpoint.descriptor.outputKeySet.keySet
  private var outputStatusMap: Map[Path, PublisherOutputValueStatus] = initial.outputStatus

  def currentInfo(): (EndpointDescriptor, Long) = (info, infoSequence)

  def currentData(keys: Seq[Path]): Map[Path, DataValueNotification] = {
    keys.flatMap { key =>
      dataMap.get(key).map(_.currentSnapshot()).map { st =>
        key -> st
      }
    }.toMap
  }

  def currentOutputStatuses(keys: Seq[Path]): Map[Path, PublisherOutputValueStatus] = {
    keys.flatMap { key =>
      outputStatusMap.get(key).map(v => key -> v)
    }.toMap
  }

  /*def current(): EndpointPublishSnapshot = {
    EndpointPublishSnapshot(
      EndpointInfoRecord(infoSequence, endpointId, info),
      dataMap.mapValues(_.currentSnapshot()))
  }*/

  def calculateDiffToSnapshot(snapshot: EndpointPublishSnapshot) = {

  }

  private def recalcValidKeys(info: EndpointDescriptor): Unit = {
    validDataKeys = info.dataKeySet.keySet
    dataMap = dataMap.filterKeys(validDataKeys.contains)
  }

  private def processDataStateUpdate(path: Path, state: DataValueState): Option[DataValueUpdate] = {
    dataMap.get(path) match {
      case Some(db) => db.processStateUpdate(state)
      case None => {
        if (validDataKeys.contains(path)) {
          dataMap += (path -> dataDef.streamForState(state))
          Some(state.asFirstUpdate)
        } else {
          None
        }
      }
    }
  }

  private def processOutputStatusStateUpdate(path: Path, state: PublisherOutputValueStatus): Option[PublisherOutputValueStatus] = {
    outputStatusMap.get(path) match {
      case Some(old) => {
        if (state.sequence > old.sequence) {
          outputStatusMap += (path -> state)
          Some(state)
        } else {
          None
        }
      }
      case None => {
        if (validOutputKeys.contains(path)) {
          outputStatusMap += (path -> state)
          Some(state)
        } else {
          None
        }
      }
    }
  }

  private def processDataValueUpdate(path: Path, update: DataValueUpdate): Option[DataValueUpdate] = {
    dataMap.get(path).flatMap(_.processValueUpdate(update))
  }

  private def processInfoUpdate(record: EndpointDescriptorRecord): (Option[(Long, EndpointDescriptor)], Option[EndpointSetEntry]) = {
    if (record.sequence > infoSequence) {
      val setUpdate = if (info.indexes != record.descriptor.indexes) Some(EndpointSetEntry(record.endpointId, record.descriptor.indexes)) else None
      infoSequence = record.sequence
      info = record.descriptor
      recalcValidKeys(info)
      (Some((record.sequence, info)), setUpdate)
    } else {
      (None, None)
    }
  }

  def processSnapshotUpdate(snapshot: EndpointPublishSnapshot): Result = {
    val (infoUpdateOpt, setUpdate) = processInfoUpdate(snapshot.endpoint)

    val valueUpdates = snapshot.data.flatMap {
      case (path, state) => processDataStateUpdate(path, state).map(up => (path, up))
    }.toVector

    val outputUpdates = snapshot.outputStatus.flatMap {
      case (path, state) => processOutputStatusStateUpdate(path, state).map(up => (path, up))
    }.toVector

    Result(setUpdate, infoUpdateOpt, valueUpdates, outputUpdates)
  }

  def processUpdates(infoUpdateOpt: Option[EndpointDescriptorRecord], dataUpdates: Seq[(Path, DataValueUpdate)], outputUpdates: Seq[(Path, PublisherOutputValueStatus)]): Result = {
    val (infoResultOpt, setUpdate) = infoUpdateOpt.map(processInfoUpdate).getOrElse(None, None)

    val processedData = dataUpdates.flatMap {
      case (path, dvu) => processDataValueUpdate(path, dvu).map(up => (path, up))
    }

    val processedOutputs = outputUpdates.flatMap {
      case (path, state) => processOutputStatusStateUpdate(path, state).map(up => (path, up))
    }.toVector

    Result(setUpdate, infoResultOpt, processedData, processedOutputs)
  }

}

