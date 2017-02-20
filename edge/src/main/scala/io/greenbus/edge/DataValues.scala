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

import com.typesafe.scalalogging.LazyLogging

trait DataValueNotification
trait DataValueState extends DataValueNotification {
  def asFirstUpdate: DataValueUpdate
}
trait DataValueUpdate extends DataValueNotification
trait DataStateDesc
case class DataStateRequest(sessionId: SessionId, desc: DataStateDesc)

trait DataStreamDb {
  def processStateUpdate(state: DataValueState): Option[DataValueUpdate]
  def processValueUpdate(state: DataValueUpdate): Option[DataValueUpdate]
  def currentSnapshot(requestOpt: Option[DataStateRequest] = None): DataValueState
}

trait DataStreamSource {
  def streamForState(state: DataValueState): DataStreamDb
}

class DefaultStreamSource extends DataStreamSource {
  def streamForState(state: DataValueState): DataStreamDb = {
    state match {
      case st: SequencedValue => new SequencedValueDb(st)
      case st: TimeSeriesState => new TimeSeriesDb(st)
      case st: TopicEventBatch => new TopicDb
      case st: ActiveSetSnapshot => new ActiveSetDb(st)
    }
  }
}

case class LatestKeyValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor
case class TimeSeriesValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor
case class EventTopicValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor
case class ActiveSetValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor
case class UnrecognizedValueDescriptor(indexes: Map[Path, IndexableValue], metadata: Map[Path, Value]) extends DataKeyDescriptor

case class ActiveSetEntry(id: Long, value: Option[Value])
case class ActiveSetSnapshot(set: Seq[ActiveSetEntry], sequence: Long) extends DataValueState {
  def asFirstUpdate: DataValueUpdate = {
    ActiveSetUpdate(set, Seq(), sequence)
  }
}
case class ActiveSetUpdate(added: Seq[ActiveSetEntry], removed: Seq[Long], sequence: Long) extends DataValueUpdate

class ActiveSetDb(start: ActiveSetSnapshot) extends DataStreamDb with LazyLogging {
  private var seq: Long = start.sequence
  private var entries: Map[Long, Option[Value]] = {
    start.set.map(e => (e.id, e.value)).toMap
  }

  private def snapshotDiff(set: Seq[ActiveSetEntry], nextSequence: Long): ActiveSetUpdate = {
    val currentKeys = entries.keySet
    val snapKeys = set.map(_.id).toSet

    val removeSet = currentKeys.diff(snapKeys)
    val addEntries = set.filter(e => !currentKeys.contains(e.id))

    entries = entries.filterKeys(k => !removeSet.contains(k)) ++ addEntries.map(e => (e.id, e.value)).toMap

    ActiveSetUpdate(addEntries, removeSet.toVector, nextSequence)
  }

  private def computeUpdate(update: ActiveSetUpdate): Unit = {
    val removeSet = update.removed.toSet
    entries = entries.filterKeys(k => !removeSet.contains(k)) ++ update.added.map(e => (e.id, e.value)).toMap
  }

  def processStateUpdate(state: DataValueState): Option[DataValueUpdate] = {
    state match {
      case snap: ActiveSetSnapshot => {
        if (snap.sequence > seq) {
          seq = snap.sequence
          Some(snapshotDiff(snap.set, snap.sequence))
        } else {
          None
        }
      }
      case _ =>
        logger.warn("Erroneous state type in ActiveSetDb: " + state)
        None
    }
  }

  def processValueUpdate(update: DataValueUpdate): Option[DataValueUpdate] = {
    update match {
      case up: ActiveSetUpdate => {
        if (up.sequence > seq) {
          seq = up.sequence
          computeUpdate(up)
          Some(up)
        } else {
          None
        }
      }
      case _ =>
        logger.warn("Erroneous state type in ActiveSetDb: " + update)
        None
    }
  }

  def currentSnapshot(requestOpt: Option[DataStateRequest]): DataValueState = {
    val set = entries.map { case (id, vOpt) => ActiveSetEntry(id, vOpt) }.toVector
    ActiveSetSnapshot(set, seq)
  }
}

// TODO: sequenced
case class TopicEvent(topic: Path, data: Option[Value])

case class TopicEventBatch(events: Seq[TopicEvent]) extends DataValueUpdate with DataValueState {
  def asFirstUpdate: DataValueUpdate = this
}

class TopicDb extends DataStreamDb with LazyLogging {

  // TODO: currently pass-through

  def processStateUpdate(state: DataValueState): Option[DataValueUpdate] = {
    state match {
      case b: TopicEventBatch => Some(b)
      case _ =>
        logger.warn("Erroneous state type in TopicDb: " + state)
        None
    }
  }

  def processValueUpdate(update: DataValueUpdate): Option[DataValueUpdate] = {
    update match {
      case b: TopicEventBatch => Some(b)
      case _ =>
        logger.warn("Erroneous update type in TopicDb: " + update)
        None
    }
  }

  def currentSnapshot(requestOpt: Option[DataStateRequest]): DataValueState = {
    TopicEventBatch(Seq())
  }
}

case class SequencedValue(sequence: Long, value: Value) extends DataValueState with DataValueUpdate {
  def asFirstUpdate: DataValueUpdate = this
}
class SequencedValueDb(orig: SequencedValue) extends DataStreamDb {
  private var current: SequencedValue = orig

  private def update(not: DataValueNotification): Option[DataValueUpdate] = {
    not match {
      case dv: SequencedValue =>
        if (dv.sequence > current.sequence) {
          current = dv
          Some(dv)
        } else {
          None
        }
      case _ => None
    }
  }

  def processStateUpdate(state: DataValueState): Option[DataValueUpdate] = {
    update(state)
  }

  def processValueUpdate(state: DataValueUpdate): Option[DataValueUpdate] = {
    update(state)
  }

  def currentSnapshot(requestOpt: Option[DataStateRequest]): DataValueState = {
    current
  }
}

case class TimeSeriesStateDesc(sequence: Option[Long], time: Option[Long]) extends DataStateDesc

case class TimeSeriesSample(time: Long, value: SampleValue)

case class TimeSeriesSequenced(sequence: Long, sample: TimeSeriesSample)

case class TimeSeriesUpdate(values: Seq[TimeSeriesSequenced]) extends DataValueUpdate

case class TimeSeriesState(values: Seq[TimeSeriesSequenced]) extends DataValueState {
  def asFirstUpdate: DataValueUpdate = TimeSeriesUpdate(values)
}

object TimeSeriesDb {
  case class Record(sample: TimeSeriesSample, sequence: Long, sessionId: SessionId)
}
class TimeSeriesDb(original: TimeSeriesState) extends DataStreamDb {

  private var history: Vector[TimeSeriesSequenced] = original.values.toVector

  def processStateUpdate(dataState: DataValueState): Option[DataValueUpdate] = {
    dataState match {
      case state: TimeSeriesState => {
        if (history.nonEmpty) {
          val latest = history.last
          val added = state.values.filter(_.sequence > latest.sequence)
          history ++= added
          Some(TimeSeriesUpdate(added))
        } else {
          val added = state.values
          history ++= added
          Some(TimeSeriesUpdate(added))
        }
      }
      case _ => None
    }
  }

  def processValueUpdate(dataUpdate: DataValueUpdate): Option[DataValueUpdate] = {
    dataUpdate match {
      case update: TimeSeriesUpdate => {
        if (history.nonEmpty) {
          val latest = history.last
          val added = update.values.filter(_.sequence > latest.sequence)
          history ++= added
          Some(TimeSeriesUpdate(added))
        } else {
          history ++= update.values
          Some(TimeSeriesUpdate(update.values))
        }
      }
      case _ => None
    }
  }

  def currentSnapshot(requestOpt: Option[DataStateRequest]): DataValueState = {
    if (history.nonEmpty) {
      TimeSeriesState(Seq(history.last))
    } else {
      TimeSeriesState(Seq())
    }
  }
}

