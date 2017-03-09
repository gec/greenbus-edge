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
package io.greenbus.edge.colset

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.collection.{ BiMultiMap, MapToUniqueValues }
import scala.collection.mutable

class SynthesizerTable extends LazyLogging {
  private var routed = Map.empty[TypeValue, Map[TableRow, RowSynthesizer]]
  private var sourceToRow = BiMultiMap.empty[PeerSourceLink, RowId]

  def handleBatch(sourceLink: PeerSourceLink, events: Seq[StreamEvent]): Seq[StreamEvent] = {
    events.flatMap {
      case ev: RowAppendEvent => {
        val tableRow = ev.rowId.tableRow
        val routingKey = ev.routingKey
        routed.get(routingKey) match {
          case None => {
            addRouted(sourceLink, ev, routingKey, Map())
          }
          case Some(routingKeyRows) =>
            routingKeyRows.get(tableRow) match {
              case None => {
                addRouted(sourceLink, ev, routingKey, routingKeyRows)
              }
              case Some(db) =>
                db.append(sourceLink, ev.appendEvent).map(RowAppendEvent(ev.rowId, _))
            }
        }
      }
      case in: RouteUnresolved => {
        routed.get(in.routingKey) match {
          case None => Seq()
          case Some(rows) => {
            // TODO: stable sort?
            val results = rows.toVector.flatMap {
              case (row, db) =>
                val rowId = RowId(in.routingKey, row.table, row.rowKey)
                db.sourceRemoved(sourceLink)
                  .map(append => RowAppendEvent(rowId, append))
            }

            val rowIds = rows.keys.map(tr => RowId(in.routingKey, tr.table, tr.rowKey)).toSet
            sourceToRow = sourceToRow.removeMappings(sourceLink, rowIds)
            results
          }
        }
      }
    }
  }

  def sourceRemoved(sourceLink: PeerSourceLink): Seq[StreamEvent] = {
    val results = sourceToRow.keyToVal.get(sourceLink) match {
      case None => Seq()
      case Some(rowSet) =>
        // TODO: stable sort?
        rowSet.toVector.flatMap { rowId =>
          lookup(rowId)
            .map(db => db.sourceRemoved(sourceLink).map(apEv => RowAppendEvent(rowId, apEv)))
            .getOrElse(Seq())
        }
    }
    sourceToRow = sourceToRow.removeKey(sourceLink)
    results
  }

  private def lookup(rowId: RowId): Option[RowSynthesizer] = {
    routed.get(rowId.routingKey).flatMap { map =>
      map.get(rowId.tableRow)
    }
  }

  private def addRouted(sourceLink: PeerSourceLink, ev: RowAppendEvent, routingKey: TypeValue, existingRows: Map[TableRow, RowSynthesizer]): Seq[StreamEvent] = {
    ev.appendEvent match {
      case resync: ResyncSession =>
        RowSynthesizer.build(ev.rowId, sourceLink, resync.sessionId, resync.snapshot) match {
          case None =>
            logger.warn(s"Initial row event could not construct log: $ev")
            Seq()
          case Some((db, events)) =>
            routed += (routingKey -> (existingRows + (ev.rowId.tableRow -> db)))
            sourceToRow += (sourceLink -> ev.rowId)
            events.map(v => RowAppendEvent(ev.rowId, v))
        }
      case _ =>
        logger.warn(s"Initial row event was not resync session: $ev")
        Seq()
    }
  }
}

object RowSynthesizer {
  def build(rowId: RowId, initSource: PeerSourceLink, initSess: PeerSessionId, snapshot: SetSnapshot): Option[(RowSynthesizer, Seq[AppendEvent])] = {
    val logOpt = SessionRowLog.build(rowId, initSess, snapshot)
    logOpt.map { log =>
      val (events, db) = log.activate()
      val synthesizer = new RowSynthesizerImpl(rowId, initSource, initSess, db)
      (synthesizer, events)
    }
  }
}
trait RowSynthesizer {
  def append(source: PeerSourceLink, event: AppendEvent): Seq[AppendEvent]
  def sourceRemoved(source: PeerSourceLink): Seq[AppendEvent]
}

/*
three modes a session can be in:
- active
- standby (not yet active)
- TODO backfill (succeeded, allowing backfill events until a timeout happens or all sources are removed)

"standby" is really "unresolved"? in other words it's not got a clear ordering from the active session

 */
// TODO: emit desync when no sources left
class RowSynthesizerImpl(rowId: RowId, initSource: PeerSourceLink, initSess: PeerSessionId, initMgr: SessionSynthesizingFilter) extends RowSynthesizer with LazyLogging {

  private var activeSession: PeerSessionId = initSess
  private var activeDb: SessionSynthesizingFilter = initMgr
  private var standbySessions = Map.empty[PeerSessionId, SessionRowLog]
  private var sessionToSources: MapToUniqueValues[PeerSessionId, PeerSourceLink] = MapToUniqueValues((initSess, initSource))
  private def sourceToSession: Map[PeerSourceLink, PeerSessionId] = sessionToSources.valToKey

  def append(source: PeerSourceLink, event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case delta: StreamDelta => {
        sourceToSession.get(source) match {
          case None =>
            logger.warn(s"$rowId got delta for inactive source link $initSource"); Seq()
          case Some(sess) => {
            if (sess == activeSession) {
              activeDb.handleDelta(delta.update).map(StreamDelta)
                .map(v => Seq(v)).getOrElse(Seq())
            } else {
              standbySessions.get(sess) match {
                case None => logger.error(s"$rowId got delta for inactive source link $initSource")
                case Some(log) => log.handleDelta(delta.update)
              }
              Seq()
            }
          }
        }
      }
      case resync: ResyncSnapshot => {
        sourceToSession.get(source) match {
          case None =>
            logger.warn(s"$rowId got snapshot for inactive source link $initSource"); Seq()
          case Some(sess) => {
            if (sess == activeSession) {
              activeDb.handleResync(resync.snapshot)
                .map(v => Seq(v)).getOrElse(Seq())
            } else {
              standbySessions.get(sess) match {
                case None => logger.error(s"$rowId got delta for inactive source link $initSource")
                case Some(log) => log.handleResync(resync.snapshot)
              }
              Seq()
            }
          }
        }
      }
      case sessionResync: ResyncSession => {

        // TODO: session succession logic by actually reading peer session id
        val prevSessOpt = sourceToSession.get(source)
        sessionToSources = sessionToSources + (sessionResync.sessionId -> source)

        if (sessionResync.sessionId == activeSession) {

          // gc sessions with no sources
          prevSessOpt.foreach { prevSess =>
            if (!sessionToSources.keyToVal.contains(prevSess)) {
              standbySessions -= prevSess
            }
          }

          activeDb.handleResync(sessionResync.snapshot)
            .map(v => Seq(v)).getOrElse(Seq())

        } else {

          standbySessions.get(sessionResync.sessionId) match {
            case None => {
              SessionRowLog.build(rowId, sessionResync.sessionId, sessionResync.snapshot) match {
                case None => logger.warn(s"Session row log for $rowId could not be created from session resync: $sessionResync")
                case Some(log) =>
                  standbySessions += (sessionResync.sessionId -> log)
              }
            }
            case Some(log) => log.handleResync(sessionResync.snapshot)
          }

          activeSessionChangeCheck()
        }
      }
    }
  }

  // TODO: need to mark succeeded sessions to prevent immediately reverting to a prev session propped up by a source who is just delayed
  private def activeSessionChangeCheck(): Seq[AppendEvent] = {
    if (!sessionToSources.keyToVal.contains(activeSession)) {

      if (standbySessions.keySet.size == 1) {
        val (nowActiveSession, log) = standbySessions.head
        standbySessions -= nowActiveSession
        activeSession = nowActiveSession
        val (events, db) = log.activate()
        activeDb = db
        events

      } else {
        Seq()
      }

    } else {
      Seq()
    }
  }

  def sourceRemoved(source: PeerSourceLink): Seq[AppendEvent] = {

    val prevSessOpt = sourceToSession.get(source)
    sessionToSources = sessionToSources.remove(source)

    prevSessOpt match {
      case None => Seq()
      case Some(prevSess) =>
        if (prevSess == activeSession) {
          activeSessionChangeCheck()
        } else {
          if (!sessionToSources.keyToVal.contains(prevSess)) {
            standbySessions -= prevSess
          }
          Seq()
        }
    }
  }

}

object SessionRowLog {
  def build(rowId: RowId, sessionId: PeerSessionId, init: SetSnapshot): Option[SessionRowLog] = {
    init match {
      case snap: ModifiedSetSnapshot => Some(new SessionModifyRowLog(rowId, sessionId, snap))
      case snap: ModifiedKeyedSetSnapshot => Some(new SessionKeyedModifyRowLog(rowId, sessionId, snap))
      case snap: AppendSetSequence => {
        if (snap.appends.nonEmpty) {
          Some(new SessionAppendRowLog(rowId, sessionId, snap.appends.init, snap.appends.last))
        } else {
          None
        }
      }
    }
  }
}
trait SessionRowLog {
  def activate(): (Seq[AppendEvent], SessionSynthesizingFilter)
  //def resyncEvents: Seq[AppendEvent]
  def handleDelta(delta: SetDelta): Unit
  def handleResync(snapshot: SetSnapshot): Unit
}

trait SessionSynthesizingFilter {
  def handleDelta(delta: SetDelta): Option[SetDelta]
  def handleResync(snapshot: SetSnapshot): Option[AppendEvent]
}

sealed trait SessionSeqElement {
  def sequence: SequencedTypeValue
}

class SessionAppendRowLog(rowId: RowId, sessionId: PeerSessionId, init: Seq[AppendSetValue], last: AppendSetValue) extends SessionRowLog with LazyLogging {

  private var sequence: SequencedTypeValue = last.sequence
  private val values: mutable.ArrayBuffer[AppendSetValue] = mutable.ArrayBuffer[AppendSetValue](init :+ last: _*)

  def activate(): (Seq[AppendEvent], SessionSynthesizingFilter) = {
    val events = Seq(ResyncSession(sessionId, AppendSetSequence(values.toVector)))
    (events, new SessionAppendSynthesizingFilter(rowId, sessionId, sequence))
  }

  private def handleInOrder(appends: Seq[AppendSetValue]): Unit = {
    appends.foreach { append =>
      if (sequence.precedes(append.sequence)) {
        values += append
        sequence = append.sequence
      }
    }
  }

  def handleDelta(delta: SetDelta): Unit = {
    delta match {
      case AppendSetSequence(appends) => {
        handleInOrder(appends)
      }
      case _ => logger.error(s"Incorrect delta type for $rowId: $delta")
    }
  }

  def handleResync(snapshot: SetSnapshot): Unit = {
    snapshot match {
      case AppendSetSequence(appends) => {
        appends.headOption.foreach { head =>
          if (head.sequence == sequence || head.sequence.isLessThan(sequence)) {
            handleInOrder(appends)
          } else {
            sequence = head.sequence
            values += head
            handleInOrder(appends.tail)
          }
        }
      }
      case _ => logger.error(s"Incorrect snapshot type for $rowId: $snapshot")
    }
  }
}

class SessionAppendSynthesizingFilter(rowId: RowId, sessionId: PeerSessionId, startSequence: SequencedTypeValue) extends SessionSynthesizingFilter with LazyLogging {

  private var sequence: SequencedTypeValue = startSequence

  private def handleInOrder(appends: Seq[AppendSetValue]): Seq[AppendSetValue] = {
    val b = Vector.newBuilder[AppendSetValue]
    appends.foreach { append =>
      if (sequence.precedes(append.sequence)) {
        sequence = append.sequence
        b += append
      }
    }
    b.result()
  }

  def handleDelta(delta: SetDelta): Option[SetDelta] = {
    delta match {
      case AppendSetSequence(appends) => {
        val results = handleInOrder(appends)
        if (results.nonEmpty) {
          Some(AppendSetSequence(results))
        } else {
          None
        }
      }
      case _ =>
        logger.error(s"Incorrect delta type for $rowId: $delta")
        None
    }
  }

  def handleResync(snapshot: SetSnapshot): Option[AppendEvent] = {
    snapshot match {
      case AppendSetSequence(appends) => {
        appends.headOption.flatMap { head =>
          if (sequence.precedes(head.sequence) || head.sequence == sequence || head.sequence.isLessThan(sequence)) {
            val deltaAppends = handleInOrder(appends)
            if (deltaAppends.nonEmpty) {
              Some(StreamDelta(AppendSetSequence(deltaAppends)))
            } else {
              None
            }
          } else {
            sequence = head.sequence
            val resyncAppends = Seq(head) ++ handleInOrder(appends.tail)
            if (resyncAppends.nonEmpty) {
              Some(ResyncSnapshot(snapshot))
            } else {
              None
            }
          }
        }
      }
      case _ =>
        logger.error(s"Incorrect snapshot type for $rowId: $snapshot")
        None
    }
  }
}

class SessionModifyRowLog(rowId: RowId, sessionId: PeerSessionId, init: ModifiedSetSnapshot) extends SessionRowLog with LazyLogging {

  private var sequence: SequencedTypeValue = init.sequence
  private var current: Set[TypeValue] = init.snapshot

  def activate(): (Seq[AppendEvent], SessionModifyRowSynthesizingFilter) = {
    val snap = ModifiedSetSnapshot(sequence, current)
    (resyncEvents,
      new SessionModifyRowSynthesizingFilter(rowId, sessionId, snap))
  }

  def resyncEvents: Seq[AppendEvent] = {
    val snap = ModifiedSetSnapshot(sequence, current)
    Seq(ResyncSession(sessionId, snap))
  }

  def handleDelta(delta: SetDelta): Unit = {
    delta match {
      case d: ModifiedSetDelta =>
        if (sequence.precedes(d.sequence)) {
          sequence = d.sequence
          current = (current -- d.removes) ++ d.adds
        } else {
          logger.debug(s"Set delta unsequenced for $rowId, session $sessionId, current: $sequence, delta: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set delta event for $rowId, session $sessionId: $delta")
        None
    }
  }

  def handleResync(snapshot: SetSnapshot): Unit = {
    snapshot match {
      case d: ModifiedSetSnapshot =>
        if (sequence.isLessThan(d.sequence)) {
          sequence = d.sequence
          current = d.snapshot
        } else {
          logger.debug(s"Set snapshot unsequenced for $rowId, session $sessionId, current: $sequence, snap: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set snapshot event for $rowId, session $sessionId: $snapshot")
        None
    }
  }
}

class SessionModifyRowSynthesizingFilter(rowId: RowId, sessionId: PeerSessionId, init: ModifiedSetSnapshot) extends SessionSynthesizingFilter with LazyLogging {

  private var sequence: SequencedTypeValue = init.sequence
  //private var current: Set[TypeValue] = init.snapshot

  def handleDelta(delta: SetDelta): Option[SetDelta] = {
    delta match {
      case d: ModifiedSetDelta => {
        if (sequence.precedes(d.sequence)) {
          sequence = d.sequence
          //current = (current -- d.removes) ++ d.adds
          Some(delta)
        } else {
          logger.debug(s"Set delta unsequenced for $rowId, session $sessionId, current: $sequence, delta: ${d.sequence}")
          None
        }
      }
      case _ =>
        logger.warn(s"Unrecognized set delta event for $rowId, session $sessionId: $delta")
        None
    }
  }

  // TODO: could compute set diff to a resync for retail instead of the full thing; is skipping seqs allowed for latest-value in the stream semantics?
  // is this what the queue is doing?
  def handleResync(snapshot: SetSnapshot): Option[AppendEvent] = {
    snapshot match {
      case d: ModifiedSetSnapshot =>
        if (sequence.isLessThan(d.sequence)) {
          sequence = d.sequence
          //current = d.snapshot
          Some(ResyncSnapshot(snapshot))
        } else {
          logger.debug(s"Set snapshot unsequenced for $rowId, session $sessionId, current: $sequence, snap: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set snapshot event for $rowId, session $sessionId: $snapshot")
        None
    }
  }
}

class SessionKeyedModifyRowLog(rowId: RowId, sessionId: PeerSessionId, init: ModifiedKeyedSetSnapshot) extends SessionRowLog with LazyLogging {

  private var sequence: SequencedTypeValue = init.sequence
  private var current: Map[TypeValue, TypeValue] = init.snapshot

  def activate(): (Seq[AppendEvent], SessionSynthesizingFilter) = {
    val snap = ModifiedKeyedSetSnapshot(sequence, current)
    (resyncEvents, new SessionKeyedModifyRowSynthesizingFilter(rowId, sessionId, snap))
  }

  def snapshot: Map[TypeValue, TypeValue] = current

  def resyncEvents: Seq[AppendEvent] = {
    val snap = ModifiedKeyedSetSnapshot(sequence, current)
    Seq(ResyncSession(sessionId, snap))
  }

  def handleDelta(delta: SetDelta): Unit = {
    delta match {
      case d: ModifiedKeyedSetDelta =>
        if (sequence.precedes(d.sequence)) {
          sequence = d.sequence
          current = (current -- d.removes) ++ d.adds ++ d.modifies
        } else {
          logger.debug(s"Set delta unsequenced for $rowId, session $sessionId, current: $sequence, delta: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set delta event for $rowId, session $sessionId: $delta")
        None
    }
  }

  def handleResync(snapshot: SetSnapshot): Unit = {
    snapshot match {
      case d: ModifiedKeyedSetSnapshot =>
        if (sequence.isLessThan(d.sequence)) {
          sequence = d.sequence
          current = d.snapshot
        } else {
          logger.debug(s"Set snapshot unsequenced for $rowId, session $sessionId, current: $sequence, snap: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set snapshot event for $rowId, session $sessionId: $snapshot")
        None
    }
  }
}

class SessionKeyedModifyRowSynthesizingFilter(rowId: RowId, sessionId: PeerSessionId, init: ModifiedKeyedSetSnapshot) extends SessionSynthesizingFilter with LazyLogging {

  private var sequence: SequencedTypeValue = init.sequence

  def handleDelta(delta: SetDelta): Option[SetDelta] = {
    delta match {
      case d: ModifiedKeyedSetDelta => {
        if (sequence.precedes(d.sequence)) {
          sequence = d.sequence
          Some(delta)
        } else {
          logger.debug(s"Set delta unsequenced for $rowId, session $sessionId, current: $sequence, delta: ${d.sequence}")
          None
        }
      }
      case _ =>
        logger.warn(s"Unrecognized set delta event for $rowId, session $sessionId: $delta")
        None
    }
  }

  def handleResync(snapshot: SetSnapshot): Option[AppendEvent] = {
    snapshot match {
      case d: ModifiedKeyedSetSnapshot =>
        if (sequence.isLessThan(d.sequence)) {
          sequence = d.sequence
          Some(ResyncSnapshot(snapshot))
        } else {
          logger.debug(s"Set snapshot unsequenced for $rowId, session $sessionId, current: $sequence, snap: ${d.sequence}")
          None
        }
      case _ =>
        logger.warn(s"Unrecognized set snapshot event for $rowId, session $sessionId: $snapshot")
        None
    }
  }
}

