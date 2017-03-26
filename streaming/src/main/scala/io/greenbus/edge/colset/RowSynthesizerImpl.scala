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

class SynthesizerTable[Source] extends LazyLogging {
  private var routed = Map.empty[TypeValue, Map[TableRow, RowSynthesizer[Source]]]
  private var sourceToRow = BiMultiMap.empty[Source, RowId]

  def handleBatch(sourceLink: Source, events: Seq[StreamEvent]): Seq[StreamEvent] = {

    logger.trace(StreamLogging.logEvents(events))

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

  def sourceRemoved(sourceLink: Source): Seq[StreamEvent] = {
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

  private def lookup(rowId: RowId): Option[RowSynthesizer[Source]] = {
    routed.get(rowId.routingKey).flatMap { map =>
      map.get(rowId.tableRow)
    }
  }

  private def addRouted(sourceLink: Source, ev: RowAppendEvent, routingKey: TypeValue, existingRows: Map[TableRow, RowSynthesizer[Source]]): Seq[StreamEvent] = {
    ev.appendEvent match {
      case resync: ResyncSession =>

        val synth = RowSynthesizer.build(ev.rowId, sourceLink, resync)
        routed += (routingKey -> (existingRows + (ev.rowId.tableRow -> synth)))
        sourceToRow += (sourceLink -> ev.rowId)
        Seq(ev)

      case _ =>
        logger.warn(s"Initial row event was not resync session: $ev")
        Seq()
    }
  }
}

object RowSynthesizer {

  def build[Source](rowId: RowId, initSource: Source, resyncSession: ResyncSession): RowSynthesizer[Source] = {
    val filter = new GenInitializedStreamFilter(rowId.toString, resyncSession)
    new RowSynthesizerImpl[Source](rowId, initSource, resyncSession.sessionId, filter)
  }
}
trait RowSynthesizer[Source] {
  def append(source: Source, event: AppendEvent): Seq[AppendEvent]
  def sourceRemoved(source: Source): Seq[AppendEvent]
}

/*
three modes a session can be in:
- active
- standby (not yet active)
- TODO backfill (succeeded, allowing backfill events until a timeout happens or all sources are removed)

"standby" is really "unresolved"? in other words it's not got a clear ordering from the active session

 */
// TODO: emit desync when no sources left
class RowSynthesizerImpl[Source](rowId: RowId, initSource: Source, initSess: PeerSessionId, initFilter: StreamFilter) extends RowSynthesizer[Source] with LazyLogging {

  private var activeSession: PeerSessionId = initSess
  private var activeFilter: StreamFilter = initFilter
  private var standbySessions = Map.empty[PeerSessionId, StandbyStreamQueue]
  private var sessionToSources: MapToUniqueValues[PeerSessionId, Source] = MapToUniqueValues((initSess, initSource))
  private def currentSessionForSource: Map[Source, PeerSessionId] = sessionToSources.valToKey

  def append(source: Source, event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case delta: StreamDelta => {
        currentSessionForSource.get(source) match {
          case None =>
            logger.warn(s"$rowId got delta for inactive source link $initSource"); Seq()
          case Some(sess) => {
            if (sess == activeSession) {

              activeFilter.handle(delta)
                .map(v => Seq(v)).getOrElse(Seq())

            } else {

              standbySessions.get(sess) match {
                case None => logger.error(s"$rowId got delta for inactive source link $initSource")
                case Some(log) => log.append(delta)
              }
              Seq()
            }
          }
        }
      }
      case resync: ResyncSnapshot => {
        currentSessionForSource.get(source) match {
          case None =>
            logger.warn(s"$rowId got snapshot for inactive source link $initSource"); Seq()
          case Some(sess) => {
            //snapshotForSession(sess, resync)
            if (sess == activeSession) {
              activeFilter.handle(resync)
                .map(v => Seq(v)).getOrElse(Seq())
            } else {
              standbySessions.get(sess) match {
                case None => logger.error(s"$rowId got snapshot for inactive source link $initSource")
                case Some(log) => log.append(resync)
              }
              Seq()
            }
          }
        }
      }
      case sessionResync: ResyncSession => {

        // TODO: session succession logic by actually reading peer session id
        val prevSessOpt = currentSessionForSource.get(source)

        if (!prevSessOpt.contains(sessionResync.sessionId)) {
          sessionToSources = sessionToSources.remove(source) + (sessionResync.sessionId -> source)
        }

        if (sessionResync.sessionId == activeSession) {

          // gc sessions with no sources
          prevSessOpt.foreach { prevSess =>
            if (!sessionToSources.keyToVal.contains(prevSess)) {
              standbySessions -= prevSess
            }
          }

          activeFilter.handle(sessionResync)
            .map(v => Seq(v))
            .getOrElse(Seq())

        } else {

          standbySessions.get(sessionResync.sessionId) match {
            case None => {
              val standbyQueue = new GenStandbyStreamQueue(rowId.toString, sessionResync)
              standbySessions += (sessionResync.sessionId -> standbyQueue)
            }
            case Some(log) => log.append(sessionResync)
          }

          activeSessionChangeCheck()
        }
      }
    }
  }

  private def snapshotForSession(session: PeerSessionId, resync: ResyncSnapshot): Seq[AppendEvent] = {
    if (session == activeSession) {
      activeFilter.handle(resync)
        .map(v => Seq(v)).getOrElse(Seq())
    } else {
      standbySessions.get(session) match {
        case None => logger.error(s"$rowId got snapshot for inactive source link $initSource")
        case Some(log) => log.append(resync)
      }
      Seq()
    }
  }

  // TODO: need to mark succeeded sessions to prevent immediately reverting to a prev session propped up by a source who is just delayed
  private def activeSessionChangeCheck(): Seq[AppendEvent] = {
    if (!sessionToSources.keyToVal.contains(activeSession)) {

      if (standbySessions.keySet.size == 1) {
        val (nowActiveSession, log) = standbySessions.head
        standbySessions -= nowActiveSession
        activeSession = nowActiveSession
        //val (events, db) = log.activate()
        val resync = log.dequeue()
        val events = Seq(resync)
        val filter = new GenInitializedStreamFilter(rowId.toString, resync)
        activeFilter = filter
        events

      } else {
        Seq()
      }

    } else {
      Seq()
    }
  }

  def sourceRemoved(source: Source): Seq[AppendEvent] = {

    val prevSessOpt = currentSessionForSource.get(source)
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

