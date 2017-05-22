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
package io.greenbus.edge.stream.engine2

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.stream.filter.{ FilteredStreamQueue, StreamFilter, StreamFilterImpl, StreamQueue }
import io.greenbus.edge.stream.{ AppendEvent, PeerSessionId, ResyncSession, StreamDelta }

import scala.collection.mutable

trait RowSynthesizer[Source] {
  def append(source: Source, event: AppendEvent): Seq[AppendEvent]
  def sourceRemoved(source: Source): Seq[AppendEvent]
}

/*
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
 */

class RowSynthImpl[Source] extends RowSynthesizer[Source] with LazyLogging {

  private var activeSessionOpt = Option.empty[(PeerSessionId, StreamFilter)]
  private val standbySessions = mutable.Map.empty[PeerSessionId, StreamQueue]
  private val sourceToSession = mutable.Map.empty[Source, PeerSessionId]

  def append(source: Source, event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case resync: ResyncSession => {
        val prevSessOpt = sourceToSession.get(source)
        if (!prevSessOpt.contains(resync.sessionId)) {
          sourceToSession.put(source, resync.sessionId)
        }

        activeSessionOpt match {
          case None => {
            val filter = new StreamFilterImpl
            filter.handle(resync)
            activeSessionOpt = Some((resync.sessionId, filter))
            Seq(resync)
          }
          case Some((activeSession, activeFilter)) => {
            if (resync.sessionId == activeSession) {
              activeFilter.handle(event).map(r => Seq(r)).getOrElse(Seq())
            } else {

              standbySessions.get(resync.sessionId) match {
                case None => {
                  val queue = new FilteredStreamQueue
                  queue.handle(event)
                  standbySessions.put(resync.sessionId, queue)
                }
                case Some(queue) => queue.handle(event)
              }

              activeChangeCheck()
            }
          }
        }
      }
      case delta: StreamDelta =>
        ???
    }
  }

  private def activeChangeCheck(): Seq[AppendEvent] = {
    ???
  }

  def sourceRemoved(source: Source): Seq[AppendEvent] = {
    ???
  }
}

/*
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
 */ 