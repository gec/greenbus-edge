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
import io.greenbus.edge.stream._

import scala.collection.mutable

trait RowSynthesizer[Source] {
  def append(source: Source, event: AppendEvent): Seq[AppendEvent]
  def sourceRemoved(source: Source): Seq[AppendEvent]
}

/*
  A -> Set[B], where a B is only ever associated with one A, and rebindings "steal" it
 */
class UniqueValueMultiMap[A, B] {
  private val keyToValueMap = mutable.Map.empty[A, Set[B]]
  private val valueToKeyMap = mutable.Map.empty[B, A]

  def contains(key: A): Boolean = keyToValueMap.contains(key)
  def get(key: A): Option[Set[B]] = keyToValueMap.get(key)
  def valueGet(v: B): Option[A] = valueToKeyMap.get(v)

  def keySet(): Set[A] = keyToValueMap.keySet.toSet

  def put(key: A, v: B): Unit = {
    doValueRemove(v)
    keyToValueMap.get(key) match {
      case None => keyToValueMap.update(key, Set(v))
      case Some(prev) => keyToValueMap.update(key, prev + v)
    }
    valueToKeyMap.update(v, key)
  }

  def removeValue(v: B): Unit = {
    doValueRemove(v)
  }

  private def doValueRemove(v: B): Unit = {
    valueToKeyMap.get(v).foreach { key =>
      val valueSet = keyToValueMap.getOrElse(key, Set())
      val without = valueSet - v
      if (without.nonEmpty) {
        keyToValueMap.update(key, without)
      } else {
        keyToValueMap -= key
      }
    }
  }
}

object RowSynthImpl {
  sealed trait SynthState
  case object Inactive extends SynthState
  case object ActiveAbsent extends SynthState
  case class ActiveSessioned(session: PeerSessionId, filter: StreamFilter) extends SynthState

  sealed trait SourceState
  case class SourceSessioned(session: PeerSessionId) extends SourceState
  case object SourceStreamAbsent extends SourceState
}
class RowSynthImpl[Source] extends RowSynthesizer[Source] with LazyLogging {
  import RowSynthImpl._

  //private var activeSessionOpt = Option.empty[(PeerSessionId, StreamFilter)]
  private var synthState: SynthState = Inactive
  private val standbySessions = mutable.Map.empty[PeerSessionId, StreamQueue]
  private val sessionToSources = new UniqueValueMultiMap[PeerSessionId, Source]
  private val sourceStates = mutable.Map.empty[Source, SourceState]

  private def activeSessionOpt: Option[(PeerSessionId, StreamFilter)] = {
    synthState match {
      case ActiveSessioned(sess, filter) => Some(sess, filter)
      case _ => None
    }
  }

  def append(source: Source, event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case resync: ResyncSession => {

        if (!sourceStates.get(source).contains(SourceSessioned(resync.sessionId))) {
          sessionToSources.put(resync.sessionId, source)
          sourceStates.put(source, SourceSessioned(resync.sessionId))
        }

        synthState match {
          case Inactive | ActiveAbsent =>
            val filter = new StreamFilterImpl
            filter.handle(resync)
            synthState = ActiveSessioned(resync.sessionId, filter)
            Seq(resync)
          case ActiveSessioned(activeSession, activeFilter) =>
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
      case delta: StreamDelta => {
        sourceStates.get(source) match {
          case None =>
            logger.warn(s"got delta for inactive source link")
            Seq()
          case Some(ss) =>
            ss match {
              case SourceSessioned(sourceSess) =>
                activeSessionOpt match {
                  case None => {
                    standbySessions.get(sourceSess) match {
                      case None => logger.error(s"got delta for source with an inactive session link")
                      case Some(queue) => queue.handle(event)
                    }
                    Seq()
                  }
                  case Some((activeSess, activeFilter)) =>
                    if (sourceSess == activeSess) {
                      activeFilter.handle(event).map(v => Seq(v)).getOrElse(Seq())
                    } else {
                      standbySessions.get(sourceSess) match {
                        case None => logger.error(s"got delta for source with an inactive session link")
                        case Some(queue) => queue.handle(event)
                      }
                      Seq()
                    }
                }

              case SourceStreamAbsent =>
                logger.warn(s"got delta for absent source stream")
                Seq()
            }
        }
      }
      case StreamAbsent => {

        sourceStates.get(source) match {
          case None =>
            sourceStates.put(source, SourceStreamAbsent)
            synthState match {
              case Inactive =>
                synthState = ActiveAbsent
                Seq(StreamAbsent)
              case ActiveAbsent =>
                Seq()
              case _: ActiveSessioned =>
                logger.debug(s"got initial absent in source for a currently active session")
                Seq()
            }

          case Some(ss) =>
            sourceStates.put(source, SourceStreamAbsent)
            ss match {
              case SourceSessioned(prevSourceSession) =>
                sessionToSources.removeValue(source)

                synthState match {
                  case Inactive =>
                    logger.warn(s"synth state was inactive on an event for an already registered source")
                    synthState = ActiveAbsent
                    Seq(StreamAbsent)
                  case ActiveAbsent =>
                    Seq()
                  case ActiveSessioned(sess, _) => {
                    if (sess == prevSourceSession) {
                      activeChangeCheck()
                    } else {
                      Seq()
                    }
                  }
                }

                if (activeSessionOpt.exists(_._1 == prevSourceSession)) {
                  activeChangeCheck()
                } else {
                  Seq()
                }
              case SourceStreamAbsent =>
                Seq()
            }
        }
      }
    }
  }

  // if the active session doesn't exist or has no sources, nominate a new session if it's the only one resolved
  // TODO: need to mark succeeded sessions to prevent immediately reverting to a prev session propped up by a source who is still delayed
  private def activeChangeCheck(): Seq[AppendEvent] = {
    val aliveActiveSession = activeSessionOpt.exists { case (sess, _) => sessionToSources.contains(sess) }
    if (!aliveActiveSession) {
      val keys = sessionToSources.keySet()
      if (keys.size == 1) {
        val nominatedSession = keys.head
        val queuedEvents = standbySessions.get(nominatedSession).map(q => q.dequeue()).getOrElse(Seq())
        standbySessions -= nominatedSession
        val filter = new StreamFilterImpl
        queuedEvents.foreach(filter.handle)
        synthState = ActiveSessioned(nominatedSession, filter)
        queuedEvents
      } else {
        if (keys.isEmpty && sourceStates.nonEmpty && sourceStates.forall(_._2 == SourceStreamAbsent)) {
          synthState = ActiveAbsent
          Seq(StreamAbsent)
        } else {
          Seq()
        }
      }
    } else {
      Seq()
    }
  }

  def sourceRemoved(source: Source): Seq[AppendEvent] = {
    val prevSessOpt = sessionToSources.valueGet(source)
    sessionToSources.removeValue(source)
    sourceStates.remove(source)

    prevSessOpt match {
      case None => Seq()
      case Some(prevSess) =>
        if (activeSessionOpt.map(_._1).contains(prevSess)) {
          activeChangeCheck()
        } else {
          if (!sessionToSources.contains(prevSess)) {
            standbySessions -= prevSess
          }
          Seq()
        }
    }
  }
}
