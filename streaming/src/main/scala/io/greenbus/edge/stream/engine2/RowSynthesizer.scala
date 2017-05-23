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
      case Some(prev) => keyToValueMap.update(key, prev)
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

class RowSynthImpl[Source] extends RowSynthesizer[Source] with LazyLogging {

  private var activeSessionOpt = Option.empty[(PeerSessionId, StreamFilter)]
  private val standbySessions = mutable.Map.empty[PeerSessionId, StreamQueue]
  private val sessionToSources = new UniqueValueMultiMap[PeerSessionId, Source]

  def append(source: Source, event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case resync: ResyncSession => {
        val prevSessOpt = sessionToSources.valueGet(source)
        if (!prevSessOpt.contains(resync.sessionId)) {
          sessionToSources.put(resync.sessionId, source)
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
      case delta: StreamDelta => {
        sessionToSources.valueGet(source) match {
          case None =>
            logger.warn(s"got delta for inactive source link")
            Seq()
          case Some(sourceSess) => {
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
        activeSessionOpt = Some((nominatedSession, filter))
        queuedEvents
      } else {
        Seq()
      }
    } else {
      Seq()
    }
  }

  def sourceRemoved(source: Source): Seq[AppendEvent] = {
    val prevSessOpt = sessionToSources.valueGet(source)
    sessionToSources.removeValue(source)

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
