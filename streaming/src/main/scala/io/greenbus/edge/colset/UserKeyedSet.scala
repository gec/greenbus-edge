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

case class KeyedSetDiff[A, B](snapshot: Map[A, B], removed: Set[A], added: Set[(A, B)], modified: Set[(A, B)])

trait UserKeyedSet[A, B] {
  def handle(events: Seq[AppendEvent]): Unit
  def lastSnapshot: Map[A, B]
  def dequeue(): Option[KeyedSetDiff[A, B]]
}

object RawUserKeyedSet {
  def build: RawUserKeyedSet = {
    new RawUserKeyedSetImpl
  }
}
trait RawUserKeyedSet extends UserKeyedSet[TypeValue, TypeValue] {
  def handle(events: Seq[AppendEvent]): Unit
  def lastSnapshot: Map[TypeValue, TypeValue]
  def dequeue(): Option[KeyedSetDiff[TypeValue, TypeValue]]
}
class RawUserKeyedSetImpl extends RawUserKeyedSet with LazyLogging {

  private var last = Map.empty[TypeValue, TypeValue]
  private var updatedOpt = Option.empty[Map[TypeValue, TypeValue]]

  def lastSnapshot: Map[TypeValue, TypeValue] = last

  def handleDelta(sd: SetDelta): Unit = {
    sd match {
      case delta: ModifiedKeyedSetDelta =>
        val updates = (updatedOpt.getOrElse(last) -- delta.removes) ++ delta.adds ++ delta.modifies
        updatedOpt = Some(updates)
      case _ =>
        logger.error("UserKeyedSet saw non-keyed set delta")
    }
  }
  def handleSnapshot(sd: SetSnapshot): Unit = {
    sd match {
      case snap: ModifiedKeyedSetSnapshot =>
        updatedOpt = Some(snap.snapshot)
      case _ =>
        logger.error("UserKeyedSet saw non-keyed set delta")
    }
  }

  def handle(events: Seq[AppendEvent]): Unit = {
    events.foreach {
      case StreamDelta(sd) => handleDelta(sd)
      case ResyncSnapshot(ss) => handleSnapshot(ss)
      case ResyncSession(_, ss) => handleSnapshot(ss)
    }
  }

  def dequeue(): Option[KeyedSetDiff[TypeValue, TypeValue]] = {
    updatedOpt.map { updated =>

      val removed = last.keySet -- updated.keySet

      val added = Vector.newBuilder[(TypeValue, TypeValue)]
      val modified = Vector.newBuilder[(TypeValue, TypeValue)]
      updated.foreach {
        case (k, v) =>
          last.get(k) match {
            case None => added += (k -> v)
            case Some(lastV) =>
              if (lastV != v) {
                modified += (k -> v)
              }
          }
      }

      last = updated
      updatedOpt = None

      KeyedSetDiff(updated, removed, added.result().toSet, modified.result().toSet)
    }
  }
}

class RenderedUserKeyedSet[A, B](parseKey: TypeValue => Option[A], parseValue: TypeValue => Option[B]) extends UserKeyedSet[A, B] {

  private val raw = RawUserKeyedSet.build

  def handle(events: Seq[AppendEvent]): Unit = raw.handle(events)

  private def parseTuple(tup: (TypeValue, TypeValue)): Option[(A, B)] = {
    tup match {
      case (k, v) =>
        for { pK <- parseKey(k); pV <- parseValue(v) } yield (pK, pV)
    }
  }

  def lastSnapshot: Map[A, B] = {
    raw.lastSnapshot.flatMap(parseTuple)
  }

  def dequeue(): Option[KeyedSetDiff[A, B]] = {
    raw.dequeue().map { update =>
      KeyedSetDiff(update.snapshot.flatMap(parseTuple),
        update.removed.map(parseKey).flatten,
        update.added.flatMap(parseTuple),
        update.modified.flatMap(parseTuple))
    }
  }
}