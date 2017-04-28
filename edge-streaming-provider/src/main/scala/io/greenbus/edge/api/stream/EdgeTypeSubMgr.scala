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
package io.greenbus.edge.api.stream

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.stream.TypeValue
import io.greenbus.edge.stream.subscribe._

trait ObservedEdgeTypeSubMgr extends EdgeTypeSubMgr {

  private var observerSet = Set.empty[EdgeUpdateQueue]
  def observers: Set[EdgeUpdateQueue] = observerSet
  def addObserver(buffer: EdgeUpdateQueue): Unit = {
    observerSet += buffer
  }
  def removeObserver(buffer: EdgeUpdateQueue): Unit = {
    observerSet -= buffer
  }
}

trait EdgeTypeSubMgr {
  def codec: EdgeSubCodec
  def handle(update: ValueUpdate): Set[EdgeUpdateQueue]
  def observers: Set[EdgeUpdateQueue]
  def addObserver(buffer: EdgeUpdateQueue): Unit
  def removeObserver(buffer: EdgeUpdateQueue): Unit
}

trait EdgeSubCodec {

  def simpleToUpdate(v: EdgeDataStatus[Nothing]): IdentifiedEdgeUpdate

  def updateFor(v: DataValueUpdate, metaOpt: Option[TypeValue]): Seq[IdentifiedEdgeUpdate]
}

class GenEdgeTypeSubMgrComp(logId: String, val codec: EdgeSubCodec) extends ObservedEdgeTypeSubMgr with LazyLogging {

  def handle(update: ValueUpdate): Set[EdgeUpdateQueue] = {
    update match {
      case vs: ValueSync =>
        val updates = codec.updateFor(vs.initial, vs.metadata)
        if (updates.nonEmpty) {
          observers.foreach(obs => updates.foreach(up => obs.enqueue(up)))
          observers
        } else {
          Set()
        }
      case vd: ValueDelta =>
        val updates = codec.updateFor(vd.update, None)
        if (updates.nonEmpty) {
          observers.foreach(obs => updates.foreach(up => obs.enqueue(up)))
          observers
        } else {
          Set()
        }
      case ValueAbsent =>
        val up = codec.simpleToUpdate(ResolvedAbsent)
        observers.foreach(_.enqueue(up))
        observers
      case ValueUnresolved =>
        val up = codec.simpleToUpdate(DataUnresolved)
        observers.foreach(_.enqueue(up))
        observers
      case ValueDisconnected =>
        val up = codec.simpleToUpdate(Disconnected)
        observers.foreach(_.enqueue(up))
        observers
      case _ => Set()
    }
  }
}
