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
package io.greenbus.edge.stream.consume

import java.util.UUID

import io.greenbus.edge.flow.{ Handler, RemoteBoundQueuedDistributor, Sink, Source }
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.engine2._
import io.greenbus.edge.stream.subscribe.{ RowUpdate, ValueUnresolved, ValueUpdate }
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable

class UserUpdateSink(queue: UserUpdateQueue, notify: (UserUpdateQueue) => Unit) extends KeyStreamObserver {
  private val synth = new ValueUpdateSynthesizerImpl
  def handle(event: AppendEvent): Unit = {
    synth.handle(event).foreach(queue.enqueue)
    notify(queue)
  }
  def unresolved(): Unit = {
    queue.enqueue(ValueUnresolved)
    notify(queue)
  }
}

trait UserUpdateQueue {
  def enqueue(update: ValueUpdate): Unit
  def flush(): Unit
}

class UserRouteSubscription(val map: Map[TableRow, UserUpdateSink]) extends StreamObserver {
  def handle(routeEvent: StreamEvent): Unit = {
    routeEvent match {
      case RouteUnresolved(_) => map.values.foreach(_.unresolved())
    }
  }
}

object UserSubscriptionSynth {
  def build(rows: Set[RowId]): UserSubscriptionSynth = {
    val map = rows.groupBy(_.routingKey).map {
      case (route, rowMap) =>
        val synthMap = rowMap.map { row =>
          (row.tableRow, new ValueUpdateSynthesizerImpl)
        }.toMap

        route -> synthMap
    }

    new UserSubscriptionSynth(map)
  }
}
class UserSubscriptionSynth(map: Map[TypeValue, Map[TableRow, ValueUpdateSynthesizer]]) {
  def handle(events: Seq[StreamEvent]): Seq[RowUpdate] = {
    val results = Vector.newBuilder[RowUpdate]
    events.foreach {
      case ev: RowAppendEvent =>
        lookup(ev.rowId).foreach { synth =>
          synth.handle(ev.appendEvent).foreach { up => results += RowUpdate(ev.rowId, up) }
        }
      case ev: RouteUnresolved =>
        map.get(ev.routingKey).foreach { rows =>
          rows.foreach {
            case (row, _) =>
              results += RowUpdate(row.toRowId(ev.routingKey), ValueUnresolved)
          }
        }
    }

    results.result()
  }

  private def lookup(rowId: RowId): Option[ValueUpdateSynthesizer] = {
    map.get(rowId.routingKey).flatMap(m => m.get(rowId.tableRow))
  }
}

