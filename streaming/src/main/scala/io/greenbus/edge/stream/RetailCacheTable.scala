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
package io.greenbus.edge.stream

import com.typesafe.scalalogging.LazyLogging

class RetailCacheTable extends LazyLogging {
  private var rows = Map.empty[TypeValue, Map[TableRow, StreamCache]]

  def getSync(row: RowId): Seq[StreamEvent] = {
    lookup(row).map(log => log.sync())
      .map(ev => Seq(RowAppendEvent(row, ev)))
      .getOrElse(Seq())
  }

  def handleBatch(events: Seq[StreamEvent]): Unit = {
    events.foreach {
      case ev: RowAppendEvent => {
        lookup(ev.rowId) match {
          case None =>
            addRouted(ev, ev.rowId.routingKey, rows.getOrElse(ev.rowId.routingKey, Map()))
          case Some(log) => log.handle(ev.appendEvent)
        }
      }
      case ev: RouteUnresolved =>
    }
  }

  private def lookup(rowId: RowId): Option[StreamCache] = {
    rows.get(rowId.routingKey).flatMap { map =>
      map.get(rowId.tableRow)
    }
  }

  private def addRouted(ev: RowAppendEvent, routingKey: TypeValue, existingRows: Map[TableRow, StreamCache]): Unit = {
    ev.appendEvent match {
      case resync: ResyncSession =>
        val cache = new RetailStreamCache(ev.rowId.toString, resync)
        rows += (routingKey -> (existingRows + (ev.rowId.tableRow -> cache)))
      case _ =>
        logger.warn(s"Initial row event was not resync session: $ev")
    }
  }

  def removeRoute(route: TypeValue): Unit = {
    rows -= route
  }
}