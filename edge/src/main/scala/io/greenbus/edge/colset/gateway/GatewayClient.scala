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
package io.greenbus.edge.colset.gateway

import io.greenbus.edge.CallMarshaller
import io.greenbus.edge.colset._
import io.greenbus.edge.flow.{ Closeable, Source }

case class RouteServiceRequest(row: TableRow, value: TypeValue, respond: TypeValue => Unit)

sealed trait EventSink

trait SetEventSink extends EventSink {
  def update(set: Set[TypeValue])
  //def update(removes: Set[TypeValue], adds: Set[TypeValue])
}

trait KeyedSetEventSink extends EventSink {
  def update(map: Map[TypeValue, TypeValue])
  //def update(removes: Set[TypeValue], adds: Set[(TypeValue, TypeValue)], modifies: Set[(TypeValue, TypeValue)])
}

trait AppendEventSink extends EventSink {
  def append(value: TypeValue*): Unit
}

trait DynamicTable {
  def rowsSubscribed(rowKeys: Set[TypeValue])
  def rowsUnsubscribed(rowKeys: Set[TypeValue])
}

/*
Support:
- vanilla cases
- dynamic typed maps
- dynamic keys (i.e. prefixed index subscriptions)
- hopefully peer manifest stuff

 */
trait RouteSource {

  def events: Map[TableRow, EventSink]

  def requests: Source[Seq[RouteServiceRequest]]

  def close(): Unit
}

trait GatewayClient {

}

trait RouteSourceHandle {
  def setRow(id: TableRow): SetEventSink
  def keyedSetRow(id: TableRow): KeyedSetEventSink
  def appendSetRow(id: TableRow): AppendEventSink

  def requests: Source[Seq[RouteServiceRequest]]

  def flushEvents(): Unit

  def close(): Unit
}

trait RouteSourceSubscription extends Closeable {

}

class RouteSourceMgr(route: TypeValue) {

  //def resync(): Seq[StreamEvent]

}
/*
- user pushes data
- event logs store some amount of it (latest value for sets)
- connection connects
- communicate routes
- connection sends subs
- subs fulfilled by giving some amount of data from the event logs

 */

class SetLog extends SetEventSink {

  def update(set: Set[TypeValue]): Unit = {

  }
}

class RouteManager(eventThread: CallMarshaller) {

  private var routes = Map.empty[TypeValue, RouteSourceMgr]

  private var subscribedRows = Set.empty[RowId]

  //private var subscription

  def connected(proxy: GatewayProxyChannel): Unit = {
    proxy.onClose.subscribe(() => connectionClosed(proxy))
    proxy.requests.bind(reqs => serviceRequest(proxy, reqs))
    proxy.subscriptions.bind(rows => subscribed(proxy, rows))

    val initialEvent = GatewayEvents(Some(routes.keySet), Seq())
    proxy.events.push(initialEvent)
  }

  def subscribed(proxy: GatewayProxyChannel, rows: Set[RowId]): Unit = {

    val added = rows -- subscribedRows
    val removed = subscribedRows -- rows
    subscribedRows = rows

    //added.groupBy(_.routingKey)
  }

  def serviceRequest(proxy: GatewayProxyChannel, serviceRequestBatch: Seq[ServiceRequest]): Unit = {

  }

  def connectionClosed(proxy: GatewayProxyChannel): Unit = {

  }
}

