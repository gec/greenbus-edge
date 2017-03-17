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

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.CallMarshaller
import io.greenbus.edge.colset._
import io.greenbus.edge.flow._

import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

case class RouteServiceRequest(row: TableRow, value: TypeValue, respond: TypeValue => Unit)

sealed trait EventSink

trait SetEventSink extends EventSink {
  def update(set: Set[TypeValue])
}

trait KeyedSetEventSink extends EventSink {
  def update(map: Map[TypeValue, TypeValue])
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

trait RouteSourceHandle {
  def setRow(id: TableRow): SetEventSink
  def keyedSetRow(id: TableRow): KeyedSetEventSink
  def appendSetRow(id: TableRow, maxBuffered: Int): AppendEventSink

  def requests: Source[Seq[RouteServiceRequest]]

  def flushEvents(): Unit

  //def close(): Unit
}

trait RouteSourceSubscription extends Closeable {

}

/*
- user pushes data
- event logs store some amount of it (latest value for sets)
- connection connects
- communicate routes
- connection sends subs
- subs fulfilled by giving some amount of data from the event logs

 */

trait BindableRowMgr {
  def bind(snapshot: Sender[SetSnapshot, Boolean], deltas: Sender[SetDelta, Boolean]): Unit
  def unbind(): Unit
}

class RouteHandleImpl(eventThread: CallMarshaller, mgr: RouteMgr, reqSrc: Source[Seq[RouteServiceRequest]]) extends RouteSourceHandle {
  def setRow(id: TableRow): SetEventSink = {
    val bindable = new SetSink
    mgr.addBindable(id, bindable)
    bindable
  }

  def keyedSetRow(id: TableRow): KeyedSetEventSink = {
    val bindable = new KeyedSetSink
    mgr.addBindable(id, bindable)
    bindable
  }

  def appendSetRow(id: TableRow, maxBuffered: Int): AppendEventSink = {
    val bindable = new AppendSink(maxBuffered, eventThread)
    mgr.addBindable(id, bindable)
    bindable
  }

  def requests: Source[Seq[RouteServiceRequest]] = reqSrc

  def flushEvents(): Unit = {
    mgr.flushEvents()
  }
}

object RouteMgr {
  case class Binding(buffer: EventBuffer, subscribed: Set[TableRow])
}
class RouteMgr(route: TypeValue, mgr: SourceMgr, requestSink: Sink[Seq[RouteServiceRequest]]) {
  import RouteMgr._

  private var rows = Map.empty[TableRow, BindableRowMgr]
  private var bindingOpt = Option.empty[Binding]

  def addBindable(row: TableRow, mgr: BindableRowMgr): Unit = {
    if (rows.contains(row)) {
      throw new IllegalArgumentException(s"Cannot double bind a row: $row for route $route")
    }

    rows += (row -> mgr)
    bindingOpt.foreach { binding =>
      if (binding.subscribed.contains(row)) {
        val rowId = row.toRowId(route)
        mgr.bind(binding.buffer.snapshotSender(rowId), binding.buffer.deltaSender(rowId))
      }
    }
  }

  def subscriptionUpdate(subbed: Set[TableRow]): Unit = {
    bindingOpt.foreach { binding =>

      val before = binding.subscribed
      val added = subbed -- before
      val removed = before -- subbed

      removed.flatMap(rows.get).foreach(_.unbind())
      added.foreach { row =>
        val rowId = row.toRowId(route)
        rows.get(row).foreach { mgr =>
          mgr.bind(binding.buffer.snapshotSender(rowId), binding.buffer.deltaSender(rowId))
        }
      }

      bindingOpt = Some(binding.copy(subscribed = subbed))
    }
  }

  def handleRequests(reqs: Seq[RouteServiceRequest]): Unit = {
    requestSink.push(reqs)
  }

  def flushEvents(): Unit = {
    mgr.routeFlushed(route)
  }

  def bind(buffer: EventBuffer): Unit = {
    bindingOpt = Some(Binding(buffer, Set()))
  }

  def unbindAll(): Unit = {
    bindingOpt = None
    rows.values.foreach(_.unbind())
  }

}

case class SourceConnectedState(buffer: EventBuffer, subscribedRows: Set[RowId], unsourcedRoutes: Map[TypeValue, Set[TableRow]])

class SourceMgr(eventThread: CallMarshaller) extends LazyLogging {

  private var routes = Map.empty[TypeValue, RouteMgr]

  private var connectionOpt = Option.empty[SourceConnectedState]

  def route(route: TypeValue): RouteSourceHandle = {
    val requestDist = new RemoteBoundQueuedDistributor[Seq[RouteServiceRequest]](eventThread)
    val mgr = new RouteMgr(route, this, requestDist)
    eventThread.marshal {
      onRouteSourced(route, mgr)
    }
    new RouteHandleImpl(eventThread, mgr, requestDist)
  }

  private def onRouteSourced(route: TypeValue, mgr: RouteMgr): Unit = {
    routes += (route -> mgr)

    connectionOpt.foreach { ctx =>
      ctx.unsourcedRoutes.get(route).foreach { rows =>
        mgr.bind(ctx.buffer)
        mgr.subscriptionUpdate(rows)
      }
      ctx.buffer.flush(Some(routes.keySet))
    }
  }

  def routeFlushed(route: TypeValue): Unit = {
    connectionOpt.foreach(_.buffer.flush(None))
  }

  def connected(proxy: GatewayProxyChannel): Unit = {
    proxy.onClose.subscribe(() => connectionClosed(proxy))
    proxy.requests.bind(reqs => serviceRequest(proxy, reqs))
    proxy.subscriptions.bind(rows => subscribed(rows))

    val buffer = new EventBuffer(proxy)
    connectionOpt = Some(SourceConnectedState(buffer, Set(), Map()))

    routes.foreach {
      case (route, mgr) => mgr.bind(buffer)
    }
    buffer.flush(Some(routes.keySet))
  }

  def subscribed(rows: Set[RowId]): Unit = {

    connectionOpt match {
      case None => logger.warn(s"Received subscriptions while disconnected")
      case Some(ctx) => {

        val added = rows -- ctx.subscribedRows
        val removed = ctx.subscribedRows -- rows

        val addedRoutes = added.map(_.routingKey)
        val removedRoutes = removed.map(_.routingKey)

        val modifiedRoutes = addedRoutes ++ removedRoutes

        val byRoute = rows.groupBy(_.routingKey)

        var unsourced = ctx.unsourcedRoutes -- removedRoutes

        modifiedRoutes.foreach { route =>
          byRoute.get(route).foreach { rowIds =>
            val tableRows = rowIds.map(_.tableRow)
            routes.get(route) match {
              case None => unsourced += (route -> tableRows)
              case Some(mgr) => mgr.subscriptionUpdate(tableRows)
            }
          }
        }

        ctx.buffer.flush(None)

        connectionOpt = Some(ctx.copy(subscribedRows = rows, unsourcedRoutes = unsourced))
      }
    }

  }

  def serviceRequest(proxy: GatewayProxyChannel, serviceRequestBatch: Seq[ServiceRequest]): Unit = {

    serviceRequestBatch.groupBy(_.row.routingKey).foreach {
      case (route, reqs) => {

        routes.get(route) match {
          case None => logger.debug(s"Service requests for unbound route $route")
          case Some(mgr) =>
            val prepared = reqs.map { req =>
              def respond(tv: TypeValue): Unit = {
                val resp = ServiceResponse(req.row, tv, req.correlation)
                proxy.responses.push(Seq(resp))
              }

              RouteServiceRequest(req.row.tableRow, req.value, respond)
            }

            mgr.handleRequests(prepared)
        }
      }
    }
  }

  def connectionClosed(proxy: GatewayProxyChannel): Unit = {
    connectionOpt = None
    routes.foreach {
      case (route, mgr) => mgr.unbindAll()
    }
  }
}

class EventBuffer(proxy: GatewayProxy) extends LazyLogging {

  private val events = mutable.ArrayBuffer.empty[StreamEvent]
  private val callbacks = mutable.ArrayBuffer.empty[Try[Boolean] => Unit]

  def snapshotSender(row: RowId): Sender[SetSnapshot, Boolean] = {
    new Sender[SetSnapshot, Boolean] {
      def send(obj: SetSnapshot, handleResponse: (Try[Boolean]) => Unit): Unit = {
        events += RowAppendEvent(row, ResyncSnapshot(obj))
        callbacks += handleResponse
      }
    }
  }

  def deltaSender(row: RowId): Sender[SetDelta, Boolean] = {
    new Sender[SetDelta, Boolean] {
      def send(obj: SetDelta, handleResponse: (Try[Boolean]) => Unit): Unit = {
        events += RowAppendEvent(row, StreamDelta(obj))
        callbacks += handleResponse
      }
    }
  }

  def flush(routeUpdates: Option[Set[TypeValue]]): Unit = {
    if (events.nonEmpty || routeUpdates.nonEmpty) {
      val evs = events.toVector
      val cbs = callbacks.toVector
      events.clear()
      callbacks.clear()

      def onResponse(result: Try[Boolean]): Unit = {
        result match {
          case Success(_) => cbs.foreach(f => f(Success(true)))
          case Failure(ex) =>
            logger.debug(s"EventBuffer send failure: " + ex)
        }
      }

      proxy.events.send(GatewayEvents(routeUpdates, evs), onResponse)
    }
  }
}
