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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.colset._
import io.greenbus.edge.flow._
import io.greenbus.edge.thread.CallMarshaller

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
  def setRow(id: TableRow, metadata: Option[TypeValue]): SetEventSink
  def mapSetRow(id: TableRow, metadata: Option[TypeValue]): KeyedSetEventSink
  def appendSetRow(id: TableRow, maxBuffered: Int, metadata: Option[TypeValue]): AppendEventSink
  def dynamicTable(table: String, source: DynamicTableSource): Unit

  def requests: Source[Seq[RouteServiceRequest]]

  def flushEvents(): Unit
}

/*
- user pushes data
- event logs store some amount of it (latest value for sets)
- connection connects
- communicate routes
- connection sends subs
- subs fulfilled by giving some amount of data from the event logs

 */

trait BindableTableMgr {
  def bind(sender: Sender[RowAppendEvent, Boolean], session: PeerSessionId): Unit
  def setUpdate(keys: Set[TypeValue]): Unit
  def unbind(): Unit
}

trait DynamicTableSource {
  def added(row: TypeValue): BindableRowMgr
  def removed(row: TypeValue): Unit
}

object DynamicBindableTableMgr {

  def sendPair(row: RowId, session: PeerSessionId, sender: Sender[RowAppendEvent, Boolean]): (Sender[UserResync, Boolean], Sender[Delta, Boolean]) = {
    val snap = new Sender[UserResync, Boolean] {
      def send(obj: UserResync, handleResponse: (Try[Boolean]) => Unit): Unit = {
        sender.send(RowAppendEvent(row, ResyncSession(session, obj.ctx, obj.resync)), handleResponse)
      }
    }
    val delt = new Sender[Delta, Boolean] {
      def send(obj: Delta, handleResponse: (Try[Boolean]) => Unit): Unit = {
        sender.send(RowAppendEvent(row, StreamDelta(obj)), handleResponse)
      }
    }
    (snap, delt)
  }
}
class DynamicBindableTableMgr(route: TypeValue, table: String, source: DynamicTableSource) extends BindableTableMgr {

  private var rows = Map.empty[TypeValue, BindableRowMgr]
  private var bindingOpt = Option.empty[(Sender[RowAppendEvent, Boolean], PeerSessionId)]

  def bind(sender: Sender[RowAppendEvent, Boolean], session: PeerSessionId): Unit = {
    bindingOpt = Some((sender, session))
  }

  def setUpdate(keys: Set[TypeValue]): Unit = {

    val added = keys -- rows.keySet
    val removed = rows.keySet -- keys

    added.foreach { key =>
      val rowMgr = source.added(key)

      bindingOpt.foreach {
        case (sender, session) =>
          val (snap, delt) = DynamicBindableTableMgr.sendPair(RowId(route, table, key), session, sender)
          rowMgr.bind(snap, delt)
      }

      // Bind
      rows += (key -> rowMgr)
    }

    removed.foreach { row =>
      rows.get(row).foreach(_.unbind())
      source.removed(row)
    }
  }

  def unbind(): Unit = {
    rows.foreach(_._2.unbind())
    rows.keys.foreach(r => source.removed(r))
  }

}

trait BindableRowMgr {
  def bind(snapshot: Sender[UserResync, Boolean], deltas: Sender[Delta, Boolean]): Unit
  def unbind(): Unit
}

class RouteHandleImpl(eventThread: CallMarshaller, route: TypeValue, mgr: RouteMgr, reqSrc: Source[Seq[RouteServiceRequest]]) extends RouteSourceHandle {
  def setRow(id: TableRow, metadata: Option[TypeValue]): SetEventSink = {
    val bindable = new SetSink(SequenceCtx(None, metadata))
    eventThread.marshal {
      mgr.addBindable(id, bindable)
    }
    bindable
  }

  def mapSetRow(id: TableRow, metadata: Option[TypeValue]): KeyedSetEventSink = {
    val bindable = new KeyedSetSink(SequenceCtx(None, metadata))
    eventThread.marshal {
      mgr.addBindable(id, bindable)
    }
    bindable
  }

  def appendSetRow(id: TableRow, maxBuffered: Int, metadata: Option[TypeValue]): AppendEventSink = {
    val bindable = new AppendSink(maxBuffered, SequenceCtx(None, metadata), eventThread)
    eventThread.marshal {
      mgr.addBindable(id, bindable)
    }
    bindable
  }

  def dynamicTable(table: String, source: DynamicTableSource): Unit = {
    val b = new DynamicBindableTableMgr(route, table, source)
    eventThread.marshal {
      mgr.addDynamicTable(table, b)
    }
  }

  def requests: Source[Seq[RouteServiceRequest]] = reqSrc

  def flushEvents(): Unit = {
    mgr.flushEvents()
  }
}

object RouteMgr {
  case class Binding(buffer: EventBuffer, session: PeerSessionId, subscribed: Set[TableRow])
}
class RouteMgr(route: TypeValue, mgr: SourceMgr, requestSink: Sink[Seq[RouteServiceRequest]]) extends LazyLogging {
  import RouteMgr._

  private var dynamicTables = Map.empty[String, BindableTableMgr]
  private var rows = Map.empty[TableRow, BindableRowMgr]
  private var bindingOpt = Option.empty[Binding]

  def addBindable(row: TableRow, mgr: BindableRowMgr): Unit = {
    logger.debug(s"Route $route saw bindable added for row: $row")
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

  def addDynamicTable(table: String, mgr: BindableTableMgr): Unit = {
    logger.debug(s"Route $route saw dynamic bindable added for table: $table")
    if (dynamicTables.contains(table)) {
      throw new IllegalArgumentException(s"Cannot double bind a table: $table for route $route")
    }

    dynamicTables += (table -> mgr)
    bindingOpt.foreach { binding =>
      val rows = binding.subscribed.filter(_.table == table).map(_.rowKey)
      mgr.bind(binding.buffer.rowSender(), binding.session)
      mgr.setUpdate(rows)
    }
  }

  def subscriptionUpdate(subbed: Set[TableRow]): Unit = {
    logger.debug(s"Route $route subscription update: $subbed")
    bindingOpt.foreach { binding =>

      val before = binding.subscribed
      val added = subbed -- before
      val removed = before -- subbed

      logger.trace(s"added: $added")
      logger.trace(s"removed: $removed")

      val (removeDynamic, removeStatic) = removed.partition(tr => dynamicTables.contains(tr.table))

      removeStatic.flatMap(rows.get).foreach(_.unbind())
      val dynTablesWithRemoves = removeDynamic.map(_.table)

      val (addedDynamic, addedStatic) = added.partition(tr => dynamicTables.contains(tr.table))

      addedStatic.foreach { row =>
        val rowId = row.toRowId(route)
        rows.get(row).foreach { mgr =>
          mgr.bind(binding.buffer.snapshotSender(rowId), binding.buffer.deltaSender(rowId))
        }
      }

      val dynTablesWithAdds = addedDynamic.map(_.table)

      val dynTablesChanged = dynTablesWithRemoves ++ dynTablesWithAdds

      val subbedDynChanged = subbed.filter(tr => dynTablesChanged.contains(tr.table))
      val dynChangedByTable = subbedDynChanged.groupBy(_.table)
      dynChangedByTable.foreach {
        case (table, trs) =>
          dynamicTables.get(table).foreach(mgr => mgr.setUpdate(trs.map(_.rowKey)))
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

  def bind(buffer: EventBuffer, session: PeerSessionId): Unit = {
    logger.debug(s"Route $route bound")
    bindingOpt = Some(Binding(buffer, session, Set()))
    dynamicTables.foreach {
      case (_, bindable) => bindable.bind(buffer.rowSender(), session)
    }
  }

  def unbindAll(): Unit = {
    logger.debug(s"Route $route unbound")
    bindingOpt = None
    rows.values.foreach(_.unbind())
    dynamicTables.foreach(_._2.unbind())
  }

}

object GatewayRouteSource {
  def build(eventThread: CallMarshaller): GatewayRouteSource = {
    new SourceMgr(eventThread)
  }
}
trait GatewayRouteSource {
  def route(route: TypeValue): RouteSourceHandle
  def connect(channel: GatewayProxyChannel): Unit
}

case class SourceConnectedState(buffer: EventBuffer, subscribedRows: Set[RowId], unsourcedRoutes: Map[TypeValue, Set[TableRow]])

// TODO: get rid of unsourced routes in favor of something like just storing subs separately or blank route mgrs like in peer sourcing mgr
class SourceMgr(eventThread: CallMarshaller) extends GatewayRouteSource with LazyLogging {

  private val gatewaySession = PeerSessionId(UUID.randomUUID(), 0)
  private var routes = Map.empty[TypeValue, RouteMgr]

  private var connectionOpt = Option.empty[SourceConnectedState]

  def route(route: TypeValue): RouteSourceHandle = {
    val requestDist = new RemoteBoundQueuedDistributor[Seq[RouteServiceRequest]](eventThread)
    val mgr = new RouteMgr(route, this, requestDist)
    eventThread.marshal {
      onRouteSourced(route, mgr)
    }
    new RouteHandleImpl(eventThread, route, mgr, requestDist)
  }

  private def onRouteSourced(route: TypeValue, mgr: RouteMgr): Unit = {
    routes += (route -> mgr)

    logger.debug(s"Route $route sourced, connected: " + connectionOpt.nonEmpty)
    connectionOpt.foreach { ctx =>
      mgr.bind(ctx.buffer, gatewaySession)
      ctx.unsourcedRoutes.get(route).foreach { rows =>
        mgr.subscriptionUpdate(rows)
      }
      ctx.buffer.flush(Some(routes.keySet))

      connectionOpt = Some(ctx.copy(unsourcedRoutes = ctx.unsourcedRoutes - route))
    }
  }

  def routeFlushed(route: TypeValue): Unit = {
    eventThread.marshal {
      connectionOpt.foreach(_.buffer.flush(None))
    }
  }

  private def onConnect(buffer: EventBuffer): Unit = {
    logger.debug(s"SourceMgr connected")
    connectionOpt = Some(SourceConnectedState(buffer, Set(), Map()))

    routes.foreach {
      case (route, mgr) => mgr.bind(buffer, gatewaySession)
    }
    buffer.flush(Some(routes.keySet))
  }

  def connect(proxy: GatewayProxyChannel): Unit = {
    logger.debug(s"Channel connected")
    proxy.onClose.subscribe(() => eventThread.marshal { onConnectionClosed(proxy) })
    proxy.requests.bind(reqs => eventThread.marshal { onServiceRequest(proxy, reqs) })
    proxy.subscriptions.bind(rows => eventThread.marshal { onSubscriptionUpdate(rows) })

    val buffer = new EventBuffer(proxy, gatewaySession)
    eventThread.marshal {
      onConnect(buffer)
    }
  }

  private def onSubscriptionUpdate(rows: Set[RowId]): Unit = {
    logger.debug(s"Row subscriptions updated: " + rows)

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
          val rowsForRoute = byRoute.getOrElse(route, Set()).map(_.tableRow)
          routes.get(route) match {
            case None => {
              if (rowsForRoute.nonEmpty) {
                unsourced += (route -> rowsForRoute)
              }
            }
            case Some(mgr) => mgr.subscriptionUpdate(rowsForRoute)
          }
        }

        ctx.buffer.flush(None)

        connectionOpt = Some(ctx.copy(subscribedRows = rows, unsourcedRoutes = unsourced))
      }
    }

  }

  private def onServiceRequest(proxy: GatewayProxyChannel, serviceRequestBatch: Seq[ServiceRequest]): Unit = {

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

  private def onConnectionClosed(proxy: GatewayProxyChannel): Unit = {
    logger.debug(s"Channel closed")
    connectionOpt = None
    routes.foreach {
      case (route, mgr) => mgr.unbindAll()
    }
  }
}

case class UserResync(ctx: SequenceCtx, resync: Resync)

class EventBuffer(proxy: GatewayProxy, session: PeerSessionId) extends LazyLogging {

  private val events = mutable.ArrayBuffer.empty[StreamEvent]
  private val callbacks = mutable.ArrayBuffer.empty[Try[Boolean] => Unit]

  def snapshotSender(row: RowId): Sender[UserResync, Boolean] = {
    new Sender[UserResync, Boolean] {
      def send(obj: UserResync, handleResponse: (Try[Boolean]) => Unit): Unit = {
        events += RowAppendEvent(row, ResyncSession(session, obj.ctx, obj.resync))
        callbacks += handleResponse
      }
    }
  }

  def deltaSender(row: RowId): Sender[Delta, Boolean] = {
    new Sender[Delta, Boolean] {
      def send(obj: Delta, handleResponse: (Try[Boolean]) => Unit): Unit = {
        events += RowAppendEvent(row, StreamDelta(obj))
        callbacks += handleResponse
      }
    }
  }

  def rowSender(): Sender[RowAppendEvent, Boolean] = {
    new Sender[RowAppendEvent, Boolean] {
      def send(obj: RowAppendEvent, handleResponse: (Try[Boolean]) => Unit): Unit = {
        events += obj
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
