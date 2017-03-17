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
import io.greenbus.edge.flow.{ Closeable, Responder, Sender, Sink, Source }

import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

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

/*trait GatewayClient {

}*/

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

/*
- user pushes data
- event logs store some amount of it (latest value for sets)
- connection connects
- communicate routes
- connection sends subs
- subs fulfilled by giving some amount of data from the event logs

 */

trait BindableRowMgr {
  def bind(sink: Sender[SetDelta, Boolean]): SetSnapshot
  def unbind(): Unit
}

class SetLog extends SetEventSink {

  def update(set: Set[TypeValue]): Unit = {

  }
}
class AppendLog(maxBuffered: Int, eventThread: CallMarshaller) extends AppendEventSink with BindableRowMgr with LazyLogging {
  private var buffer = Vector.empty[(Long, TypeValue)]
  private var sequence: Long = 0
  private var publishOpt = Option.empty[Sender[SetDelta, Boolean]]

  def bind(sink: Sender[SetDelta, Boolean]): SetSnapshot = {
    publishOpt = Some(sink)
    toAppendSeq(buffer)
  }
  def unbind(): Unit = {
    publishOpt = None
  }

  def confirmed(seq: Long): Unit = {
    eventThread.marshal {
      buffer = buffer.dropWhile(_._1 <= seq)
    }
  }

  def append(values: TypeValue*): Unit = {
    eventThread.marshal {
      if (values.nonEmpty) {
        val vseq = Range(0, values.size).map(_ + sequence).zip(values).toVector
        buffer ++= vseq
        sequence += values.size
        publishOpt.foreach { publisher =>
          publish(publisher, vseq)
        }
      }
    }
  }

  private def publish(publisher: Sender[SetDelta, Boolean], updates: Seq[(Long, TypeValue)]): Unit = {
    if (updates.nonEmpty) {

      val last = updates.last._1

      def handleResult(result: Try[Boolean]): Unit = {
        result match {
          case Success(_) => confirmed(last)
          case Failure(ex) =>
            logger.debug(s"AppendLog send failure: " + ex)
        }
      }

      publisher.send(toAppendSeq(updates), handleResult)
    }
  }

  private def toAppendSeq(updates: Seq[(Long, TypeValue)]): AppendSetSequence = {
    val values = updates.map {
      case (seq, v) => AppendSetValue(Int64Val(seq), v)
    }

    AppendSetSequence(values)
  }
}

class RouteHandleImpl(mgr: RouteSourceMgr) extends RouteSourceHandle {
  def setRow(id: TableRow): SetEventSink = ???

  def keyedSetRow(id: TableRow): KeyedSetEventSink = ???

  def appendSetRow(id: TableRow): AppendEventSink = ???

  def requests: Source[Seq[RouteServiceRequest]] = ???

  def flushEvents(): Unit = ???

  def close(): Unit = ???
}

class RouteSourceMgr(route: TypeValue) {

  private var rows = Map.empty[TableRow, BindableRowMgr]
  //private var unfulfilledSubs = Map.empty[TableRow, Sender[]]

  def subscriptionUpdate(buffer: EventBuffer, added: Set[TableRow], removed: Set[TableRow]): Seq[RowAppendEvent] = {
    removed.flatMap(rows.get).foreach(_.unbind())
    val snaps = added.toVector.flatMap { row =>
      rows.get(row).map(mgr => mgr.bind(buffer.sender(row.toRowId(route)))).map(ss => (row, ss))
    }

    snaps.map { case (tr, ss) => RowAppendEvent(tr.toRowId(route), ResyncSnapshot(ss)) }
  }
  def unbindAll(): Unit = {
    rows.values.foreach(_.unbind())
  }

}

class EventBuffer(proxy: GatewayProxy) extends LazyLogging {

  private val events = mutable.ArrayBuffer.empty[StreamEvent]
  private val callbacks = mutable.ArrayBuffer.empty[Try[Boolean] => Unit]

  def sender(row: RowId): Sender[SetDelta, Boolean] = {
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

class RouteManager(eventThread: CallMarshaller) {

  private var routes = Map.empty[TypeValue, RouteSourceMgr]

  private var subscribedRows = Set.empty[RowId]

  private var bufferOpt = Option.empty[EventBuffer]
  //private var subscription

  def connected(proxy: GatewayProxyChannel): Unit = {
    proxy.onClose.subscribe(() => connectionClosed(proxy))
    proxy.requests.bind(reqs => serviceRequest(proxy, reqs))
    proxy.subscriptions.bind(rows => subscribed(proxy, rows))

    val initialEvent = GatewayEvents(Some(routes.keySet), Seq())
    proxy.events.send(initialEvent, _ => {})
  }

  def subscribed(proxy: GatewayProxyChannel, rows: Set[RowId]): Unit = {

    val added = rows -- subscribedRows
    val removed = subscribedRows -- rows
    subscribedRows = rows

    val addedPerRoute = added.groupBy(_.routingKey)
    val removedPerRoute = removed.groupBy(_.routingKey)

    val allModifiedRoutes = addedPerRoute.keySet ++ removedPerRoute.keySet

    val buffer = new EventBuffer(proxy)
    bufferOpt = Some(buffer)

    allModifiedRoutes.flatMap { route =>
      routes.get(route) match {
        case None => Seq()
        case Some(mgr) => {
          val addedForRoute = addedPerRoute.getOrElse(route, Set())
          val removedForRoute = removedPerRoute.getOrElse(route, Set())
          mgr.subscriptionUpdate(buffer, addedForRoute.map(_.tableRow), removedForRoute.map(_.tableRow))
        }
      }
    }

  }

  def serviceRequest(proxy: GatewayProxyChannel, serviceRequestBatch: Seq[ServiceRequest]): Unit = {

  }

  def connectionClosed(proxy: GatewayProxyChannel): Unit = {
    routes.foreach {
      case (route, mgr) => mgr.unbindAll()
    }
    subscribedRows = Set()
    bufferOpt = None
  }
}

