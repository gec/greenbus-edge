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

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.collection.OneToManyUniquely
import io.greenbus.edge.flow._

import scala.collection.mutable

trait GatewayProxy extends ServiceConsumer {
  def subscriptions: Source[Set[RowId]]
  def events: Sender[GatewayEvents, Boolean]
}
trait GatewayProxyChannel extends GatewayProxy with CloseableComponent

trait GatewayClientProxy extends ServiceProvider {
  def subscriptions: Sink[Set[RowId]]
  def events: Source[GatewayEvents]
}

trait GatewayClientProxyChannel extends GatewayClientProxy with CloseableComponent

case class GatewayEvents(routesUpdate: Option[Set[TypeValue]], events: Seq[StreamEvent])
case class GatewayClientContext(proxy: GatewayClientProxy, id: UUID)

class Gateway(localSession: PeerSessionId) extends LocalGateway with LazyLogging {

  private val clientToRoutes = OneToManyUniquely.empty[GatewayClientProxy, TypeValue]
  private val subscriptions = mutable.Map.empty[TypeValue, Set[TableRow]]

  private val eventDist = new QueuedDistributor[GatewayEvents]
  private val respDist = new QueuedDistributor[Seq[ServiceResponse]]

  // Keyed off of UUID because the source ID stays around until re-written, which may be never
  private val synthesizer = new GatewaySynthesizer[UUID](localSession)

  def events: Source[GatewayEvents] = eventDist
  def responses: Source[Seq[ServiceResponse]] = respDist

  def handleClientOpened(proxy: GatewayClientProxy): Unit = {
    val id = UUID.randomUUID()
    val ctx = GatewayClientContext(proxy, id)
    proxy.events.bind(ev => clientEvents(ctx, ev.routesUpdate, ev.events))
    proxy.responses.bind(resps => clientServiceResponses(ctx, resps))
  }

  def handleClientClosed(proxy: GatewayClientProxy): Unit = {
    val start = clientToRoutes.values
    clientToRoutes.removeAll(proxy)
    val end = clientToRoutes.values
    if (end != start) {
      eventDist.push(GatewayEvents(Some(end), Seq()))
    }
  }

  private def clientEvents(ctx: GatewayClientContext, routeUpdate: Option[Set[TypeValue]], events: Seq[StreamEvent]): Unit = {
    logger.trace(s"Client events $routeUpdate with $events")
    val setUpdate = routeUpdate.flatMap { routes =>
      logger.debug(s"Client route update: $routes")
      val gatewayRoutesBefore = clientToRoutes.values

      val proxyRoutesBefore = clientToRoutes.getFirst(ctx.proxy).getOrElse(Set())
      val subsBefore = proxyRoutesBefore.flatMap(r => subscriptions.getOrElse(r, Set()).map(_.toRowId(r)))

      routes.foreach(r => clientToRoutes.put(ctx.proxy, r))

      val proxyRoutesAfter = clientToRoutes.getFirst(ctx.proxy).getOrElse(Set())
      val subsAfter = proxyRoutesAfter.flatMap(r => subscriptions.getOrElse(r, Set()).map(_.toRowId(r)))
      if (subsBefore != subsAfter) {
        ctx.proxy.subscriptions.push(subsAfter)
      }

      val gatewayRoutesAfter = clientToRoutes.values

      if (gatewayRoutesAfter != gatewayRoutesBefore) {
        Some(gatewayRoutesAfter)
      } else {
        None
      }
    }

    val synthesizedEvents = synthesizer.handle(ctx.id, events)
    logger.trace("Synthesized gateway before:" + StreamLogging.logEvents(events))
    logger.trace("Synthesized gateway after:" + StreamLogging.logEvents(synthesizedEvents))

    eventDist.push(GatewayEvents(setUpdate, synthesizedEvents))
  }

  private def clientServiceResponses(ctx: GatewayClientContext, responses: Seq[ServiceResponse]): Unit = {
    respDist.push(responses)
  }

  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = {
    logger.debug(s"Rows for route $route updates: $rows")
    val existing = subscriptions.get(route)
    if (existing != rows) {
      subscriptions.put(route, rows)
      clientToRoutes.getSecond(route).foreach { proxy =>
        proxy.subscriptions.push(rows.map(_.toRowId(route)))
      }
    }
  }

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {
    requests.groupBy(_.row.routingKey).foreach {
      case (route, reqs) =>
        clientToRoutes.getSecond(route).foreach(p => p.requests.push(reqs))
    }
  }
}

trait GatewayRowSynthesizer {
  def append(event: AppendEvent): Seq[AppendEvent]
  def current: SequencedTypeValue
}

object GatewayRowSynthesizerImpl {

  def resequenceAppendEvent(appendEvent: AppendEvent, localSession: PeerSessionId, start: SequencedTypeValue): (AppendEvent, SequencedTypeValue) = {
    appendEvent match {
      case sd: StreamDelta => {
        val (delta, endSeq) = resequenceDelta(sd.update, start)
        (StreamDelta(delta), endSeq)
      }
      case rs: ResyncSnapshot => {
        val (snap, endSeq) = resequenceSnapshot(rs.resync, start)
        (ResyncSnapshot(snap), endSeq)
      }
      case rss: ResyncSession => {
        val (snap, endSeq) = resequenceSnapshot(rss.resync, start)
        (ResyncSession(localSession, rss.context, snap), endSeq)
      }
    }

  }

  /*private def resequenceAppendSeq(original: AppendSetSequence, startSequence: SequencedTypeValue): (AppendSetSequence, SequencedTypeValue) = {
    var seqVar = startSequence
    val values = original.appends.map { v =>
      val vseq = seqVar
      seqVar = seqVar.next
      AppendSetValue(vseq, v.value)
    }
    (AppendSetSequence(values), seqVar)
  }*/

  def resequenceStreamSnap(ss: SequenceSnapshot, startSequence: SequencedTypeValue): (SequenceSnapshot, SequencedTypeValue) = {
    ss match {
      case _: SetSnapshot => (ss, startSequence.next)
      case _: MapSnapshot => (ss, startSequence.next)
      case snap: AppendSnapshot => {

        var seqVar = startSequence
        val prevs = snap.previous.map { diff =>
          val vseq = seqVar
          seqVar = seqVar.next
          diff.copy(sequence = vseq)
        }

        val currentSeq = seqVar.next
        val current = snap.current.copy(sequence = currentSeq)
        (AppendSnapshot(current, prevs), currentSeq)
      }
    }
  }

  def resequenceSnapshot(r: Resync, startSequence: SequencedTypeValue): (Resync, SequencedTypeValue) = {
    val (streamSnap, seq) = resequenceStreamSnap(r.snapshot, startSequence)
    (r.copy(sequence = seq, snapshot = streamSnap), seq)
  }

  def resequenceDelta(delta: Delta, startSequence: SequencedTypeValue): (Delta, SequencedTypeValue) = {
    var seqVar = startSequence

    val diffs = delta.diffs.map { diff =>
      val vseq = seqVar
      seqVar = seqVar.next
      diff.copy(sequence = vseq)
    }

    (Delta(diffs), seqVar)
  }

  /*def resequenceSnapshot(snapshot: SetSnapshot, seq: SequencedTypeValue): (SetSnapshot, SequencedTypeValue) = {
    snapshot match {
      case d: ModifiedSetSnapshot => (d.copy(sequence = seq), seq.next)
      case d: ModifiedKeyedSetSnapshot => (d.copy(sequence = seq), seq.next)
      case d: AppendSetSequence => resequenceAppendSeq(d, seq)
    }
  }*/

  sealed trait State
  case object Uninit extends State
  case class Synced(filter: StreamFilter, ctx: SequenceCtx) extends State
}

/*

class GenInitializedStreamFilter(cid: String, startInit: ResyncSession) extends StreamFilter {

  private var session = startInit.sessionId
  //private var ctx = startInit.init

  private var seqFilter = new GenSequenceFilter(cid, startInit.resync.sequence)

  def handle(event: AppendEvent): Option[AppendEvent] = {
    event match {
      case StreamDelta(delta) => seqFilter.delta(delta).map(StreamDelta)
      case ResyncSnapshot(resync) => seqFilter.resync(resync).map(ResyncSnapshot)
      case resyncSession: ResyncSession => {
        if (resyncSession.sessionId != session) {
          session = resyncSession.sessionId
          seqFilter = new GenSequenceFilter(cid, resyncSession.resync.sequence)

          Some(resyncSession)
        } else {
          seqFilter.resync(resyncSession.resync).map(up => resyncSession.copy(resync = up))
        }
      }
    }
  }
}
 */

class GatewayRowSynthesizerImpl(row: RowId, peerSession: PeerSessionId, startSequence: SequencedTypeValue) extends GatewayRowSynthesizer with LazyLogging {
  import GatewayRowSynthesizerImpl._

  private var sessionSequence: SequencedTypeValue = startSequence
  //private var ctx = Option.empty[]
  //private var filterOpt = Option.empty[SequenceFilter]

  private var state: State = Uninit

  def current: SequencedTypeValue = sessionSequence

  def append(event: AppendEvent): Seq[AppendEvent] = {
    state match {
      case Uninit => {
        event match {
          case resync: ResyncSession => {
            handleAndReinitialize(resync)
            /*val filter = new GenInitializedStreamFilter(row.toString, resync)
            state = Synced(filter, resync.context)
            val (resequenced, updatedSeq) = resequenceAppendEvent(resync, peerSession, sessionSequence)
            sessionSequence = updatedSeq
            Seq(resequenced)*/
          }
          case _ =>
            logger.warn(s"Uinitialized gateway synthesizer saw stream delta instead of resync session: $row")
            Seq()
        }
      }
      case Synced(filter, ctx) => {
        event match {
          case ev: StreamDelta => handleEvent(ev)
          case ev: ResyncSnapshot => handleEvent(ev)
          case ev: ResyncSession => handleAndReinitialize(ev)
        }
      }
    }
  }

  private def handleEvent(event: AppendEvent): Seq[AppendEvent] = {
    val (resequenced, updatedSeq) = resequenceAppendEvent(event, peerSession, sessionSequence)
    sessionSequence = updatedSeq
    Seq(resequenced)
  }

  private def handleAndReinitialize(resync: ResyncSession): Seq[AppendEvent] = {
    val filter = new GenInitializedStreamFilter(row.toString, resync)
    state = Synced(filter, resync.context)
    handleEvent(resync)
  }

  /*def append(event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case delta: StreamDelta => {
        state match {
          case Uninit =>
            logger.warn(s"Uinitialized gateway synthesizer saw stream delta instead of resync session: $row")
            Seq()
          case Synced(filter, ctx) => {
            val deltaOpt = filter.delta(delta.update).map { filteredDelta =>
              val (resequenced, updatedSeq) = resequenceDelta(filteredDelta, sessionSequence)
              sessionSequence = updatedSeq
              StreamDelta(resequenced)
            }
            deltaOpt.map(Seq(_)).getOrElse(Seq())
          }
        }
      }
      case snap: ResyncSnapshot => {
        state match {
          case Uninit =>
            logger.warn(s"Uinitialized gateway synthesizer saw stream snapshot instead of resync session: $row")
            Seq()
          case Synced(filter, ctx) => {
            filter.resync(snap.resync).map { appendEvent =>
              val (resequenced, updatedSeq) = resequenceAppendEvent(ResyncSnapshot(appendEvent), peerSession, sessionSequence)
              sessionSequence = updatedSeq
              Seq(resequenced)
            }.getOrElse(Seq())
          }
        }
      }
      case snap: ResyncSession => {
        state match {
          case Uninit =>
            logger.warn(s"Uinitialized gateway synthesizer saw stream snapshot instead of resync session: $row")
            Seq()
          case Synced(filter, ctx) => {
            filter.resync(snap.resync).map { appendEvent =>
              val (resequenced, updatedSeq) = resequenceAppendEvent(ResyncSnapshot(appendEvent), peerSession, sessionSequence)
              sessionSequence = updatedSeq
              Seq(resequenced)
            }.getOrElse(Seq())
          }
        }
      }
    }
  }*/

  /*private def handleSnapshot(snapshot: Resync): Seq[AppendEvent] = {
    filterOpt match {
      case None => {

        val filter = new GenSequenceFilter(row.toString, snapshot.sequence)
        filterOpt = Some(filter)
        val (resequenced, updatedSeq) = resequenceSnapshot(snapshot, sessionSequence)
        sessionSequence = updatedSeq
        Seq(ResyncSession(peerSession, resequenced))
        /*val builtFilterOpt = SessionSynthesizingFilter.build(row, peerSession, snapshot)
        builtFilterOpt match {
          case None =>
            logger.warn(s"Uninitialized gateway synthesizer could not build filter from snapshot: $row")
            Seq()
          case Some(filter) =>
            filterOpt = Some(filter)
            val (resequenced, updatedSeq) = resequenceSnapshot(snapshot, sessionSequence)
            sessionSequence = updatedSeq
            Seq(ResyncSession(peerSession, resequenced))
        }*/
      }
      case Some(filter) => {
        filter.resync(snapshot).map { appendEvent =>
          val (resequenced, updatedSeq) = resequenceAppendEvent(appendEvent, peerSession, sessionSequence)
          sessionSequence = updatedSeq
          Seq(resequenced)
        }.getOrElse(Seq())
      }
    }
  }*/

  /*def append(event: AppendEvent): Seq[AppendEvent] = {
    event match {
      case delta: StreamDelta => {
        filterOpt match {
          case None =>
            logger.warn(s"Uinitialized gateway synthesizer saw stream delta instead of snapshot: $row")
            Seq()
          case Some(filter) => {
            val deltaOpt = filter.handleDelta(delta.update).map { filteredDelta =>
              val (resequenced, updatedSeq) = resequenceDelta(filteredDelta, sessionSequence)
              sessionSequence = updatedSeq
              StreamDelta(resequenced)
            }

            deltaOpt.map(Seq(_)).getOrElse(Seq())
          }
        }
      }
      case snap: ResyncSnapshot => {
        handleSnapshot(snap.resync)
      }
      case snap: ResyncSession => {
        handleSnapshot(snap.resync)
      }
    }
  }*/
}

import scala.collection.mutable

class GatewaySynthesizer[Source](localSession: PeerSessionId) extends LazyLogging {

  private val rowSynthesizers = mutable.Map.empty[TypeValue, mutable.Map[TableRow, (Source, GatewayRowSynthesizer)]]

  def handle(source: Source, events: Seq[StreamEvent]): Seq[StreamEvent] = {
    events.flatMap {
      case ev: RowAppendEvent =>
        val routeMap = rowSynthesizers.getOrElseUpdate(ev.rowId.routingKey, mutable.Map.empty[TableRow, (Source, GatewayRowSynthesizer)])
        routeMap.get(ev.rowId.tableRow) match {
          case None => {
            addRowSynthesizer(ev, Int64Val(0), source, routeMap)
          }
          case Some((rowSource, synthesizer)) =>
            if (rowSource == source) {
              synthesizer.append(ev.appendEvent).map { appendEvent => RowAppendEvent(ev.rowId, appendEvent) }
            } else {
              // Restart the filter with the same resequencing
              addRowSynthesizer(ev, synthesizer.current, source, routeMap)
            }
        }
      case other =>
        logger.warn("Gateway saw unexpected stream event: " + other)
        Seq()
    }
  }

  private def addRowSynthesizer(ev: RowAppendEvent, startSequence: SequencedTypeValue, source: Source, routeMap: mutable.Map[TableRow, (Source, GatewayRowSynthesizer)]): Seq[RowAppendEvent] = {
    logger.trace(s"Adding gateway row synthesizer: " + StreamLogging.logEvent(ev) + ", startSequence: " + startSequence)
    val synthesizer = new GatewayRowSynthesizerImpl(ev.rowId, localSession, startSequence)
    routeMap.put(ev.rowId.tableRow, (source, synthesizer))
    synthesizer.append(ev.appendEvent).map(append => RowAppendEvent(ev.rowId, append))
  }
}
