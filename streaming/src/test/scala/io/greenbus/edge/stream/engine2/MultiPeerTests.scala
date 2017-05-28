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
import io.greenbus.edge.stream._
import org.junit.runner.RunWith
import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

object PeerTestFramework extends Matchers with LazyLogging {

  class MockSubscriber {

  }

  object MockPeer {

    def subscribe(name: String, source: MockPeer, target: MockPeer): MockPeerSource = {
      new MockPeerSource(name, source, target)
    }
  }
  class MockPeer(val name: String, sessionOpt: Option[PeerSessionId] = None) {
    val gateway = new MockRouteSource(s"src-$name")
    val session: PeerSessionId = sessionOpt.getOrElse(Helpers.sessId)
    val engine = new StreamEngine(name, session, route => new SingleSubscribeSourcingStrategy(route))

    def routeRow: RowId = {
      PeerRouteSource.peerRouteRow(session)
    }

    def gatewayPublish(events: SourceEvents): Unit = {
      engine.sourceUpdate(gateway, events)
    }
    def gatewayPublish(routes: Option[Set[TypeValue]], events: Seq[StreamEvent]): Unit = {
      val manifestMapOpt = routes.map(set => set.map(r => r -> RouteManifestEntry(0)).toMap)
      engine.sourceUpdate(gateway, SourceEvents(manifestMapOpt, events))
    }

    def subscribe(target: TargetQueueMgr, rows: Set[RowId]): Unit = {
      val observers = target.subscriptionUpdate(rows)
      engine.targetSubscriptionUpdate(target, observers)
    }
  }
  class MockPeerSource(val name: String, val source: MockPeer, val target: MockPeer) {
    private val routeMap = mutable.Map.empty[TypeValue, Set[TableRow]]

    val sourcing = new MockRouteSource(s"src-$name")
    val peerEventProcessor = new PeerLinkEventProcessor(source.session, target.engine.sourceUpdate(sourcing, _))
    val targetQueue = new TargetQueueMgr

    private val routeManifestRow = PeerRouteSource.peerRouteRow(source.session)
    routeMap.update(routeManifestRow.routingKey, Set(routeManifestRow.tableRow))

    def sourceSession: PeerSessionId = source.session
    def manifestRow: RowId = PeerRouteSource.peerRouteRow(source.session)

    def pushSourceUpdates(): Unit = {
      sourcing.subUpdates.foreach {
        case (route, rows) =>
          routeMap.update(route, rows)
          val all = routeMap.flatMap {
            case (routingKey, routeRows) => routeRows.map(_.toRowId(routingKey))
          }
          source.subscribe(targetQueue, all.toSet)
      }
      sourcing.subUpdates.clear()
    }
    def checkAndPushSourceUpdates(f: Set[RowId] => Unit): Unit = {
      logger.debug(s"CHECKANDPUSH: ${sourcing.subUpdates}")
      sourcing.subUpdates.foreach {
        case (route, rows) =>
          routeMap.update(route, rows)
      }
      val all = routeMap.flatMap {
        case (routingKey, routeRows) => routeRows.map(_.toRowId(routingKey))
      }.toSet
      f(all)
      val observers = targetQueue.subscriptionUpdate(all)
      source.engine.targetSubscriptionUpdate(targetQueue, observers)
      sourcing.subUpdates.clear()
    }

    def checkAndPushEvents(f: Seq[StreamEvent] => Unit): Unit = {
      val events = targetQueue.dequeue()
      f(events)
      peerEventProcessor.handleStreamEvents(events)
    }

    override def toString: String = {
      s"MockPeerSource($name)"
    }
  }

  def checkAndPushEventBatches(sub: MockPeerSource, check: Seq[StreamEvent] => Unit): Unit = {
    sub.checkAndPushEvents(check)
  }

  def matchAndPushEventBatches(sub: MockPeerSource, events: Seq[StreamEvent]): Unit = {
    sub.checkAndPushEvents(e => e shouldEqual events)
  }

  def matchAndClearEventBatches(sub: TargetQueueMgr, events: Seq[StreamEvent]): Unit = {
    sub.dequeue() should equal(events)
  }

  def matchAndPushSourceUpdate(link: MockPeerSource, updates: Set[RowId]): Unit = {
    link.checkAndPushSourceUpdates(rowSet => rowSet shouldEqual updates)
    link.sourcing.subUpdates.clear()
  }
  def matchAndClearGatewayUpdate(source: MockRouteSource, updates: (TypeValue, Set[TableRow])*): Unit = {
    source.subUpdates shouldEqual updates
    source.subUpdates.clear()
  }

  def routeRow(session: PeerSessionId): RowId = {
    PeerRouteSource.peerRouteRow(session)
  }
  def manifestRowsForPeer(session: PeerSessionId): Set[RowId] = {
    Set(PeerRouteSource.peerRouteRow(session))
  }

  object MockData {
    def rowAppend(row: RowId, append: AppendEvent): RowAppendEvent = {
      RowAppendEvent(row, append)
    }
    def sessResyncSet(session: PeerSessionId, seq: Long, set: Set[Long]): ResyncSession = {
      ResyncSession(session, SequenceCtx.empty, Resync(Int64Val(seq), SetSnapshot(set.map(Int64Val))))
    }
    def sessResyncManifest(session: PeerSessionId, seq: Long, manifest: Map[TypeValue, Int]): ResyncSession = {
      ResyncSession(session, SequenceCtx.empty, Resync(Int64Val(seq), MapSnapshot(manifest.mapValues(dist => RouteManifestEntry.toTypeValue(RouteManifestEntry(dist))))))
    }

    def writeManifestTup(tup: (TypeValue, Int)): (TypeValue, TypeValue) = {
      (tup._1, Int64Val(tup._2))
    }

    def manifestUpdate(seq: Long, removes: Set[TypeValue], adds: Set[(TypeValue, Int)], modifies: Set[(TypeValue, Int)]): StreamDelta = {
      StreamDelta(Delta(Seq(SequencedDiff(Int64Val(seq), MapDiff(removes, adds.map(writeManifestTup), modifies.map(writeManifestTup))))))
    }
  }

  import MockData._

  trait Scenario {

    def peers: Seq[MockPeer]
    def links: Seq[MockPeerSource]
    def subs: Seq[TargetQueueMgr]

    def checkAllClear(): Unit = {
      checkLinksClear()
      checkGatewaysClear()
      checkSubsClear()
    }
    def checkLinkClear(l: MockPeerSource): Unit = {
      val events = l.targetQueue.dequeue()
      val subUpdates = l.sourcing.subUpdates
      if (events.nonEmpty) fail(s"${l.name} has event batches: " + events)
      if (subUpdates.nonEmpty) fail(s"${l.name} has subscription updates: " + subUpdates)
    }
    def checkLinksClear(): Unit = {
      links.foreach(checkLinkClear)
    }
    def checkGatewaysClear(): Unit = {
      peers.foreach { p =>
        if (p.gateway.subUpdates.nonEmpty) fail(s"${p.name} has gateway subscription updates: " + p.gateway.subUpdates)
      }
    }
    def checkSubClear(s: TargetQueueMgr): Unit = {
      val events = s.dequeue()
      if (events.nonEmpty) fail(s"? has event batches: " + events)
    }
    def checkSubsClear(): Unit = {
      subs.foreach(checkSubClear)
    }
  }

  /*
     src
      |
      A
    B   C
      D
      |
     sub
   */
  trait BaseFourPeers extends Scenario {

    val route1 = new SimpleRoute
    //val subQ = new SimpleMockSubscriber("subQ")
    val subQ = new TargetQueueMgr

    val peerA = new MockPeer("A")
    val peerB = new MockPeer("B")
    val peerC = new MockPeer("C")
    val peerD = new MockPeer("D")

    val peers = Seq(peerA, peerB, peerC, peerD)
    val subs = Seq(subQ)
  }

  class ConnectedFourPeers extends BaseFourPeers {

    val aToB = MockPeer.subscribe("AtoB", peerA, peerB)
    val aToC = MockPeer.subscribe("AtoC", peerA, peerC)
    val bToD = MockPeer.subscribe("BtoD", peerB, peerD)
    val cToD = MockPeer.subscribe("CtoD", peerC, peerD)

    val links = Seq(aToB, aToC, bToD, cToD)

    initialLinkExchanges(links)

    def initialLinkExchanges(links: Seq[MockPeerSource]): Unit = {

      // Manifest subscriptions
      links.foreach(l => matchAndPushSourceUpdate(l, manifestRowsForPeer(l.sourceSession)))

      // Manifest updates
      links.foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.manifestRow, sessResyncManifest(l.sourceSession, 0, Map()))))
      }
    }

    def attachRoute1ToAAndPropagate(): Unit = {
      peerA.gatewayPublish(Some(Set(route1.route)), Seq())

      Seq(aToB, aToC).foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.manifestRow, manifestUpdate(1, Set(), Set((route1.route, 0)), Set()))))
      }

      Seq(bToD, cToD).foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.manifestRow, manifestUpdate(1, Set(), Set((route1.route, 1)), Set()))))
      }
    }

    def transferPipe(p1: MockPeerSource, p2: MockPeerSource, all: Set[MockPeerSource], batch: Seq[RowAppendEvent]): Unit = {

      peerA.gatewayPublish(None, batch)

      (all - p1).foreach(checkLinkClear)
      checkGatewaysClear()
      matchAndPushEventBatches(p1, batch)

      (all - p2).foreach(checkLinkClear)
      checkGatewaysClear()
      matchAndPushEventBatches(p2, batch)

      checkGatewaysClear()
      checkLinksClear()
      matchAndClearEventBatches(subQ, batch)
    }

    def transferAtoBtoD(batch: Seq[RowAppendEvent]): Unit = {
      peerA.gatewayPublish(None, batch)

      Seq(aToC, bToD, cToD).foreach(checkLinkClear)
      checkGatewaysClear()
      matchAndPushEventBatches(aToB, batch)

      Seq(aToB, aToC, cToD).foreach(checkLinkClear)
      checkGatewaysClear()
      matchAndPushEventBatches(bToD, batch)

      checkGatewaysClear()
      checkLinksClear()
      matchAndClearEventBatches(subQ, batch)
    }
  }

}

@RunWith(classOf[JUnitRunner])
class MultiPeerTests extends FunSuite with Matchers with LazyLogging {

  import PeerTestFramework._

  def simpleTwoBatchPropagate(s: ConnectedFourPeers): Unit = {
    import s._

    // Which one is chosen is unstable
    val (p1, p2) = if (bToD.sourcing.subUpdates.nonEmpty) {
      cToD.sourcing.subUpdates.isEmpty shouldEqual true

      matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))
      matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))

      (aToB, bToD)

    } else {
      bToD.sourcing.subUpdates.isEmpty shouldEqual true

      matchAndPushSourceUpdate(cToD, route1.rowSet ++ Set(cToD.source.routeRow))
      matchAndPushSourceUpdate(aToC, route1.rowSet ++ Set(aToC.source.routeRow))

      (aToC, cToD)
    }

    checkLinksClear()
    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    transferPipe(p1, p2, links.toSet, route1.firstBatch(peerA.session))
    checkAllClear()

    transferPipe(p1, p2, links.toSet, route1.secondBatch())
    checkAllClear()
  }

  test("four peer scenario, subscribe before source arrives") {

    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    peerD.subscribe(subQ, route1.rowSet)
    matchAndClearEventBatches(subQ, Seq(RouteUnresolved(route1.route)))

    checkAllClear()

    attachRoute1ToAAndPropagate()

    simpleTwoBatchPropagate(s)
  }

  test("four peer scenario, source arrives then subscribe") {

    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    attachRoute1ToAAndPropagate()

    checkAllClear()

    // now subscribe
    peerD.subscribe(subQ, route1.rowSet)

    simpleTwoBatchPropagate(s)
  }

  test("four peer scenario, nodes killed, reconfigure before second batch") {

    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    attachRoute1ToAAndPropagate()

    checkAllClear()

    // Now subscribe
    peerD.subscribe(subQ, route1.rowSet)

    val (p1x, p2x, p1y, p2y) = if (bToD.sourcing.subUpdates.nonEmpty) {
      cToD.sourcing.subUpdates.isEmpty shouldEqual true

      matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))
      matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))

      (aToB, bToD, aToC, cToD)

    } else {
      bToD.sourcing.subUpdates.isEmpty shouldEqual true

      matchAndPushSourceUpdate(cToD, route1.rowSet ++ Set(cToD.source.routeRow))
      matchAndPushSourceUpdate(aToC, route1.rowSet ++ Set(aToC.source.routeRow))

      (aToC, cToD, aToB, bToD)
    }

    checkLinksClear()
    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    val firstBatch = route1.firstBatch(peerA.session)

    transferPipe(p1x, p2x, links.toSet, route1.firstBatch(peerA.session))
    checkAllClear()

    // Kill B
    logger.info("Removing B")
    peerA.engine.targetRemoved(p1x.targetQueue)
    matchAndClearGatewayUpdate(peerA.gateway, (route1.route, Set()))
    checkAllClear()
    peerD.engine.sourceRemoved(p2x.sourcing)

    // Reorganization: D subscribes to C, C subscribes to A
    matchAndPushSourceUpdate(p2y, route1.rowSet ++ Set(p2y.source.routeRow))
    matchAndPushSourceUpdate(p1y, route1.rowSet ++ Set(p1y.source.routeRow))

    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    // This should be filtered by the synthesizer, so below in aToC we have one batch caused by source update above
    peerA.gatewayPublish(None, firstBatch)

    // Resync: C must get resynced, then forwards to D who squelches it before it goes to the subscriber, since it's not new
    matchAndPushEventBatches(p1y, firstBatch)
    matchAndPushEventBatches(p2y, firstBatch)

    checkAllClear()

    val all = Set(p1x, p2x, p1y, p2y)
    // Transfer A to C to D
    val batch = route1.secondBatch()
    peerA.gatewayPublish(None, batch)

    (all - p1y).foreach(checkLinkClear)
    checkGatewaysClear()
    matchAndPushEventBatches(p1y, batch)

    (all - p2y).foreach(checkLinkClear)
    checkGatewaysClear()
    matchAndPushEventBatches(p2y, batch)

    checkGatewaysClear()
    checkLinksClear()
    matchAndClearEventBatches(subQ, batch)

    // Kill C
    logger.info("Removing C")
    peerA.engine.targetRemoved(p1y.targetQueue)
    matchAndClearGatewayUpdate(peerA.gateway, (route1.route, Set()))
    checkAllClear()
    peerD.engine.sourceRemoved(p2y.sourcing)

    matchAndClearEventBatches(subQ, Seq(RouteUnresolved(route1.route)))
  }

  test("four peer scenario, middle nodes have connectivity to A cut") {
    import MockData._
    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    attachRoute1ToAAndPropagate()

    checkAllClear()

    // Now subscribe
    peerD.subscribe(subQ, route1.rowSet)

    val (p1x, p2x, p1y, p2y, midPeerX, midPeerY) = if (bToD.sourcing.subUpdates.nonEmpty) {
      cToD.sourcing.subUpdates.isEmpty shouldEqual true
      matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))
      matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))
      (aToB, bToD, aToC, cToD, peerB, peerC)
    } else {
      bToD.sourcing.subUpdates.isEmpty shouldEqual true
      matchAndPushSourceUpdate(cToD, route1.rowSet ++ Set(cToD.source.routeRow))
      matchAndPushSourceUpdate(aToC, route1.rowSet ++ Set(aToC.source.routeRow))
      (aToC, cToD, aToB, bToD, peerC, peerB)
    }

    checkLinksClear()
    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    val firstBatch = route1.firstBatch(peerA.session)
    transferPipe(p1x, p2x, links.toSet, route1.firstBatch(peerA.session))
    checkAllClear()

    // Disconnect A <-> B
    logger.info("Removing B")
    peerA.engine.targetRemoved(p1x.targetQueue)
    matchAndClearGatewayUpdate(peerA.gateway, (route1.route, Set()))
    checkAllClear()
    midPeerX.engine.sourceRemoved(p1x.sourcing)

    checkAndPushEventBatches(p2x, events => events.toSet shouldEqual Set(rowAppend(p2x.source.routeRow, manifestUpdate(2, Set(route1.route), Set(), Set())), RouteUnresolved(route1.route)))

    checkSubsClear()

    // Reorganization: D subscribes to C, C subscribes to A
    matchAndPushSourceUpdate(p2y, route1.rowSet ++ Set(p2y.source.routeRow))
    matchAndPushSourceUpdate(p1y, route1.rowSet ++ Set(p1y.source.routeRow))

    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    // This should be filtered by the synthesizer, so below in aToC we have one batch caused by source update above
    peerA.gatewayPublish(None, firstBatch)

    // Resync: C must get resynced, then forwards to D who squelches it before it goes to the subscriber, since it's not new
    matchAndPushEventBatches(p1y, firstBatch)
    matchAndPushEventBatches(p2y, firstBatch)

    checkAllClear()

    val all = Set(p1x, p2x, p1y, p2y)
    // Transfer A to C to D
    val batch = route1.secondBatch()
    peerA.gatewayPublish(None, batch)

    (all - p1y).foreach(checkLinkClear)
    checkGatewaysClear()
    matchAndPushEventBatches(p1y, batch)

    (all - p2y).foreach(checkLinkClear)
    checkGatewaysClear()
    matchAndPushEventBatches(p2y, batch)

    checkGatewaysClear()
    checkLinksClear()
    matchAndClearEventBatches(subQ, batch)

    // Disconnect A <-> C
    logger.info("Removing C")
    peerA.engine.targetRemoved(p1y.targetQueue)
    matchAndClearGatewayUpdate(peerA.gateway, (route1.route, Set()))
    checkAllClear()
    midPeerY.engine.sourceRemoved(p1y.sourcing)

    checkAndPushEventBatches(p2y, events => events.toSet shouldEqual Set(rowAppend(p2y.source.routeRow, manifestUpdate(2, Set(route1.route), Set(), Set())), RouteUnresolved(route1.route)))

    matchAndClearEventBatches(subQ, Seq(RouteUnresolved(route1.route)))
  }

}
