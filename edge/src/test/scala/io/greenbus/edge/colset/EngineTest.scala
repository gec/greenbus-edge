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
import io.greenbus.edge.flow.Sink
import org.junit.runner.RunWith
import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

object Helpers {
  def sessId: PeerSessionId = PeerSessionId(UUID.randomUUID(), 0)
}

class MockGateway extends LocalGateway with LazyLogging {
  val subUpdates: mutable.Queue[(TypeValue, Set[TableRow])] = mutable.Queue.empty[(TypeValue, Set[TableRow])]

  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = {
    logger.debug("gateway got push: " + rows + ", prev: " + subUpdates)
    //(new Throwable()).printStackTrace()
    subUpdates += ((route, rows))
  }

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {}
}

trait MockSource extends PeerLink with LazyLogging {
  val name: String

  val subUpdates: mutable.Queue[Set[RowId]] = mutable.Queue.empty[Set[RowId]]

  def setSubscriptions(rows: Set[RowId]): Unit = {
    logger.debug(s"$name source got push: " + rows + ", prev: " + subUpdates)
    subUpdates += rows
  }
}

trait MockSubscriber extends SubscriptionTarget with LazyLogging {
  val name: String

  val batches: mutable.Queue[Seq[StreamEvent]] = mutable.Queue.empty[Seq[StreamEvent]]

  def handleBatch(events: Seq[StreamEvent]): Unit = {
    logger.debug(s"$name subscriber got events: $events")
    batches += events
  }
}

class SimpleMockSubscriber(val name: String) extends MockSubscriber

class MockPeerSource(val name: String, val source: MockPeer, val target: MockPeer) extends MockSubscriber with PeerLink with MockSource {

  def pushSubs(): Unit = {
    subUpdates.foreach { rowSet =>
      source.engine.subscriptionsRegistered(this, StreamSubscriptionParams(rowSet.toVector))
    }
    subUpdates.clear()
  }

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {}
}

object MockPeer {

  def subscribe(name: String, source: MockPeer, target: MockPeer): MockPeerSource = {
    val mps = new MockPeerSource(name, source, target)
    target.engine.peerSourceConnected(source.session, mps)
    mps
  }
}
class MockPeer(val name: String, sessionOpt: Option[PeerSessionId] = None) {
  val gateway = new MockGateway
  val session: PeerSessionId = sessionOpt.getOrElse(Helpers.sessId)
  val engine = new PeerStreamEngine(name, session, gateway)

  def routeRow: RowId = {
    PeerRouteSource.peerRouteRow(session)
  }
}

class SimpleRoute {
  val route = UuidVal(UUID.randomUUID())
  val row1 = RowId(route, SymbolVal("testTable"), SymbolVal("row1"))
  val row2 = RowId(route, SymbolVal("testTable"), SymbolVal("row2"))
  val row3 = RowId(route, SymbolVal("testTable"), SymbolVal("row3"))
  val rows = Seq(row1, row2, row3)
  val rowSet = rows.toSet
  val tableRowsSet = rows.map(_.tableRow).toSet

  def routeToTableRows: (TypeValue, Set[TableRow]) = route -> tableRowsSet

  def firstBatch(session: PeerSessionId, n: Int = 0): Seq[RowAppendEvent] = {
    Seq(
      RowAppendEvent(row1, ResyncSession(session, ModifiedSetSnapshot(Int64Val(0), Set(Int64Val(5 + n), Int64Val(3 + n))))),
      RowAppendEvent(row2, ResyncSession(session, ModifiedKeyedSetSnapshot(Int64Val(0), Map(Int64Val(3 + n) -> Int64Val(9 + n))))),
      RowAppendEvent(row3, ResyncSession(session, AppendSetSequence(Seq(AppendSetValue(Int64Val(0), Int64Val(66 + n)))))))
  }

  def secondBatch(n: Int = 0): Seq[RowAppendEvent] = {
    Seq(
      RowAppendEvent(row1, StreamDelta(ModifiedSetDelta(Int64Val(1), Set(Int64Val(5 + n)), Set(Int64Val(7 + n))))),
      RowAppendEvent(row2, StreamDelta(ModifiedKeyedSetDelta(Int64Val(1), Set(), Set(Int64Val(4) -> Int64Val(55 + n)), Set(Int64Val(3 + n) -> Int64Val(8 + n))))),
      RowAppendEvent(row3, StreamDelta(AppendSetSequence(Seq(AppendSetValue(Int64Val(1), Int64Val(77 + n)))))))
  }
}

@RunWith(classOf[JUnitRunner])
class EngineTest extends FunSuite with Matchers with LazyLogging {

  import Helpers._

  test("subscribe then local gateway publishes") {

    val route1 = new SimpleRoute

    val gateway = new MockGateway
    val subQ = new SimpleMockSubscriber("subQ")

    val sessA = sessId

    val engine = new PeerStreamEngine("peer", sessA, gateway)

    engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1.rows))

    engine.localGatewayEvents(Some(Set(route1.route)), Seq())

    {
      gateway.subUpdates.size should equal(1)

      val gateSub = gateway.subUpdates.dequeue()
      gateSub shouldEqual ((route1.route, route1.tableRowsSet))
    }

    {
      subQ.batches.size should equal(1)
      val batch = subQ.batches.dequeue()
      batch shouldEqual Seq(RouteUnresolved(route1.route))
    }

    val firstRetailBatch = Seq(
      RowAppendEvent(route1.row1, ResyncSession(sessA, ModifiedSetSnapshot(Int64Val(0), Set(Int64Val(5), Int64Val(3))))),
      RowAppendEvent(route1.row2, ResyncSession(sessA, ModifiedKeyedSetSnapshot(Int64Val(0), Map(Int64Val(3) -> Int64Val(9))))),
      RowAppendEvent(route1.row3, ResyncSession(sessA, AppendSetSequence(Seq(AppendSetValue(Int64Val(0), Int64Val(66)))))))

    engine.localGatewayEvents(None, firstRetailBatch)

    {
      subQ.batches.size should equal(1)

      val batch = subQ.batches.dequeue()
      batch shouldEqual firstRetailBatch
    }

    val secondRetailBatch = Seq(
      RowAppendEvent(route1.row1, StreamDelta(ModifiedSetDelta(Int64Val(1), Set(Int64Val(5)), Set(Int64Val(7))))),
      RowAppendEvent(route1.row2, StreamDelta(ModifiedKeyedSetDelta(Int64Val(1), Set(), Set(Int64Val(4) -> Int64Val(55)), Set(Int64Val(3) -> Int64Val(8))))),
      RowAppendEvent(route1.row3, StreamDelta(AppendSetSequence(Seq(AppendSetValue(Int64Val(1), Int64Val(77)))))))

    engine.localGatewayEvents(None, secondRetailBatch)

    {
      subQ.batches.size should equal(1)

      val batch = subQ.batches.dequeue()
      batch shouldEqual secondRetailBatch
    }

    engine.localGatewayEvents(Some(Set()), Seq())

    {
      subQ.batches.size should equal(1)

      val batch = subQ.batches.dequeue()
      batch shouldEqual Seq(RouteUnresolved(route1.route))
    }
  }

  def matchEventBatches(sub: MockSubscriber, batches: Seq[StreamEvent]*): Unit = {
    sub.batches.toSet shouldEqual batches.toSet
  }

  def matchAndPushEventBatches(sub: MockPeerSource, batches: Seq[StreamEvent]*): Unit = {
    matchEventBatches(sub, batches: _*)
    sub.batches.foreach(batch => sub.target.engine.peerSourceEvents(sub, batch))
    sub.batches.clear()
  }

  def matchAndClearEventBatches(sub: MockSubscriber, batches: Seq[StreamEvent]*): Unit = {
    sub.batches shouldEqual batches
    sub.batches.clear()
  }

  def matchSourceUpdate(source: MockSource, updates: Set[RowId]*): Unit = {
    source.subUpdates shouldEqual updates
  }
  def matchAndClearSourceUpdate(source: MockSource, updates: Set[RowId]*): Unit = {
    matchSourceUpdate(source, updates: _*)
    source.subUpdates.clear()
  }
  def matchAndPushSourceUpdate(link: MockPeerSource, updates: Set[RowId]*): Unit = {
    matchSourceUpdate(link, updates: _*)
    link.subUpdates.foreach(up => link.source.engine.subscriptionsRegistered(link, StreamSubscriptionParams(up.toSeq)))
    link.subUpdates.clear()
  }

  def matchGatewayUpdate(source: MockGateway, updates: (TypeValue, Set[TableRow])*): Unit = {
    source.subUpdates shouldEqual updates
  }
  def matchAndClearGatewayUpdate(source: MockGateway, updates: (TypeValue, Set[TableRow])*): Unit = {
    matchGatewayUpdate(source, updates: _*)
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
      ResyncSession(session, ModifiedSetSnapshot(Int64Val(seq), set.map(Int64Val)))
    }
    def sessResyncManifest(session: PeerSessionId, seq: Long, manifest: Map[TypeValue, Int]): ResyncSession = {
      ResyncSession(session, ModifiedKeyedSetSnapshot(Int64Val(seq), manifest.mapValues(dist => RouteManifestEntry.toTypeValue(RouteManifestEntry(dist)))))
    }

    def writeManifestTup(tup: (TypeValue, Int)): (TypeValue, TypeValue) = {
      (tup._1, Int64Val(tup._2))
    }

    def manifestUpdate(seq: Long, removes: Set[TypeValue], adds: Set[(TypeValue, Int)], modifies: Set[(TypeValue, Int)]): StreamDelta = {
      StreamDelta(ModifiedKeyedSetDelta(Int64Val(seq), removes, adds.map(writeManifestTup), modifies.map(writeManifestTup)))
    }
  }

  import MockData._

  trait Scenario {

    def peers: Seq[MockPeer]
    def links: Seq[MockPeerSource]
    def subs: Seq[SimpleMockSubscriber]

    def checkAllClear(): Unit = {
      checkLinksClear()
      checkGatewaysClear()
      checkSubsClear()
    }
    def checkLinkClear(l: MockPeerSource): Unit = {
      if (l.batches.nonEmpty) fail(s"${l.name} has event batches: " + l.batches)
      if (l.subUpdates.nonEmpty) fail(s"${l.name} has subscription updates: " + l.subUpdates)
    }
    def checkLinksClear(): Unit = {
      links.foreach(checkLinkClear)
    }
    def checkGatewaysClear(): Unit = {
      peers.foreach { p =>
        if (p.gateway.subUpdates.nonEmpty) fail(s"${p.name} has gateway subscription updates: " + p.gateway.subUpdates)
      }
    }
    def checkSubClear(s: SimpleMockSubscriber): Unit = {
      if (s.batches.nonEmpty) fail(s"${s.name} has event batches: " + s.batches)
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
    val subQ = new SimpleMockSubscriber("subQ")

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
      links.foreach(l => matchAndPushSourceUpdate(l, manifestRowsForPeer(l.source.session)))

      // Manifest updates
      links.foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, sessResyncManifest(l.source.session, 0, Map()))))
      }
    }

    def attachRoute1ToAAndPropagate(): Unit = {
      peerA.engine.localGatewayEvents(Some(Set(route1.route)), Seq())

      Seq(aToB, aToC).foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 0)), Set()))))
      }

      Seq(bToD, cToD).foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 1)), Set()))))
      }
    }

    def transferAtoBtoD(batch: Seq[RowAppendEvent]): Unit = {
      peerA.engine.localGatewayEvents(None, batch)

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

  test("four peer scenario, subscribe before source arrives") {

    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    peerD.engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1.rows))
    matchAndClearEventBatches(subQ, Seq(RouteUnresolved(route1.route)))

    checkAllClear()

    attachRoute1ToAAndPropagate()

    matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))
    matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))

    checkLinksClear()
    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    transferAtoBtoD(route1.firstBatch(peerA.session))
    checkAllClear()

    transferAtoBtoD(route1.secondBatch())
    checkAllClear()
  }

  test("four peer scenario, source arrives then subscribe") {

    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    attachRoute1ToAAndPropagate()

    checkAllClear()

    // now subscribe
    peerD.engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1.rows))

    matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))
    matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))

    checkLinksClear()
    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    transferAtoBtoD(route1.firstBatch(peerA.session))
    checkAllClear()

    transferAtoBtoD(route1.secondBatch())
    checkAllClear()
  }

  test("four peer scenario, nodes killed, reconfigure before second batch") {

    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    attachRoute1ToAAndPropagate()

    checkAllClear()

    // Now subscribe
    peerD.engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1.rows))

    matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))
    matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))

    checkLinksClear()
    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    val firstBatch = route1.firstBatch(peerA.session)
    transferAtoBtoD(firstBatch)
    checkAllClear()

    // Kill B
    logger.info("Removing B")
    peerA.engine.subscriberRemoved(aToB)
    matchAndClearGatewayUpdate(peerA.gateway, (route1.route, Set()))
    checkAllClear()
    peerD.engine.sourceDisconnected(bToD)

    // Reorganization: D subscribes to C, C subscribes to A
    matchAndPushSourceUpdate(cToD, route1.rowSet ++ Set(cToD.source.routeRow))
    matchAndPushSourceUpdate(aToC, route1.rowSet ++ Set(aToC.source.routeRow))

    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    // This should be filtered by the synthesizer, so below in aToC we have one batch caused by source update above
    peerA.engine.localGatewayEvents(None, firstBatch)

    // Resync: C must get resynced, then forwards to D who squelches it before it goes to the subscriber, since it's not new
    matchAndPushEventBatches(aToC, firstBatch)
    matchAndPushEventBatches(cToD, firstBatch)

    checkAllClear()

    // Transfer A to C to D
    val batch = route1.secondBatch()
    peerA.engine.localGatewayEvents(None, batch)

    Seq(aToB, bToD, cToD).foreach(checkLinkClear)
    checkGatewaysClear()
    matchAndPushEventBatches(aToC, batch)

    Seq(aToB, aToC, bToD).foreach(checkLinkClear)
    checkGatewaysClear()
    matchAndPushEventBatches(cToD, batch)

    checkGatewaysClear()
    checkLinksClear()
    matchAndClearEventBatches(subQ, batch)

    // Kill C
    logger.info("Removing C")
    peerA.engine.subscriberRemoved(aToC)
    matchAndClearGatewayUpdate(peerA.gateway, (route1.route, Set()))
    checkAllClear()
    peerD.engine.sourceDisconnected(cToD)

    matchAndClearEventBatches(subQ, Seq(RouteUnresolved(route1.route)))
  }

  test("four peer scenario, middle nodes have connectivity to A cut") {

    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    attachRoute1ToAAndPropagate()

    checkAllClear()

    // Now subscribe
    peerD.engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1.rows))

    matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))
    matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))

    checkLinksClear()
    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    val firstBatch = route1.firstBatch(peerA.session)
    transferAtoBtoD(firstBatch)
    checkAllClear()

    // Disconnect A <-> B
    logger.info("Removing B")
    peerA.engine.subscriberRemoved(aToB)
    matchAndClearGatewayUpdate(peerA.gateway, (route1.route, Set()))
    checkAllClear()
    peerB.engine.sourceDisconnected(aToB)

    matchAndPushEventBatches(bToD, Seq(RouteUnresolved(route1.route)), Seq(rowAppend(bToD.source.routeRow, manifestUpdate(2, Set(route1.route), Set(), Set()))))

    checkSubsClear()

    // Reorganization: D subscribes to C, C subscribes to A
    matchAndPushSourceUpdate(cToD, route1.rowSet ++ Set(cToD.source.routeRow))
    matchAndPushSourceUpdate(aToC, route1.rowSet ++ Set(aToC.source.routeRow))

    matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

    // This should be filtered by the synthesizer, so below in aToC we have one batch caused by source update above
    peerA.engine.localGatewayEvents(None, firstBatch)

    // Resync: C must get resynced, then forwards to D who squelches it before it goes to the subscriber, since it's not new
    matchAndPushEventBatches(aToC, firstBatch)
    matchAndPushEventBatches(cToD, firstBatch)

    checkAllClear()

    // Transfer A to C to D
    val batch = route1.secondBatch()
    peerA.engine.localGatewayEvents(None, batch)

    Seq(aToB, bToD, cToD).foreach(checkLinkClear)
    checkGatewaysClear()
    matchAndPushEventBatches(aToC, batch)

    Seq(aToB, aToC, bToD).foreach(checkLinkClear)
    checkGatewaysClear()
    matchAndPushEventBatches(cToD, batch)

    checkGatewaysClear()
    checkLinksClear()
    matchAndClearEventBatches(subQ, batch)

    // Disconnect A <-> C
    logger.info("Removing C")
    peerA.engine.subscriberRemoved(aToC)
    matchAndClearGatewayUpdate(peerA.gateway, (route1.route, Set()))
    checkAllClear()
    peerC.engine.sourceDisconnected(aToC)

    matchAndPushEventBatches(cToD, Seq(RouteUnresolved(route1.route)), Seq(rowAppend(cToD.source.routeRow, manifestUpdate(2, Set(route1.route), Set(), Set()))))

    matchAndClearEventBatches(subQ, Seq(RouteUnresolved(route1.route)))
  }
  /*
  class ReplaceableAFourPeers extends Scenario {

    val route1 = new SimpleRoute
    val subQ = new SimpleMockSubscriber("subQ")

    //val peerA = new MockPeer("A")
    val peerB = new MockPeer("B")
    val peerC = new MockPeer("C")
    val peerD = new MockPeer("D")

    val bToD = MockPeer.subscribe("BtoD", peerB, peerD)
    val cToD = MockPeer.subscribe("CtoD", peerC, peerD)

    val peers = Seq(peerB, peerC, peerD)
    val links = Seq(bToD, cToD)
    val subs = Seq(subQ)

    // Manifest subscriptions
    links.foreach(l => matchAndPushSourceUpdate(l, manifestRowsForPeer(l.source.session)))

    // Manifest updates
    links.foreach { l =>
      matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, sessResyncManifest(l.source.session, 0, Map()))))
    }
  }*/

  /*abstract class SubScenario(base: Scenario) extends Scenario {
    protected var subPeers = Vector.empty[MockPeer]
    protected var subLinks = Vector.empty[MockPeerSource]
    protected var subSubs = Vector.empty[SimpleMockSubscriber]

    protected def peer(name: String, sessionId: Option[PeerSessionId] = None): MockPeer = {
      val p = new MockPeer(name, sessionId)
      subPeers :+= p
      p
    }
    protected def subscribe(name: String, source: MockPeer, target: MockPeer): MockPeerSource = {
      val l = MockPeer.subscribe("BtoD", source, target)
      subLinks :+= l
      l
    }

    def peers: Seq[MockPeer] = subPeers ++ base.peers

    def links: Seq[MockPeerSource] = subLinks ++ base.links

    def subs: Seq[SimpleMockSubscriber] = subSubs ++ base.subs

    def pushLinking(): Unit = {
      subLinks.foreach(l => matchAndPushSourceUpdate(l, manifestRowsForPeer(l.source.session)))

      subLinks.foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, sessResyncManifest(l.source.session, 0, Map()))))
      }
    }
  }


  class SubScenario(base: Scenario) extends Scenario {
    protected var subPeers = Vector.empty[MockPeer]
    protected var subLinks = Vector.empty[MockPeerSource]
    protected var subSubs = Vector.empty[SimpleMockSubscriber]

    def peer(name: String, sessionId: Option[PeerSessionId] = None): MockPeer = {
      val p = new MockPeer(name, sessionId)
      subPeers :+= p
      p
    }
    def subscribe(name: String, source: MockPeer, target: MockPeer): MockPeerSource = {
      val l = MockPeer.subscribe("BtoD", source, target)
      subLinks :+= l
      l
    }

    def peers: Seq[MockPeer] = subPeers ++ base.peers

    def links: Seq[MockPeerSource] = subLinks ++ base.links

    def subs: Seq[SimpleMockSubscriber] = subSubs ++ base.subs

  }
  */

  class ReplaceableFourPeers extends ConnectedFourPeers {

    protected var subPeers = Vector.empty[MockPeer]
    protected var subLinks = Vector.empty[MockPeerSource]
    protected var subSubs = Vector.empty[SimpleMockSubscriber]

    def peer(name: String, sessionId: Option[PeerSessionId] = None): MockPeer = {
      val p = new MockPeer(name, sessionId)
      subPeers :+= p
      p
    }
    def subscribe(name: String, source: MockPeer, target: MockPeer): MockPeerSource = {
      val l = MockPeer.subscribe(name, source, target)
      subLinks :+= l
      l
    }

    def transferAtoBtoDtoSub(peerA: MockPeer, aToB: MockPeerSource, aToC: MockPeerSource, batch: Seq[RowAppendEvent]): Unit = {
      peerA.engine.localGatewayEvents(None, batch)

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

    def transferAtoBtoDOnly(peerA: MockPeer, aToB: MockPeerSource, aToC: MockPeerSource, batch: Seq[RowAppendEvent]): Unit = {
      peerA.engine.localGatewayEvents(None, batch)

      Seq(aToC, bToD, cToD).foreach(checkLinkClear)
      checkGatewaysClear()
      matchAndPushEventBatches(aToB, batch)

      Seq(aToB, aToC, cToD).foreach(checkLinkClear)
      checkGatewaysClear()
      matchAndPushEventBatches(bToD, batch)

      checkAllClear()
    }

    override def checkLinksClear(): Unit = (this.links ++ subLinks).foreach(checkLinkClear)

    def twoBatchStartingSequence(): Unit = {

      // Attach route
      peerA.engine.localGatewayEvents(Some(Set(route1.route)), Seq())

      Seq(aToB, aToC).foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 0)), Set()))))
      }

      Seq(bToD, cToD).foreach { l =>
        matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 1)), Set()))))
      }

      checkAllClear()

      // Now subscribe
      peerD.engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1.rows))

      matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))
      matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))

      checkLinksClear()
      matchAndClearGatewayUpdate(peerA.gateway, route1.routeToTableRows)

      transferAtoBtoDtoSub(peerA, aToB, aToC, route1.firstBatch(peerA.session, n = 1))
      transferAtoBtoDtoSub(peerA, aToB, aToC, route1.secondBatch(n = 1))
    }
  }

  test("A reboots, sessions are synthesized after timeout") {

    val s = new ReplaceableFourPeers
    import s._

    twoBatchStartingSequence()

    // PeerA1 is dead but not disconnected (yet)
    val peerA2 = peer("A2")
    val a2ToB = subscribe("A2toB", peerA2, peerB)
    matchAndPushSourceUpdate(a2ToB, manifestRowsForPeer(a2ToB.source.session))
    matchAndPushEventBatches(a2ToB, Seq(rowAppend(a2ToB.source.routeRow, sessResyncManifest(a2ToB.source.session, 0, Map()))))

    peerA2.engine.localGatewayEvents(Some(Set(route1.route)), Seq())

    matchAndPushEventBatches(a2ToB, Seq(rowAppend(a2ToB.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 0)), Set()))))

    checkAllClear()

    peerB.engine.sourceDisconnected(aToB)

    matchAndPushSourceUpdate(a2ToB, route1.rowSet ++ Set(a2ToB.source.routeRow))
    matchAndClearGatewayUpdate(peerA2.gateway, route1.routeToTableRows)

    checkAllClear()

    val a2batch = route1.firstBatch(peerA2.session, n = 2)
    transferAtoBtoDtoSub(peerA2, a2ToB, aToC, a2batch)

    checkAllClear()
  }

  /*test("A reboots, session is replaced due to ordering") {

    val s = new ReplaceableFourPeers
    import s._

    twoBatchStartingSequence()

    // PeerA1 is dead but not disconnected (yet)
    val peerA2 = peer("A2", sessionId = Some(peerA.session.copy(instanceId = 1)))
    val a2ToB = subscribe("A2toB", peerA2, peerB)
    matchAndPushSourceUpdate(a2ToB, manifestRowsForPeer(a2ToB.source.session))
    matchAndPushEventBatches(a2ToB, Seq(rowAppend(a2ToB.source.routeRow, sessResyncManifest(a2ToB.source.session, 0, Map()))))

    peerA2.engine.localGatewayEvents(Some(Set(route1.route)), Seq())

    matchAndPushEventBatches(a2ToB, Seq(rowAppend(a2ToB.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 0)), Set()))))

    matchAndPushSourceUpdate(a2ToB, route1.rowSet ++ Set(a2ToB.source.routeRow))
    matchAndClearGatewayUpdate(peerA2.gateway, route1.routeToTableRows)

    checkAllClear()

    val a2batch = route1.firstBatch(peerA2.session, n = 2)
    transferAtoBtoDtoSub(peerA2, a2ToB, aToC, a2batch)

    checkAllClear()

  }*/

  /*test("A reboots twice, sessions are synthesized once singular") {

    val s = new ReplaceableFourPeers
    import s._

    twoBatchStartingSequence()

    def peerAStartupSequence(peerAX: MockPeer, axToB: MockPeerSource): Unit = {

      matchAndPushSourceUpdate(axToB, manifestRowsForPeer(axToB.source.session))
      matchAndPushEventBatches(axToB, Seq(rowAppend(axToB.source.routeRow, sessResyncManifest(axToB.source.session, 0, Map()))))

      peerAX.engine.localGatewayEvents(Some(Set(route1.route)), Seq())

      matchAndPushEventBatches(axToB, Seq(rowAppend(axToB.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 0)), Set()))))

      checkAllClear()
    }

    // PeerA1 is dead but not disconnected (yet)
    val peerA2 = peer("A2")
    val a2ToB = subscribe("A2toB", peerA2, peerB)
    peerAStartupSequence(peerA2, a2ToB)

    val peerA3 = peer("A3")
    val a3toB = subscribe("A3toB", peerA3, peerB)
    peerAStartupSequence(peerA3, a3toB)

    logger.debug("Removing aToB")
    peerB.engine.sourceDisconnected(aToB)

    checkAllClear()
  }*/
}
