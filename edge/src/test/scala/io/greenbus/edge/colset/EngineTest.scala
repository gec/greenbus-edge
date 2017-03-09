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
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

object Helpers {
  def sessId: PeerSessionId = PeerSessionId(UUID.randomUUID(), 0)
}

class MockGateway extends LocalGateway {
  val subUpdates: mutable.Queue[(TypeValue, Set[TableRow])] = mutable.Queue.empty[(TypeValue, Set[TableRow])]

  def updateRowsForRoute(route: TypeValue, rows: Set[TableRow]): Unit = {
    subUpdates += ((route, rows))
  }
}

trait MockSource extends StreamSource {

  val subUpdates: mutable.Queue[Set[RowId]] = mutable.Queue.empty[Set[RowId]]

  def setSubscriptions(rows: Set[RowId]): Unit = {
    subUpdates += rows
  }
}

//class MockPeerSourceLink extends PeerSourceLink with MockSource

trait MockSubscriber extends SubscriptionTarget {
  val batches: mutable.Queue[Seq[StreamEvent]] = mutable.Queue.empty[Seq[StreamEvent]]

  def handleBatch(events: Seq[StreamEvent]): Unit = {
    batches += events
  }
}

class SimpleMockSubscriber extends MockSubscriber

class MockPeerSource(val name: String, val source: MockPeer, val target: MockPeer) extends MockSubscriber with PeerSourceLink with MockSource {

  def pushSubs(): Unit = {
    subUpdates.foreach { rowSet =>
      source.engine.subscriptionsRegistered(this, StreamSubscriptionParams(rowSet.toVector))
    }
    subUpdates.clear()
  }
}

object MockPeer {

  def subscribe(name: String, source: MockPeer, target: MockPeer): MockPeerSource = {
    val mps = new MockPeerSource(name, source, target)
    target.engine.sourceConnected(source.session, mps)
    mps
  }
}
class MockPeer(val name: String) {
  val gateway = new MockGateway
  val session: PeerSessionId = Helpers.sessId
  val engine = new PeerStreamEngine(name, session, gateway)

  def routeRow: RowId = {
    PeerManifestDb.peerRouteRow(session)
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

  //val typeValueRows =
}

@RunWith(classOf[JUnitRunner])
class EngineTest extends FunSuite with Matchers with LazyLogging {

  import Helpers._

  test("subscribe then local gateway publishes") {

    val route1 = new SimpleRoute

    val gateway = new MockGateway
    val subQ = new SimpleMockSubscriber

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
      RowAppendEvent(route1.row1, ResyncSession(sessA, ModifiedSetSnapshot(UInt64Val(0), Set(UInt64Val(5), UInt64Val(3))))),
      RowAppendEvent(route1.row2, ResyncSession(sessA, ModifiedKeyedSetSnapshot(UInt64Val(0), Map(UInt64Val(3) -> UInt64Val(9))))),
      RowAppendEvent(route1.row3, ResyncSession(sessA, AppendSetSequence(Seq(AppendSetValue(UInt64Val(0), UInt64Val(66)))))))

    engine.localGatewayEvents(None, firstRetailBatch)

    {
      subQ.batches.size should equal(1)

      val batch = subQ.batches.dequeue()
      batch shouldEqual firstRetailBatch
    }

    val secondRetailBatch = Seq(
      RowAppendEvent(route1.row1, StreamDelta(ModifiedSetDelta(UInt64Val(1), Set(UInt64Val(5)), Set(UInt64Val(7))))),
      RowAppendEvent(route1.row2, StreamDelta(ModifiedKeyedSetDelta(UInt64Val(1), Set(), Set(UInt64Val(4) -> UInt64Val(55)), Set(UInt64Val(3) -> UInt64Val(8))))),
      RowAppendEvent(route1.row3, StreamDelta(AppendSetSequence(Seq(AppendSetValue(UInt64Val(1), UInt64Val(77)))))))

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

  def matchAndPushEventBatches(sub: MockPeerSource, batches: Seq[StreamEvent]*): Unit = {
    matchEventBatches(sub, batches: _*)
    sub.batches.foreach(batch => sub.target.engine.peerSourceEvents(sub, batch))
    sub.batches.clear()
  }

  def matchAndClearEventBatches(sub: MockSubscriber, batches: Seq[StreamEvent]*): Unit = {
    sub.batches.size shouldEqual batches.size
    batches.foreach { matchBatch =>
      val batch = sub.batches.dequeue()
      batch shouldEqual matchBatch
    }
  }

  def matchEventBatches(sub: MockSubscriber, batches: Seq[StreamEvent]*): Unit = {
    sub.batches shouldEqual batches
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

  def routeRow(session: PeerSessionId): RowId = {
    PeerManifestDb.peerRouteRow(session)
  }
  def manifestRowsForPeer(session: PeerSessionId): Set[RowId] = {
    Set(PeerManifestDb.peerRouteRow(session))
  }


  object MockData {
    def rowAppend(row: RowId, append: AppendEvent): RowAppendEvent = {
      RowAppendEvent(row, append)
    }
    def sessResyncSet(session: PeerSessionId, seq: Long, set: Set[Long]): ResyncSession = {
      ResyncSession(session, ModifiedSetSnapshot(UInt64Val(seq), set.map(UInt64Val)))
    }
    def sessResyncManifest(session: PeerSessionId, seq: Long, manifest: Map[TypeValue, Int]): ResyncSession = {
      ResyncSession(session, ModifiedKeyedSetSnapshot(UInt64Val(seq), manifest.mapValues(dist => RouteManifestEntry.toTypeValue(RouteManifestEntry(dist)))))
    }

    def writeManifestTup(tup: (TypeValue, Int)): (TypeValue, TypeValue) = {
      (tup._1, UInt64Val(tup._2))
    }

    def manifestUpdate(seq: Long, removes: Set[TypeValue], adds: Set[(TypeValue, Int)], modifies: Set[(TypeValue, Int)]): StreamDelta = {
      StreamDelta(ModifiedKeyedSetDelta(UInt64Val(seq), removes, adds.map(writeManifestTup), modifies.map(writeManifestTup)))
    }
  }

  import MockData._

  test("four peer scenario") {

    val route1 = new SimpleRoute
    val subQ = new SimpleMockSubscriber

    val peerA = new MockPeer("A")
    val peerB = new MockPeer("B")
    val peerC = new MockPeer("C")
    val peerD = new MockPeer("D")

    val aToB = MockPeer.subscribe("AtoB", peerA, peerB)
    val aToC = MockPeer.subscribe("AtoC", peerA, peerC)
    val bToD = MockPeer.subscribe("BtoD", peerB, peerD)
    val cToD = MockPeer.subscribe("CtoD", peerC, peerD)

    val links = Seq(aToB, aToC, bToD, cToD)

    links.foreach(l => matchAndPushSourceUpdate(l, manifestRowsForPeer(l.source.session)))

    def checkLinksClear() = {
      links.foreach { l =>
        if (l.batches.nonEmpty) fail(s"${l.name} has event batches: " + l.batches)
        if (l.subUpdates.nonEmpty) fail(s"${l.name} has subscription updates: " + l.subUpdates)
      }
    }

    //links.foreach(_.pushSubs())

    links.foreach { l =>
      logger.debug("Match manifest resyncs: " + l.name)
      matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, sessResyncManifest(l.source.session, 0, Map()))))
    }

    checkLinksClear()

    peerD.engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1.rows))
    matchAndClearEventBatches(subQ, Seq(RouteUnresolved(route1.route)))

    checkLinksClear()

    peerA.engine.localGatewayEvents(Some(Set(route1.route)), Seq())

    Seq(aToB, aToC).foreach { l =>
      logger.debug("Manifest update: " + l.name)
      matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 0)), Set()))))
    }

    Seq(bToD, cToD).foreach { l =>
      logger.debug("Manifest update: " + l.name)
      matchAndPushEventBatches(l, Seq(rowAppend(l.source.routeRow, manifestUpdate(1, Set(), Set((route1.route, 1)), Set()))))
    }

    println(aToB.subUpdates)

    matchAndPushSourceUpdate(bToD, route1.rowSet ++ Set(bToD.source.routeRow))

    println(aToB.subUpdates)

    matchAndPushSourceUpdate(aToB, route1.rowSet ++ Set(aToB.source.routeRow))

    checkLinksClear()


    /*
org.scalatest.exceptions.TestFailedException:
Queue(Set(RowId(TupleVal(List(UuidVal(1dcf0685-6c3a-46bd-a155-9b7d84118395), UInt64Val(0))),SymbolVal(__manifest),SymbolVal(routes))), Set(RowId(TupleVal(List(UuidVal(1dcf0685-6c3a-46bd-a155-9b7d84118395), UInt64Val(0))),SymbolVal(__manifest),SymbolVal(routes)), RowId(UuidVal(bc43470e-6139-4d90-9d8a-947481f264b0),SymbolVal(testTable),SymbolVal(row1)), RowId(UuidVal(bc43470e-6139-4d90-9d8a-947481f264b0),SymbolVal(testTable),SymbolVal(row2)), RowId(UuidVal(bc43470e-6139-4d90-9d8a-947481f264b0),SymbolVal(testTable),SymbolVal(row3)))) did not equal
Array(Set(RowId(UuidVal(bc43470e-6139-4d90-9d8a-947481f264b0),SymbolVal(testTable),SymbolVal(row1)), RowId(UuidVal(bc43470e-6139-4d90-9d8a-947481f264b0),SymbolVal(testTable),SymbolVal(row2)), RowId(UuidVal(bc43470e-6139-4d90-9d8a-947481f264b0),SymbolVal(testTable),SymbolVal(row3)), RowId(TupleVal(List(UuidVal(1dcf0685-6c3a-46bd-a155-9b7d84118395), UInt64Val(0))),SymbolVal(__manifest),SymbolVal(routes))))


    Queue(Set(RowId(TupleVal(List(UuidVal(5f69bcbc-9c4f-47f2-ae9f-2dbc03f29d92), UInt64Val(0))),SymbolVal(__manifest),SymbolVal(routes)), RowId(UuidVal(7ab3c9e3-3892-4183-bcbd-56253aa6ee71),SymbolVal(testTable),SymbolVal(row1)), RowId(UuidVal(7ab3c9e3-3892-4183-bcbd-56253aa6ee71),SymbolVal(testTable),SymbolVal(row2)), RowId(UuidVal(7ab3c9e3-3892-4183-bcbd-56253aa6ee71),SymbolVal(testTable),SymbolVal(row3))))
    Array(Set(RowId(UuidVal(7ab3c9e3-3892-4183-bcbd-56253aa6ee71),SymbolVal(testTable),SymbolVal(row1)), RowId(UuidVal(7ab3c9e3-3892-4183-bcbd-56253aa6ee71),SymbolVal(testTable),SymbolVal(row2)), RowId(UuidVal(7ab3c9e3-3892-4183-bcbd-56253aa6ee71),SymbolVal(testTable),SymbolVal(row3))))

     */

    //matchAndPushEventBatches(aToB, peerB.engine, )
    //matchEventBatches()

    /*peerD.engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1.rows))

    matchAndClearEventBatches(subQ, Seq(RouteUnresolved(route1.route)))

    peerA.engine.localGatewayEvents(Some(Set(route1.route)), Seq())

    println(aToB.batches)
    println(aToB.subUpdates)*/
    //matchSubs(aToB.subUpdates, Seq())

  }
}
