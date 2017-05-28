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
import io.greenbus.edge.stream.{ MockPeerSource, MockSource, _ }
import org.junit.runner.RunWith
import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

object PeerTestFramework extends Matchers {

  class MockSubscriber {

  }

  object MockPeer {

    def subscribe(name: String, source: MockPeer, target: MockPeer): MockPeerSource = {
      new MockPeerSource(name, source, target)
    }
  }
  class MockPeer(val name: String, sessionOpt: Option[PeerSessionId] = None) {
    val gateway = new MockRouteSource
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

    val sourcing = new MockRouteSource
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
  }

  def matchAndPushEventBatches(sub: MockPeerSource, events: Seq[StreamEvent]): Unit = {
    sub.checkAndPushEvents(e => e should equal(events))
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
        println(l.name)
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

  ignore("four peer scenario, subscribe before source arrives") {

    val s = new ConnectedFourPeers
    import s._

    checkAllClear()

    peerD.subscribe(subQ, route1.rowSet)
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
}
