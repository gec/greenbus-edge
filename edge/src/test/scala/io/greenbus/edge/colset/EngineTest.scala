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

import org.junit.runner.RunWith
import org.scalatest.{ FunSuite, Matchers }
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

class MockPeerSourceLink extends PeerSourceLink with MockSource

class MockSubscriber extends SubscriptionTarget {
  val batches: mutable.Queue[Seq[StreamEvent]] = mutable.Queue.empty[Seq[StreamEvent]]

  def handleBatch(events: Seq[StreamEvent]): Unit = {
    batches += events
  }
}

@RunWith(classOf[JUnitRunner])
class EngineTest extends FunSuite with Matchers {
  import Helpers._

  test("subscribe then local gateway publishes") {

    val route1 = UuidVal(UUID.randomUUID())
    val route1Row1 = RowId(route1, SymbolVal("testTable"), SymbolVal("row1"))
    val route1Row2 = RowId(route1, SymbolVal("testTable"), SymbolVal("row2"))
    val route1Row3 = RowId(route1, SymbolVal("testTable"), SymbolVal("row3"))
    val route1Rows = Seq(route1Row1, route1Row2, route1Row3)
    val route1RowsSet = route1Rows.toSet
    val route1TableRowsSet = route1Rows.map(_.tableRow).toSet

    val gateway = new MockGateway
    val subQ = new MockSubscriber

    val sessA = sessId

    val engine = new PeerStreamEngine(sessA, gateway)

    engine.subscriptionsRegistered(subQ, StreamSubscriptionParams(route1Rows))

    engine.localGatewayRoutingUpdate(Set(route1))

    {
      gateway.subUpdates.size should equal(1)

      val gateSub = gateway.subUpdates.dequeue()
      gateSub shouldEqual ((route1, route1TableRowsSet))
    }

    {
      subQ.batches.size should equal(1)
      val batch = subQ.batches.dequeue()
      batch shouldEqual Seq(RouteUnresolved(route1))
    }

    val firstRetailBatch = Seq(
      RowAppendEvent(route1Row1, ResyncSession(sessA, ModifiedSetSnapshot(UInt64Val(0), Set(UInt64Val(5), UInt64Val(3))))),
      RowAppendEvent(route1Row2, ResyncSession(sessA, ModifiedKeyedSetSnapshot(UInt64Val(0), Map(UInt64Val(3) -> UInt64Val(9))))),
      RowAppendEvent(route1Row3, ResyncSession(sessA, AppendSetSequence(Seq(AppendSetValue(UInt64Val(0), UInt64Val(66)))))))

    engine.localGatewayRetailEvents(firstRetailBatch)

    {
      subQ.batches.size should equal(1)

      val batch = subQ.batches.dequeue()
      batch shouldEqual firstRetailBatch
    }

    val secondRetailBatch = Seq(
      RowAppendEvent(route1Row1, StreamDelta(ModifiedSetDelta(UInt64Val(1), Set(UInt64Val(5)), Set(UInt64Val(7))))),
      RowAppendEvent(route1Row2, StreamDelta(ModifiedKeyedSetDelta(UInt64Val(1), Set(), Set(UInt64Val(4) -> UInt64Val(55)), Set(UInt64Val(3) -> UInt64Val(8))))),
      RowAppendEvent(route1Row3, StreamDelta(AppendSetSequence(Seq(AppendSetValue(UInt64Val(1), UInt64Val(77)))))))

    engine.localGatewayRetailEvents(secondRetailBatch)

    {
      subQ.batches.size should equal(1)

      val batch = subQ.batches.dequeue()
      batch shouldEqual secondRetailBatch
    }

    engine.localGatewayRoutingUpdate(Set())

    {
      subQ.batches.size should equal(1)

      val batch = subQ.batches.dequeue()
      batch shouldEqual Seq(RouteUnresolved(route1))
    }

  }

}
