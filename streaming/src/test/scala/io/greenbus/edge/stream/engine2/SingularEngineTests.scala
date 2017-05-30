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
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ FunSuite, Matchers }

import scala.collection.mutable

class MockRouteSource(name: String) extends RouteStreamSource with LazyLogging {
  val subUpdates: mutable.Queue[(TypeValue, Set[TableRow])] = mutable.Queue.empty[(TypeValue, Set[TableRow])]

  def updateSourcing(route: TypeValue, rows: Set[TableRow]): Unit = {
    logger.debug(s"gateway $name got push: " + rows + ", prev: " + subUpdates)
    subUpdates += ((route, rows))
  }

  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit = {}

  override def toString: String = {
    s"MockRouteSource($name)"
  }
}

@RunWith(classOf[JUnitRunner])
class SingularEngineTests extends FunSuite with Matchers with LazyLogging {

  import Helpers._

  class BasicTest {

    val route1 = new SimpleRoute

    val source = new MockRouteSource("sourceA")

    val sessA = sessId

    val engine = new StreamEngine("A", sessA)

    val target = new TargetQueueMgr
    val observers = target.subscriptionUpdate(route1.rows.toSet)
  }

  test("target before source") {
    val t = new BasicTest
    import t._

    engine.targetSubscriptionUpdate(target, observers)

    {
      target.dequeue() should equal(Seq(RouteUnresolved(route1.route)))
    }

    engine.sourceUpdate(source, SourceEvents(Some(Map(route1.route -> RouteManifestEntry(0))), Seq()))

    {
      source.subUpdates.size should equal(1)

      val gateSub = source.subUpdates.dequeue()
      gateSub shouldEqual ((route1.route, route1.tableRowsSet))
    }
  }

  test("source before target") {
    val t = new BasicTest
    import t._

    engine.sourceUpdate(source, SourceEvents(Some(Map(route1.route -> RouteManifestEntry(0))), Seq()))

    {
      source.subUpdates.isEmpty should equal(true)
      target.dequeue().isEmpty should equal(true)
    }

    engine.targetSubscriptionUpdate(target, observers)

    {
      target.dequeue().isEmpty should equal(true) // No unresolved since a source is available

      val gateSub = source.subUpdates.dequeue()
      gateSub shouldEqual ((route1.route, route1.tableRowsSet))
    }
  }

  test("subscribe then local gateway publishes") {
    val t = new BasicTest
    import t._

    engine.targetSubscriptionUpdate(target, observers)

    {
      target.dequeue() should equal(Seq(RouteUnresolved(route1.route)))
    }

    engine.sourceUpdate(source, SourceEvents(Some(Map(route1.route -> RouteManifestEntry(0))), Seq()))

    {
      source.subUpdates.size should equal(1)

      val gateSub = source.subUpdates.dequeue()
      gateSub shouldEqual ((route1.route, route1.tableRowsSet))
    }

    val firstRetailBatch = Seq(
      RowAppendEvent(route1.row1, ResyncSession(sessA, SequenceCtx.empty, Resync(Int64Val(0), SetSnapshot(Set(Int64Val(5), Int64Val(3)))))),
      RowAppendEvent(route1.row2, ResyncSession(sessA, SequenceCtx.empty, Resync(Int64Val(0), MapSnapshot(Map(Int64Val(3) -> Int64Val(9)))))),
      RowAppendEvent(route1.row3, ResyncSession(sessA, SequenceCtx.empty, Resync(Int64Val(0), AppendSnapshot(SequencedDiff(Int64Val(0), AppendValue(Int64Val(66))), Seq())))))

    engine.sourceUpdate(source, SourceEvents(None, firstRetailBatch))

    {
      target.dequeue() should equal(firstRetailBatch)
    }

    val secondRetailBatch = Seq(
      RowAppendEvent(route1.row1, StreamDelta(Delta(Seq(SequencedDiff(Int64Val(1), SetDiff(Set(Int64Val(5)), Set(Int64Val(7)))))))),
      RowAppendEvent(route1.row2, StreamDelta(Delta(Seq(SequencedDiff(Int64Val(1), MapDiff(Set(), Set(Int64Val(4) -> Int64Val(55)), Set(Int64Val(3) -> Int64Val(8)))))))),
      RowAppendEvent(route1.row3, StreamDelta(Delta(Seq(SequencedDiff(Int64Val(1), AppendValue(Int64Val(77))))))))

    engine.sourceUpdate(source, SourceEvents(None, secondRetailBatch))

    {
      target.dequeue() should equal(secondRetailBatch)
    }

    engine.sourceUpdate(source, SourceEvents(Some(Map()), Seq()))

    {
      target.dequeue() should equal(Seq(RouteUnresolved(route1.route)))
    }

  }
}