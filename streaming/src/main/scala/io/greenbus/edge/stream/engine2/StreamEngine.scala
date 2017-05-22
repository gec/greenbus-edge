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
import io.greenbus.edge.flow.{ Sink, Source }
import io.greenbus.edge.stream._

import scala.collection.mutable

/*

  alternative:

  KeyStream
    -> intake filter?
    -> synthesizer
      - session set,
    -> cache
    -> target set
    -> queues

  maps:
    updates/removes to source links (potentially) remove sessions in synth
    updates/removes to source links can remove from cache
    lack of subscription means whole stream goes away
    added subscription means new key stream created or anchored

    peer has separate map for manifest keys?
   */

case class SourceEvents(routeUpdatesOpt: Option[Map[TypeValue, RouteManifestEntry]], events: Seq[StreamEvent])

trait GenericSource {
  def subscriptions: Sink[Set[RowId]]
  def events: Source[SourceEvents]
  def requests: Sink[Seq[ServiceRequest]]
  def responses: Source[Seq[ServiceResponse]]
}

trait GenericTarget {
  def events(events: Seq[StreamEvent]): Unit
}

trait RouteSourcing {
  //def observeForDelivery(events: Seq[StreamEvent]): Unit
  def issueServiceRequests(requests: Seq[ServiceRequest]): Unit
}

trait StreamSourcingManager[Source, Target] {

  def sourcing: Map[TypeValue, RouteSourcing]

  def sourceUpdate(source: Source, routes: Map[TypeValue, RouteManifestEntry]): Unit
  def sourceRemoved(source: Source): Unit

  def targetUpdate(target: Target, rows: Set[RowId]): Unit
  def targetRemoved(target: Target): Unit

}
abstract class StreamEngine {
  // cache
  // sourcing map

  def sourceAdded(source: GenericSource): Unit
  def sourceRemoved(source: GenericSource): Unit
  //def sourceEvents(events: SourceEvents): Unit

  //def targetAdded(): Unit
  def targetSubscriptionUpdate(rows: Set[RowId]): Unit
  def targetRemoved(): Unit

}

trait StreamObserver {
  def handle(routeEvent: StreamEvent): Unit
}

trait RouteStreamSource {
  def updateSourcing(route: TypeValue, rows: Set[TableRow]): Unit
}

trait RowSynthesizer[Source] {
  def append(source: Source, event: AppendEvent): Seq[AppendEvent]
  def sourceRemoved(source: Source): Seq[AppendEvent]
}

/*
class GatewayKeyStream[Source] extends KeyStream[Source] {
  private val retail = new RetailKeyStream
  //private val synth = new SynthKeyStream[Source](retail)

  def handle(source: Source, event: AppendEvent): Unit = {
    retail.handle(event)
  }

  def sourceRemoved(source: Source): Unit = {
    //retail.sourceRemoved(source)
  }

  def targeted(): Boolean = {
    retail.targeted()
  }

  def targetAdded(observer: KeyStreamObserver): Unit = {
    retail.targetAdded(observer)
  }

  def targetRemoved(observer: KeyStreamObserver): Unit = {
    retail.targetRemoved(observer)
  }
}
*/
