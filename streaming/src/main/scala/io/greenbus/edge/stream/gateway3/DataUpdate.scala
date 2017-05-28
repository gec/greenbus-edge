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
package io.greenbus.edge.stream.gateway3

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.stream.{ AppendEvent, PeerSessionId, SequenceCtx, TypeValue }
import io.greenbus.edge.stream.gateway2.{ AppendSequencer, MapSequencer, SetSequencer }

sealed trait ProducerDataUpdate
case class AppendProducerUpdate(values: Seq[TypeValue]) extends ProducerDataUpdate
case class SetProducerUpdate(value: Set[TypeValue]) extends ProducerDataUpdate
case class MapProducerUpdate(value: Map[TypeValue, TypeValue]) extends ProducerDataUpdate

object UpdateSequencer {

  def build(update: ProducerDataUpdate, session: PeerSessionId, ctx: SequenceCtx): UpdateSequencer = {
    update match {
      case _: AppendProducerUpdate => new AppendUpdateSequencer(session, ctx)
      case _: SetProducerUpdate => new SetUpdateSequencer(session, ctx)
      case _: MapProducerUpdate => new MapUpdateSequencer(session, ctx)
    }
  }
}
trait UpdateSequencer {
  def handle(update: ProducerDataUpdate): Seq[AppendEvent]
}

class AppendUpdateSequencer(session: PeerSessionId, ctx: SequenceCtx) extends UpdateSequencer with LazyLogging {

  private val sequencer = new AppendSequencer(session, ctx)

  def handle(update: ProducerDataUpdate): Seq[AppendEvent] = {
    update match {
      case AppendProducerUpdate(values) => sequencer.handle(values)
      case _ =>
        logger.warn(s"Append sequencer got incorrect update type: $update")
        Seq()
    }
  }
}
class SetUpdateSequencer(session: PeerSessionId, ctx: SequenceCtx) extends UpdateSequencer with LazyLogging {

  private val sequencer = new SetSequencer(session, ctx)

  def handle(update: ProducerDataUpdate): Seq[AppendEvent] = {
    update match {
      case SetProducerUpdate(values) => sequencer.handle(values)
      case _ =>
        logger.warn(s"Append sequencer got incorrect update type: $update")
        Seq()
    }
  }
}
class MapUpdateSequencer(session: PeerSessionId, ctx: SequenceCtx) extends UpdateSequencer with LazyLogging {

  private val sequencer = new MapSequencer(session, ctx)

  def handle(update: ProducerDataUpdate): Seq[AppendEvent] = {
    update match {
      case MapProducerUpdate(values) => sequencer.handle(values)
      case _ =>
        logger.warn(s"Append sequencer got incorrect update type: $update")
        Seq()
    }
  }
}
