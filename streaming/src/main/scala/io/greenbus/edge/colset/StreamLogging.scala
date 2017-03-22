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

object StreamLogging {

  def simpleRow(rowId: RowId): String = {
    s"Row(${rowId.routingKey.simpleString()}, ${rowId.table}, ${rowId.rowKey.simpleString()})"
  }

  def simpleTup(tup: (TypeValue, TypeValue)): String = {
    s"(${tup._1.simpleString()}, ${tup._2.simpleString()})"
  }

  def simpleAppendSetSequence(up: AppendSetSequence): String = {
    up.appends.map(av => (av.sequence.simpleString(), av.value.simpleString())).mkString("Appends(", ", ", ")")
  }

  def simpleDelta(update: SetDelta): String = {
    update match {
      case up: ModifiedSetDelta => s"ModifiedSetDelta(${up.sequence.simpleString()}, ${up.removes.map(_.simpleString())}, ${up.adds.map(_.simpleString())})"
      case up: ModifiedKeyedSetDelta => s"ModifiedKeyedSetDelta(${up.sequence.simpleString()}, ${up.removes.map(_.simpleString())}, ${up.adds.map(simpleTup)}, ${up.modifies.map(simpleTup)})"
      case up: AppendSetSequence => simpleAppendSetSequence(up)
    }
  }

  def simpleSnapshot(update: SetSnapshot): String = {
    update match {
      case up: ModifiedSetSnapshot => s"ModifiedSetSnapshot(${up.sequence.simpleString()}, ${up.snapshot.map(_.simpleString())})"
      case up: ModifiedKeyedSetSnapshot => s"ModifiedKeyedSetSnapshot(${up.sequence.simpleString()}, ${up.snapshot.map(simpleTup)})"
      case up: AppendSetSequence => simpleAppendSetSequence(up)
    }
  }

  def simpleAppend(append: AppendEvent): String = {
    append match {
      case sd: StreamDelta => s"StreamDelta(${simpleDelta(sd.update)})"
      case sd: ResyncSnapshot => s"ResyncSnapshot(${simpleSnapshot(sd.snapshot)})"
      case sd: ResyncSession => s"ResyncSession(${sd.sessionId}, ${simpleSnapshot(sd.snapshot)})"
    }
  }

  def logEvent(ev: StreamEvent): String = {
    ev match {
      case un: RouteUnresolved => s"Routing key unresolved: ${un.routingKey}"
      case rapp: RowAppendEvent =>
        simpleRow(rapp.rowId) + " : " + simpleAppend(rapp.appendEvent)
    }
  }

  def logEvents(events: Seq[StreamEvent]): String = {
    if (events.nonEmpty) events.map(StreamLogging.logEvent).mkString("\n\t", "\n\t", "") else ""
  }
}