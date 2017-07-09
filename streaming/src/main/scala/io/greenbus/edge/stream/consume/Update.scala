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
package io.greenbus.edge.stream.consume

import io.greenbus.edge.stream._

trait ValueUpdateSynthesizer {
  def handle(event: AppendEvent): Option[ValueUpdate]
  def current(): ValueUpdate
}

class ValueUpdateSynthesizerImpl extends ValueUpdateSynthesizer {

  private var activeSynthOpt = Option.empty[(Option[TypeValue], UpdateSynthesizer[_ <: DataValueUpdate])]

  def handle(event: AppendEvent): Option[ValueUpdate] = {
    println(activeSynthOpt)
    event match {
      case ev: ResyncSession => {
        activeSynthOpt match {
          case None => {
            val (update, updateFilter) = ev.resync.snapshot match {
              case s: SetSnapshot => (SetUpdated(s.snapshot, Set(), s.snapshot), new SetUpdateSynthesizer(s.snapshot))
              case s: MapSnapshot => (MapUpdated(s.snapshot, Set(), s.snapshot.toSet, Set()), new MapUpdateSynthesizer(s.snapshot))
              case s: AppendSnapshot =>
                val all = AppendUpdateSynthesizer.snapToAppends(s)
                val synth = new AppendUpdateSynthesizer
                synth.resync(ev.resync) // initialize latest value
                (Appended(all), synth)
            }
            println("synth: " + update)
            println("synth: " + updateFilter)

            activeSynthOpt = Some((ev.context.userMetadata, updateFilter))
            Some(ValueSync(ev.context.userMetadata, update))
          }
          case Some((_, synth)) => {
            activeSynthOpt = Some((ev.context.userMetadata, synth))
            synth.resync(ev.resync).map(ValueSync(ev.context.userMetadata, _))
          }
        }
      }
      case ev: StreamDelta => {
        activeSynthOpt.flatMap(_._2.delta(ev.update)).map(dv => ValueDelta(dv))
      }
      case StreamAbsent => {
        Some(ValueAbsent)
      }
    }
  }

  def current(): ValueUpdate = {
    activeSynthOpt match {
      case None => ValueUnresolved // TODO: pending?
      case Some((metaOpt, synth)) => ValueSync(metaOpt, synth.get())
    }
  }
}

