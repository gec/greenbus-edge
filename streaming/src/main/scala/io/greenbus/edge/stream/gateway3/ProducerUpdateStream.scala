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

import io.greenbus.edge.stream.{ PeerSessionId, SequenceCtx }
import io.greenbus.edge.stream.engine2.StreamObserverSet
import io.greenbus.edge.stream.filter.{ StreamCache, StreamCacheImpl }

class ProducerUpdateStream(sessionId: PeerSessionId, ctx: SequenceCtx, appendLimit: Int) {

  private var sequencerOpt = Option.empty[UpdateSequencer]
  private var observerOpt = Option.empty[StreamObserverSet]

  protected val streamCache = new StreamCacheImpl(appendLimit)

  def cache: StreamCache = streamCache

  def handle(update: ProducerDataUpdate): Unit = {
    val sequencer = sequencerOpt.getOrElse {
      val seq = UpdateSequencer.build(update, sessionId, ctx)
      sequencerOpt = Some(seq)
      seq
    }

    val sequenced = sequencer.handle(update)
    sequenced.foreach(streamCache.handle)
    observerOpt.foreach { set =>
      set.observers.foreach { observer =>
        sequenced.foreach(observer.handle)
      }
    }
  }

  def bind(set: StreamObserverSet): Unit = {
    observerOpt = Some(set)
  }

  def unbind(): Unit = {
    observerOpt = None
  }
}