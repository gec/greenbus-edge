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
package io.greenbus.edge.stream.gateway

import io.greenbus.edge.stream._

/*

Route unthreadsafe object:
- user sink handles
- update the sinks
- flush

if bound, publish delta
else cache



dest:
stream event batch


user ->
sequencer ->
batch publish when bound

user ->
sequencer ->
"queue" <- pop, respecting backpressure

------------------------
[user -> sequencer *> batch] <- flush
[handleFlush -> queues] <- pop

OR

[user -> batch] <- flush
[handleFlush -> sequencer -> queues] <- pop


(abstract)
sequencer -> cache -> channel queue

sequencer -> mgr [cache, observable] -> channel queue

(really abstract)
source (synth | sequencer) -> mgr [cache, observable] -> channel queue (<- pop)

THREE 1/2 PLACES:
- producer (gateway client)
- peer synthesis
  - synth keeps logs around until they become active, then is a filter
  - "retail cache"
- subscriber (volatile link to peer)
  - needs to do deep equality on session change


cache:
producer cache is always the same session/ctx
peer cache changes ctx; could keep multiple contexts around for history resync



 */

class Gateway2 {

}

trait SequenceCache {
  def append(event: SequenceEvent): Unit
}

/*
trait StreamCache {
  def handle(append: AppendEvent): Unit
  def sync(): ResyncSession
}
 */
trait StreamCache {
  def handle(event: AppendEvent): Unit
  def resync(): RowEvent
}

class StreamMgrImpl {
  def handle(event: AppendEvent): Unit = {

  }
}

class AppendSequencer[A] {
  private var sequence: Long = 0

  def append(values: A*): Iterable[(Long, A)] = {
    val start = sequence
    val results = values.toIterable.zipWithIndex.map {
      case (v, i) => (start + i, v)
    }
    sequence += values.size
    results
  }
}

/*class AppendCache[A] {
  def updates(sequenced: Iterable[(Long, A)]): Unit = {

  }
  def sync()
}*/

class AppendChannelQueue[A] {

  //def resync()
}

/*

class AppendSequencer[A] {
  private var sequence: Long = 0

  def append(values: A*): Iterable[(Long, A)] = {
    val start = sequence
    val results = values.toIterable.zipWithIndex.map {
      case (v, i) => (start + i, v)
    }
    sequence += values.size
    results
  }
}*/
