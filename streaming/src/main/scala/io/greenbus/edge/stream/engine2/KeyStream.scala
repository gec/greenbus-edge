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

import io.greenbus.edge.stream.{ AppendEvent, filter }

import scala.collection.mutable

trait KeyStreamObserver {
  def handle(event: AppendEvent): Unit
}

trait SourcedKeyStreamObserver[Source] {
  def handle(source: Source, event: AppendEvent): Unit
}

trait KeyStream[Source] extends SourcedKeyStreamObserver[Source] {

  def sourceRemoved(source: Source): Unit

  def targeted(): Boolean
  def targetAdded(observer: KeyStreamObserver): Unit
  def targetRemoved(observer: KeyStreamObserver): Unit
}

trait KeyStreamSubject {
  def targeted(): Boolean
  def targetAdded(observer: KeyStreamObserver): Unit
  def targetRemoved(observer: KeyStreamObserver): Unit
}

trait StreamObserverSet {
  def observers: Iterable[KeyStreamObserver]
}

trait CachingKeyStreamSubject extends KeyStreamSubject with StreamObserverSet {
  //protected val cache: filter.StreamCache
  protected def sync(): Seq[AppendEvent]
  private val observerSet = mutable.Set.empty[KeyStreamObserver]

  def observers: Iterable[KeyStreamObserver] = observerSet

  def targeted(): Boolean

  def targetAdded(observer: KeyStreamObserver): Unit = {
    if (!observerSet.contains(observer)) {
      observerSet += observer
      val emitted = sync()
      emitted.foreach(observer.handle)
    }
  }
  def targetRemoved(observer: KeyStreamObserver): Unit = {
    observerSet -= observer
  }
}

class SynthesizedKeyStream[Source](appendLimitDefault: Int) extends KeyStream[Source] {
  private val retail = new RetailKeyStream(appendLimitDefault)
  private val synth = new SynthKeyStream[Source](retail, appendLimitDefault)

  def handle(source: Source, event: AppendEvent): Unit = {
    synth.handle(source, event)
  }

  def sourceRemoved(source: Source): Unit = {
    synth.sourceRemoved(source)
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

class SynthKeyStream[Source](observer: KeyStreamObserver, appendLimitDefault: Int) extends SourcedKeyStreamObserver[Source] {

  private val rowSynthesizer: RowSynthesizer[Source] = new RowSynthImpl[Source](appendLimitDefault)

  def handle(source: Source, event: AppendEvent): Unit = {
    val emitted = rowSynthesizer.append(source, event)
    if (emitted.nonEmpty) {
      emitted.foreach(ev => observer.handle(ev))
    }
  }

  def sourceRemoved(source: Source): Unit = {
    val emitted = rowSynthesizer.sourceRemoved(source)
    if (emitted.nonEmpty) {
      emitted.foreach(ev => observer.handle(ev))
    }
  }
}

class RetailKeyStream(appendLimitDefault: Int) extends KeyStreamObserver with CachingKeyStreamSubject {
  protected val cache = new filter.StreamCacheImpl(appendLimitDefault)
  protected def sync(): Seq[AppendEvent] = cache.resync()

  def targeted(): Boolean = observers.nonEmpty

  def handle(event: AppendEvent): Unit = {
    cache.handle(event)
    this.observers.foreach(_.handle(event))
  }
}
