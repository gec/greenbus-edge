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
package io.greenbus.edge.channel

import io.greenbus.edge.flow.Closeable

trait Fillable {
  def isEmpty: Boolean
  def nonEmpty: Boolean = !isEmpty
}

class Bucket[A] extends Fillable {
  protected var vOpt = Option.empty[A]
  def isEmpty: Boolean = vOpt.isEmpty
  def put(v: A): Unit = {
    vOpt = Some(v)
  }
  def option: Option[A] = vOpt
  def get: A = vOpt.get
}

trait Aggregator {
  def check(): Boolean
  def cleanup(): Unit = {}
}

trait BaseTypedAggregator[Base] extends Aggregator {
  private var completed = false
  private var buckets = Vector.empty[Bucket[_ <: Base]]

  protected def addContainer[A <: Base](bucket: Bucket[A]): Bucket[A] = {
    buckets = buckets :+ bucket
    bucket
  }

  def check(): Boolean = {
    if (!completed && buckets.forall(_.nonEmpty)) {
      completed = true
      onComplete()
      true
    } else {
      false
    }
  }

  protected def filled: Seq[Base] = buckets.flatMap(_.option)

  protected def onComplete(): Unit
}

trait CloseableAggregator extends BaseTypedAggregator[Closeable] {

  def bucket[A <: Closeable]: Bucket[A] = {
    val buck = new Bucket[A]
    addContainer(buck)
  }

  override def cleanup(): Unit = {
    filled.foreach(_.close())
  }
}
