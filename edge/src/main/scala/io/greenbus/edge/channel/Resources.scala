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

trait BaseAggregator[Result] extends Aggregator {
  private var completed = false
  private var buckets = Vector.empty[Fillable]

  protected def addContainer[A <: Fillable](obj: A): A = {
    buckets = buckets :+ obj
    obj
  }

  def check(): Boolean = {
    if (!completed && buckets.forall(_.nonEmpty)) {
      complete(result())
      completed = true
      true
    } else {
      false
    }
  }

  protected def result(): Result

  protected val complete: Result => Unit
}
trait ObjectAggregator[Result] extends BaseAggregator[Result] {
  def bucket[A]: Bucket[A] = {
    val buck = new Bucket[A]
    addContainer(buck)
  }
}

trait BaseTypedAggregator[Base, Result] extends Aggregator {
  private var completed = false
  private var buckets = Vector.empty[Bucket[_ <: Base]]

  protected def addContainer[A <: Base](bucket: Bucket[A]): Bucket[A] = {
    buckets = buckets :+ bucket
    bucket
  }

  def check(): Boolean = {
    if (!completed && buckets.forall(_.nonEmpty)) {
      complete(result())
      completed = true
      true
    } else {
      false
    }
  }

  protected def filled: Seq[Base] = buckets.flatMap(_.option)

  protected def result(): Result

  protected val complete: Result => Unit
}

trait ChannelAggregator[Result] extends BaseTypedAggregator[Closeable, Result] {

  def sender[A, B]: Bucket[TransferChannelSender[A, B]] = {
    val buck = new Bucket[TransferChannelSender[A, B]]
    addContainer(buck)
  }
  def receiver[A, B]: Bucket[TransferChannelReceiver[A, B]] = {
    val buck = new Bucket[TransferChannelReceiver[A, B]]
    addContainer(buck)
  }

  override def cleanup(): Unit = {
    filled.foreach(_.close())
  }
}
