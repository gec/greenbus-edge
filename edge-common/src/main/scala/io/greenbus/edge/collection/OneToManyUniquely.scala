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
package io.greenbus.edge.collection

import scala.collection.mutable

object OneToManyUniquely {
  def empty[A, B]: OneToManyUniquely[A, B] = new OneToManyUniquely[A, B]
}
class OneToManyUniquely[A, B] {

  private val aToSetB = mutable.Map.empty[A, mutable.Set[B]]
  private val bToA = mutable.Map.empty[B, A]

  def getFirst(a: A): Option[Set[B]] = aToSetB.get(a).map(_.toSet)
  def getSecond(b: B): Option[A] = bToA.get(b)

  def keys: Set[A] = aToSetB.keySet.toSet
  def values: Set[B] = bToA.keySet.toSet

  def put(a: A, b: B): Unit = {
    bToA.get(b).foreach { a =>
      aToSetB.get(a).foreach(set => set -= b)
    }
    bToA += ((b, a))
    val set = aToSetB.getOrElseUpdate(a, mutable.Set.empty[B])
    set += b
  }

  def remove(a: A, b: B): Unit = {
    bToA.get(b).foreach { a =>
      aToSetB.get(a).foreach(set => set -= b)
    }
    bToA -= b
  }

  def removeAll(a: A): Unit = {
    aToSetB.get(a).foreach(_.foreach(bToA -= _))
    aToSetB -= a
  }
}