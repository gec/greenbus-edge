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

object MapToUniqueValues {

  def apply[A, B](args: (A, B)*): MapToUniqueValues[A, B] = {
    val kToV = MapSetBuilder.newBuilder[A, B]
    val vToK = Map.newBuilder[B, A]
    args.foreach {
      case (k, v) =>
        kToV += (k -> v)
        vToK += (v -> k)
    }
    new MapToUniqueValues[A, B](kToV.result(), vToK.result())
  }

  def empty[A, B] = {
    new MapToUniqueValues[A, B](Map.empty[A, Set[B]], Map.empty[B, A])
  }
}
class MapToUniqueValues[A, B](val keyToVal: Map[A, Set[B]], val valToKey: Map[B, A]) {

  def get(key: A): Option[Set[B]] = {
    keyToVal.get(key)
  }

  def +(tup: (A, B)): MapToUniqueValues[A, B] = {
    add(tup._1, tup._2)
  }

  def add(key: A, v: B): MapToUniqueValues[A, B] = {
    val kToV = removedFromKey(v).get(key) match {
      case None => keyToVal.updated(key, Set(v))
      case Some(set) => keyToVal.updated(key, set + v)
    }

    new MapToUniqueValues[A, B](kToV, valToKey + (v -> key))
  }

  private def removedFromKey(v: B): Map[A, Set[B]] = {
    valToKey.get(v) match {
      case None => keyToVal
      case Some(key) => {
        keyToVal.get(key) match {
          case None => keyToVal
          case Some(set) =>
            val result = set - v
            if (result.nonEmpty) {
              keyToVal.updated(key, result)
            } else {
              keyToVal - key
            }
        }
      }
    }
  }

  def remove(v: B): MapToUniqueValues[A, B] = {
    valToKey.get(v) match {
      case None => this
      case Some(key) => {
        val kToV = keyToVal.get(key) match {
          case None => keyToVal
          case Some(set) =>
            val result = set - v
            if (result.nonEmpty) {
              keyToVal.updated(key, result)
            } else {
              keyToVal - key
            }
        }
        new MapToUniqueValues[A, B](kToV, valToKey - v)
      }
    }

  }

}
