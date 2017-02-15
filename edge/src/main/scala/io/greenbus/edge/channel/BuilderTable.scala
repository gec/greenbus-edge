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

trait BuilderTable[Key, Result, Handler] {
  def handleForKey(key: Key, closeable: Closeable, set: Handler => Unit)
}

class BuilderTableImpl[Key, Result, Handler <: Aggregator](buildHandler: Key => Handler) extends BuilderTable[Key, Result, Handler] {

  private var pending = Map.empty[Key, Handler]

  def handleForKey(key: Key, closeable: Closeable, set: Handler => Unit): Unit = {
    closeable.onClose.bind(() => onClose(key))
    pending.get(key) match {
      case None =>
        val agg = buildHandler(key)
        set(agg)
        pending += (key -> agg)
        agg.check()
      case Some(agg) =>
        set(agg)
        agg.check()
    }
  }

  private def onClose(key: Key): Unit = {
    pending.get(key).foreach(_.cleanup())
    pending -= key
  }
}