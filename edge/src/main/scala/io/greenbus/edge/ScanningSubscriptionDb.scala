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
package io.greenbus.edge

object ScanningSubscriptionDb {
  case class AssocRecord(
    prefixes: Set[Path] = Set(),
    infos: Set[EndpointId] = Set(),
    datas: Set[EndpointPath] = Set(),
    outputs: Set[EndpointPath] = Set())
}
class ScanningSubscriptionDb[A] extends SubscriptionDb[A] {
  import ScanningSubscriptionDb._
  private var prefixSubscriptions = Map.empty[Path, Seq[A]]
  private var infoSubscriptions = Map.empty[EndpointId, Seq[A]]
  private var dataSubscriptions = Map.empty[EndpointPath, Seq[A]]
  private var outputSubscriptions = Map.empty[EndpointPath, Seq[A]]
  private var assocations = Map.empty[A, AssocRecord]

  private def addPrefixSub(path: Path, obj: A): Unit = {
    val entry = prefixSubscriptions.getOrElse(path, Seq())
    prefixSubscriptions += (path -> (entry :+ obj))
  }
  private def removeFromPrefix(path: Path, obj: A): Unit = {
    prefixSubscriptions.get(path).foreach { seq =>
      val filtered = seq.filterNot(_ == obj)
      if (filtered.nonEmpty) {
        prefixSubscriptions += (path -> filtered)
      } else {
        prefixSubscriptions -= path
      }
    }
  }

  private def addInfoSub(key: EndpointId, obj: A): Unit = {
    val entry = infoSubscriptions.getOrElse(key, Seq())
    infoSubscriptions += (key -> (entry :+ obj))
  }
  private def removeFromInfoSub(key: EndpointId, obj: A): Unit = {
    infoSubscriptions.get(key).foreach { seq =>
      val filtered = seq.filterNot(_ == obj)
      if (filtered.nonEmpty) {
        infoSubscriptions += (key -> filtered)
      } else {
        infoSubscriptions -= key
      }
    }
  }

  private def addDataSub(key: EndpointPath, obj: A): Unit = {
    val entry = dataSubscriptions.getOrElse(key, Seq())
    dataSubscriptions += (key -> (entry :+ obj))
  }
  private def removeFromDataSub(key: EndpointPath, obj: A): Unit = {
    dataSubscriptions.get(key).foreach { seq =>
      val filtered = seq.filterNot(_ == obj)
      if (filtered.nonEmpty) {
        dataSubscriptions += (key -> filtered)
      } else {
        dataSubscriptions -= key
      }
    }
  }

  private def addOutputSub(key: EndpointPath, obj: A): Unit = {
    val entry = outputSubscriptions.getOrElse(key, Seq())
    outputSubscriptions += (key -> (entry :+ obj))
  }
  private def removeFromOutputSub(key: EndpointPath, obj: A): Unit = {
    outputSubscriptions.get(key).foreach { seq =>
      val filtered = seq.filterNot(_ == obj)
      if (filtered.nonEmpty) {
        outputSubscriptions += (key -> filtered)
      } else {
        outputSubscriptions -= key
      }
    }
  }

  def add(params: ClientSubscriptionParams, obj: A): Unit = {

    params.endpointSetPrefixes.foreach(addPrefixSub(_, obj))
    params.infoSubscriptions.foreach(addInfoSub(_, obj))
    params.dataSubscriptions.foreach(addDataSub(_, obj))
    params.outputSubscriptions.foreach(addOutputSub(_, obj))

    val assoc = assocations.getOrElse(obj, AssocRecord())
    assocations += (obj -> AssocRecord(
      prefixes = assoc.prefixes ++ params.endpointSetPrefixes,
      infos = assoc.infos ++ params.infoSubscriptions,
      datas = assoc.datas ++ params.dataSubscriptions,
      outputs = assoc.outputs ++ params.outputSubscriptions))
  }

  def remove(obj: A): Unit = {
    assocations.get(obj).foreach { assoc =>
      assoc.prefixes.foreach(removeFromPrefix(_, obj))
      assoc.infos.foreach(removeFromInfoSub(_, obj))
      assoc.datas.foreach(removeFromDataSub(_, obj))
      assoc.outputs.foreach(removeFromOutputSub(_, obj))
      assocations -= obj
    }
  }

  def queryForEndpointSetPrefixMatches(path: Path): Seq[(Path, Seq[A])] = {
    // note: we care if param is a/b/c and the sub was a/b
    // TODO: tree walk
    prefixSubscriptions.filterKeys(subPath => Path.isPrefixOf(subPath, path)).toVector
  }

  def queryEndpointInfoSubscriptions(endpointId: EndpointId): Seq[A] = {
    infoSubscriptions.getOrElse(endpointId, Seq())
  }

  def queryDataSubscriptions(key: EndpointPath): Seq[A] = {
    dataSubscriptions.getOrElse(key, Seq())
  }

  def queryOutputSubscriptions(key: EndpointPath): Seq[A] = {
    outputSubscriptions.getOrElse(key, Seq())
  }
}
