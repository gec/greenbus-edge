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

sealed trait IndexSubscription
case class EndpointIndexSubscription(specifier: IndexSpecifier, target: ClientSubscriberProxy) extends IndexSubscription
case class DataIndexSubscription(specifier: IndexSpecifier, target: ClientSubscriberProxy) extends IndexSubscription
case class OutputIndexSubscription(specifier: IndexSpecifier, target: ClientSubscriberProxy) extends IndexSubscription

case class IndexUpdate[A](specifier: IndexSpecifier, added: Set[A], removed: Set[A], targets: Set[ClientSubscriberProxy])

abstract class TypedIndexDb[A](endpointDbMgr: EndpointDbMgr) {
  private var activeSets = Map.empty[IndexSpecifier, Set[A]]
  private var elemMap = Map.empty[A, Set[IndexSpecifier]]
  private var queryToTarget = Map.empty[IndexSpecifier, Set[ClientSubscriberProxy]]
  private var targetToQuery = Map.empty[ClientSubscriberProxy, Set[IndexSpecifier]]

  def active: Set[IndexSpecifier] = activeSets.keySet
  def activePaths: Set[Path] = activeSets.keySet.map(_.key)

  protected def activeMap: Map[IndexSpecifier, Set[A]] = activeSets
  protected def elementMap: Map[A, Set[IndexSpecifier]] = elemMap

  protected def targetsFor(specifier: IndexSpecifier): Set[ClientSubscriberProxy] = {
    queryToTarget.getOrElse(specifier, Set()) // should never not be there
  }

  protected def addToIndex(elem: A, specifier: IndexSpecifier): Unit = {
    activeSets.get(specifier) match {
      case None =>
        activeSets += (specifier -> Set(elem))
      case Some(curr) =>
        activeSets += (specifier -> (curr + elem))
    }

    elemMap.get(elem) match {
      case None =>
        elemMap += (elem -> Set(specifier))
      case Some(curr) =>
        elemMap += (elem -> (curr + specifier))
    }
  }

  protected def removeFromIndex(elem: A, specifier: IndexSpecifier): Unit = {
    activeSets.get(specifier).foreach { set =>
      val removed = set - elem
      activeSets += (specifier -> removed)
    }
    elemMap.get(elem).foreach { set =>
      val removed = set - specifier
      elemMap += (elem -> removed)
    }
  }

  def addSubscription(specifier: IndexSpecifier, target: ClientSubscriberProxy): Set[A] = {
    queryToTarget.get(specifier) match {
      case None =>
        queryToTarget += (specifier -> Set(target))
      case Some(targSet) =>
        queryToTarget += (specifier -> (targSet ++ Set(target)))
    }

    targetToQuery.get(target) match {
      case None =>
        targetToQuery += (target -> Set(specifier))
      case Some(specSet) =>
        targetToQuery += (target -> (specSet ++ Set(specifier)))
    }

    activeSets.get(specifier) match {
      case None =>
        val built = buildIndex(specifier)
        activeSets += (specifier -> built)
        built
      case Some(set) => set
    }
  }

  def removeTarget(target: ClientSubscriberProxy): Unit = {
    targetToQuery.get(target).foreach { set =>
      set.foreach { spec =>
        queryToTarget.get(spec).foreach { targSet =>
          val removed = targSet - target
          if (removed.isEmpty) {
            queryToTarget -= spec
            activeSets -= spec
          } else {
            queryToTarget += (spec -> removed)
          }
        }
      }

      targetToQuery -= target
    }
  }

  protected def identifyIndexSet(endpointId: EndpointId, descriptor: EndpointDescriptor): Seq[(A, Map[Path, IndexableValue])]

  private def buildIndex(specifier: IndexSpecifier): Set[A] = {

    val elemMatchSet = Set.newBuilder[A]

    endpointDbMgr.map.foreach {
      case (endId, db) => db.currentInfo().foreach { descRec =>
        val elemToIndexMap = identifyIndexSet(endId, descRec.descriptor)
        elemToIndexMap.foreach {
          case (elem, indexMap) =>
            indexMap.get(specifier.key).foreach { indexValue =>
              if (specifier.valueOpt.isEmpty || specifier.valueOpt.contains(indexValue)) {
                elemMatchSet += elem
              }
            }
        }
      }
    }

    val elemMatches = elemMatchSet.result()

    elemMatches.foreach(e => addToIndex(e, specifier))

    elemMatches
  }

  def observe(id: EndpointId, desc: EndpointDescriptor): Seq[IndexUpdate[A]] = {

    val elemAndIndexMaps = identifyIndexSet(id, desc)

    val matchBuilder = Vector.newBuilder[(A, IndexSpecifier)]

    elemAndIndexMaps.foreach {
      case (elem, indexes) => {

        indexes.filterKeys(activePaths.contains).foreach {
          case (path, value) =>
            if (activeMap.contains(IndexSpecifier(path, Some(value)))) {
              matchBuilder += (elem -> IndexSpecifier(path, Some(value)))
            }
            if (activeMap.contains(IndexSpecifier(path, None))) {
              matchBuilder += (elem -> IndexSpecifier(path, None))
            }
        }
      }
    }

    val matchMap: Map[A, Set[IndexSpecifier]] = {
      val all = matchBuilder.result()
      var map = Map.empty[A, Set[IndexSpecifier]]
      all.foreach {
        case (a, b) =>
          map.get(a) match {
            case None => map += (a -> Set(b))
            case Some(set) => map += (a -> (set + b))
          }
      }
      map
    }

    val adds = Vector.newBuilder[(A, IndexSpecifier)]
    val removes = Vector.newBuilder[(A, IndexSpecifier)]
    val specAddsBuilder = MapSetBuilder.build[IndexSpecifier, A]
    val specRemovesBuilder = MapSetBuilder.build[IndexSpecifier, A]

    matchMap.foreach {
      case (elem, matched) =>
        val current = elementMap.getOrElse(elem, Set())
        val added = matched -- current
        val removed = current -- matched

        added.foreach { spec =>
          specAddsBuilder += (spec -> elem)
          adds += (elem -> spec)
        }

        removed.foreach { spec =>
          specRemovesBuilder += (spec -> elem)
          removes += (elem -> spec)
        }

    }

    val specAdds = specAddsBuilder.result()
    val specRemoves = specRemovesBuilder.result()

    adds.result().foreach { case (elem, spec) => addToIndex(elem, spec) }
    removes.result().foreach { case (elem, spec) => removeFromIndex(elem, spec) }

    val specSet = matchMap.values.flatten.toVector

    specSet.map { spec =>
      val adds = specAdds.getOrElse(spec, Set())
      val removes = specRemoves.getOrElse(spec, Set())

      IndexUpdate(spec, adds, removes, targetsFor(spec))
    }
  }
}

object MapSetBuilder {

  def build[A, B] = {
    new Impl[A, B]
  }

  class Impl[A, B] extends MapSetBuilder[A, B] {
    private var map = Map.empty[A, Set[B]]
    def +=(a: A, b: B): Unit = {
      map.get(a) match {
        case None => map += (a -> Set(b))
        case Some(set) => map += (a -> (set + b))
      }
    }

    def +=(tup: (A, B)): Unit = {
      +=(tup._1, tup._2)
    }

    def result(): Map[A, Set[B]] = {
      map
    }
  }

}
trait MapSetBuilder[A, B] {
  def +=(a: A, b: B): Unit
  def +=(tup: (A, B)): Unit
  def result(): Map[A, Set[B]]
}

object MapSeqBuilder {

  def build[A, B] = {
    new Impl[A, B]
  }

  class Impl[A, B] extends MapSeqBuilder[A, B] {
    private var map = Map.empty[A, Seq[B]]
    def +=(a: A, b: B): Unit = {
      map.get(a) match {
        case None => map += (a -> Vector(b))
        case Some(set) => map += (a -> (set :+ b))
      }
    }

    def +=(tup: (A, B)): Unit = {
      +=(tup._1, tup._2)
    }

    def result(): Map[A, Seq[B]] = {
      map
    }
  }

}
trait MapSeqBuilder[A, B] {
  def +=(a: A, b: B): Unit
  def +=(tup: (A, B)): Unit
  def result(): Map[A, Seq[B]]
}

class EndpointIndexDb(endpointDbMgr: EndpointDbMgr) extends TypedIndexDb[EndpointId](endpointDbMgr) {

  protected def identifyIndexSet(endpointId: EndpointId, descriptor: EndpointDescriptor): Seq[(EndpointId, Map[Path, IndexableValue])] = {
    Seq((endpointId, descriptor.indexes))
  }
}

class DataKeyIndexDb(endpointDbMgr: EndpointDbMgr) extends TypedIndexDb[EndpointPath](endpointDbMgr) {

  protected def identifyIndexSet(endpointId: EndpointId, descriptor: EndpointDescriptor): Seq[(EndpointPath, Map[Path, IndexableValue])] = {
    descriptor.dataKeySet.map {
      case (path, desc) =>
        (EndpointPath(endpointId, path), desc.indexes)
    }.toVector
  }
}

class OutputKeyIndexDb(endpointDbMgr: EndpointDbMgr) extends TypedIndexDb[EndpointPath](endpointDbMgr) {

  protected def identifyIndexSet(endpointId: EndpointId, descriptor: EndpointDescriptor): Seq[(EndpointPath, Map[Path, IndexableValue])] = {
    descriptor.outputKeySet.map {
      case (path, desc) =>
        (EndpointPath(endpointId, path), desc.indexes)
    }.toVector
  }
}

class IndexSetDb(endpointDbMgr: EndpointDbMgr) {

  val endpoints = new EndpointIndexDb(endpointDbMgr)
  val dataKeys = new DataKeyIndexDb(endpointDbMgr)
  val outputKeys = new OutputKeyIndexDb(endpointDbMgr)

  def removeTarget(target: ClientSubscriberProxy): Unit = {
    endpoints.removeTarget(target)
    dataKeys.removeTarget(target)
    outputKeys.removeTarget(target)
  }
}