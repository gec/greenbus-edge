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
  //protected var ownerMap = Map.empty[]
  //protected var activeSets = Map.empty[Path, (Option[IndexableValue], Set[A])]
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

  //protected def buildIndex(specifier: IndexSpecifier): Set[A]

  //def observe(id: EndpointId, desc: EndpointDescriptor): Seq[IndexUpdate[A]]

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
    /*activeSets.get(specifier.key) match {
      case None =>
        val built = buildIndex(specifier)
        activeSets += (specifier.key -> (specifier.valueOpt, built))
        built
      case Some((vOpt, set)) => set
    }*/
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

    /*val elemToIndexMaps: Seq[(A, Map[Path, IndexableValue])] = endpointDbMgr.map.flatMap {
      case (endId, db) => db.currentInfo().map(descRec => identifyIndexSet(endId, descRec.descriptor))
    }.flatten.toVector

    elemToIndexMaps.for*/
    //identifyIndexSet()

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

    /*val elemAndIndexMaps = identifyIndexSet(id, desc)

    val matchBuilder = Vector.newBuilder[(IndexSpecifier, A)]

    elemAndIndexMaps.foreach {
      case (elem, indexes) => {

        indexes.filterKeys(activePaths.contains).foreach {
          case (path, value) =>
            if (activeMap.contains(IndexSpecifier(path, Some(value)))) {
              matchBuilder += (IndexSpecifier(path, Some(value)) -> elem)
            }
            if (activeMap.contains(IndexSpecifier(path, None))) {
              matchBuilder += (IndexSpecifier(path, None) -> elem)
            }
        }
      }
    }


    val matchMap = {
      val all = matchBuilder.result()
      var map = Map.empty[IndexSpecifier, Set[A]]
      all.foreach {
        case (spec, elem) =>
          map.get(spec) match {
            case None => map += (spec -> Set(elem))
            case Some(set) => map += (spec -> (set + elem))
          }
      }
      map
    }

    matchMap.foreach {
      case (spec, elemSet) =>
        elemSet.foreach { elem =>


        }
    }*/
    /*

  private var activeSets = Map.empty[IndexSpecifier, Set[A]]
  private var elemMap = Map.empty[A, Set[IndexSpecifier]]
     */

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
    //var specAdds = Map.empty[IndexSpecifier, Set[A]]
    //var specRemoves = Map.empty[IndexSpecifier, Set[A]]

    matchMap.foreach {
      case (elem, matched) =>
        val current = elementMap.getOrElse(elem, Set())
        val added = matched -- current
        val removed = current -- matched

        /*adds ++= added.map(spec => (elem, spec)).toVector
        removes ++= removed.map(spec => (elem, spec)).toVector*/

        added.foreach { spec =>
          /*specAdds.get(spec) match {
            case None => specAdds += (spec -> Set(elem))
            case Some(set) => specAdds += (spec -> (set + elem))
          }*/
          specAddsBuilder += (spec -> elem)
          adds += (elem -> spec)
        }

        removed.foreach { spec =>
          /*specRemoves.get(spec) match {
            case None => specRemoves += (spec -> Set(elem))
            case Some(set) => specRemoves += (spec -> (set + elem))
          }*/
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

  /*protected def buildIndex(specifier: IndexSpecifier): Set[EndpointId] = {
    val key = specifier.key
    val vOpt = specifier.valueOpt

    /*val setBuilder = Set.newBuilder[EndpointId]
    endpointDbMgr.map.foreach {
      case (endId, db) => db.currentInfo().foreach { descRec =>
        descRec.descriptor.indexes

      }
    }*/

    val set = endpointDbMgr.map.flatMap {
      case (endId, db) => db.currentInfo().flatMap { descRec =>
        descRec.descriptor.indexes.get(key).flatMap { v =>
          if (vOpt.isEmpty || vOpt.contains(v)) {
            Some(endId)
          } else {
            None
          }
        }
      }
    }.toSet

    set.foreach(s => addToIndex(s, specifier))
    set
  }*/

  /*def observe(id: EndpointId, desc: EndpointDescriptor): Seq[IndexUpdate[EndpointId]] = {

    val indexes = desc.indexes

    val matchBuilder = Set.newBuilder[IndexSpecifier]

    indexes.filterKeys(activePaths.contains).foreach {
      case (path, value) =>
        if (activeMap.contains(IndexSpecifier(path, Some(value)))) {
          matchBuilder += IndexSpecifier(path, Some(value))
        }
        if (activeMap.contains(IndexSpecifier(path, None))) {
          matchBuilder += IndexSpecifier(path, None)
        }
    }

    val matched = matchBuilder.result()

    val current = elementMap.getOrElse(id, Set())

    val added = matched -- current
    val removed = current -- matched

    val addResults = added.toVector.map { spec =>
      IndexUpdate(spec, Set(id), Set(), targetsFor(spec))
    }
    val removeResults = removed.toVector.map { spec =>
      IndexUpdate(spec, Set(), Set(id), targetsFor(spec))
    }

    added.foreach(spec => addToIndex(id, spec))
    removed.foreach(spec => removeFromIndex(id, spec))

    addResults ++ removeResults
  }*/
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

  /*private var activeEndpointIndexes = Map.empty[IndexSpecifier, Set[EndpointId]]
  private var endpointTargets = Map.empty[IndexSpecifier, Set[ClientSubscriberProxy]]
  private var targetToEndpoints = Map.empty[ClientSubscriberProxy, Set[IndexSpecifier]]*/

  val endpoints = new EndpointIndexDb(endpointDbMgr)
  val dataKeys = new DataKeyIndexDb(endpointDbMgr)
  val outputKeys = new OutputKeyIndexDb(endpointDbMgr)

  /*def add(subscription: IndexSubscription): Set[EndpointId] = {
    subscription match {
      case sub: EndpointIndexSubscription => {
        endpointDb.addSubscription(sub.specifier, sub.target)
      }
      case sub: DataIndexSubscription => {
        dataDb.addSubscription(sub.specifier, sub.target)
      }
      case sub: OutputIndexSubscription => {
        outputDb.addSubscription(sub.specifier, sub.target)
      }
    }

  }*/
  /*def remove(subscription: IndexSubscription): Unit = {
    subscription match {
      case sub: EndpointIndexSubscription => {
        endpointDb.removeTarget(sub.specifier, sub.target)

      }
    }
  }*/
  def removeTarget(target: ClientSubscriberProxy): Unit = {
    endpoints.removeTarget(target)
    dataKeys.removeTarget(target)
    outputKeys.removeTarget(target)
  }
}