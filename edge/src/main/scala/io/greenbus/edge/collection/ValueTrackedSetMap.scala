package io.greenbus.edge.collection

object ValueTrackedSetMap {

  def empty[A, B] = {
    new ValueTrackedSetMap[A, B](Map.empty[A, Set[B]], Map.empty[B, Set[A]])
  }
}
class ValueTrackedSetMap[A, B](val keyToVal: Map[A, Set[B]], val valToKey: Map[B, Set[A]]) {
  //private var keyToVal = Map.empty[A, Set[B]]
  //private var valToKey = Map.empty[B, Set[A]]

  def get(key: A): Option[Set[B]] = {
    keyToVal.get(key)
  }

  def +(tup: (A, B)): ValueTrackedSetMap[A, B] = {
    add(tup._1, tup._2)
  }
  /*def ++(tup: Seq[(A, B)]): ValueTrackedSetMap[A, B] = {
    //add(tup._1, tup._2)
  }*/

  def reverseAdd(v: B, keys: Set[A]): ValueTrackedSetMap[A, B] = {

    val updates = Vector.newBuilder[(A, Set[B])]
    keys.foreach { key =>
      keyToVal.get(key) match {
        case None => updates += (key -> Set(v))
        case Some(set) => updates += (key -> (set + v))
      }
    }

    val kToV = keyToVal ++ updates.result()

    val vToK = valToKey.get(v) match {
      case None => valToKey.updated(v, keys)
      case Some(set) => valToKey.updated(v, set ++ keys)
    }

    new ValueTrackedSetMap[A, B](kToV, vToK)
  }

  def add(key: A, v: B): ValueTrackedSetMap[A, B] = {
    val kToV = keyToVal.get(key) match {
      case None => keyToVal.updated(key, Set(v))
      case Some(set) => keyToVal.updated(key, set + v)
    }
    val vToK = valToKey.get(v) match {
      case None => valToKey.updated(v, Set(key))
      case Some(set) => valToKey.updated(v, set + key)
    }

    new ValueTrackedSetMap[A, B](kToV, vToK)
  }

  def remove(v: B): ValueTrackedSetMap[A, B] = {
    valToKey.get(v) match {
      case None => this
      case Some(keySet) => {
        val removes = Vector.newBuilder[A]
        val updates = Vector.newBuilder[(A, Set[B])]

        keySet.foreach { key =>
          keyToVal.get(key).foreach { set =>
            val result = set - v
            if (result.nonEmpty) {
              updates += (key -> result)
            } else {
              removes += key
            }
          }
        }

        val kToV = (keyToVal -- removes.result()) ++ updates.result()

        new ValueTrackedSetMap[A, B](kToV, valToKey - v)
      }
    }
  }

}
