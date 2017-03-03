package io.greenbus.edge.collection

object BiMultiMap {

  def apply[A, B](args: (A, B)*): BiMultiMap[A, B] = {
    val kToV = MapSetBuilder.build[A, B]
    val vToK = MapSetBuilder.build[B, A]
    args.foreach {
      case (k, v) =>
        kToV += (k -> v)
        vToK += (v -> k)
    }
    new BiMultiMap[A, B](kToV.result(), vToK.result())
  }

  def empty[A, B] = {
    new BiMultiMap[A, B](Map.empty[A, Set[B]], Map.empty[B, Set[A]])
  }
}
class BiMultiMap[A, B](val keyToVal: Map[A, Set[B]], val valToKey: Map[B, Set[A]]) {
  //private var keyToVal = Map.empty[A, Set[B]]
  //private var valToKey = Map.empty[B, Set[A]]

  def get(key: A): Option[Set[B]] = {
    keyToVal.get(key)
  }

  def +(tup: (A, B)): BiMultiMap[A, B] = {
    add(tup._1, tup._2)
  }
  /*def ++(tup: Seq[(A, B)]): ValueTrackedSetMap[A, B] = {
    //add(tup._1, tup._2)
  }*/

  def reverseAdd(v: B, keys: Set[A]): BiMultiMap[A, B] = {

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

    new BiMultiMap[A, B](kToV, vToK)
  }

  def add(key: A, v: B): BiMultiMap[A, B] = {
    val kToV = keyToVal.get(key) match {
      case None => keyToVal.updated(key, Set(v))
      case Some(set) => keyToVal.updated(key, set + v)
    }
    val vToK = valToKey.get(v) match {
      case None => valToKey.updated(v, Set(key))
      case Some(set) => valToKey.updated(v, set + key)
    }

    new BiMultiMap[A, B](kToV, vToK)
  }

  def remove(v: B): BiMultiMap[A, B] = {
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

        new BiMultiMap[A, B](kToV, valToKey - v)
      }
    }
  }

}
