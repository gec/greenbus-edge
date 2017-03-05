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
