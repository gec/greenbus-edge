package io.greenbus.edge.collection

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