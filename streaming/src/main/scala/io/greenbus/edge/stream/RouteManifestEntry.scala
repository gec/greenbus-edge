package io.greenbus.edge.stream

object RouteManifestEntry {
  def toTypeValue(entry: RouteManifestEntry): TypeValue = {
    Int64Val(entry.distance)
  }
  def fromTypeValue(tv: TypeValue): Either[String, RouteManifestEntry] = {
    tv match {
      case Int64Val(v) => Right(RouteManifestEntry(v.toInt))
      case _ => Left("Could not recognize route manifest type: " + tv)
    }
  }
}
case class RouteManifestEntry(distance: Int)