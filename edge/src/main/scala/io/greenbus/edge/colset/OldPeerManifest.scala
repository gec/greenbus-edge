package io.greenbus.edge.colset

import io.greenbus.edge.collection.BiMultiMap


object PeerManifest {

  def eitherSomeIsRightOrNone[L, R](vOpt: Option[Either[L, R]]): Either[L, Option[R]] = {
    vOpt.map(_.map(r => Some(r))).getOrElse(Right(Option.empty[R]))
  }

  def parseIndexableTypeValue(tv: TypeValue): Either[String, IndexableTypeValue] = {
    tv match {
      case v: IndexableTypeValue => Right(v)
      case _ => Left(s"Unrecognized indexable type value: $tv")
    }
  }

  def parseIndexSpecifier(tv: TypeValue): Either[String, IndexSpecifier] = {
    tv match {
      case TupleVal(seq) =>
        if (seq.size >= 2) {
          val key = seq(0)
          seq(1) match {
            case v: OptionVal =>
              eitherSomeIsRightOrNone(v.element.map(parseIndexableTypeValue))
                .map(vOpt => IndexSpecifier(key, vOpt))
            case _ => Left(s"Unrecognized index type: $tv")
          }
        } else {
          Left(s"Unrecognized index type: $tv")
        }
      case _ => Left(s"Unrecognized index type: $tv")
    }
  }
}
case class PeerManifest(routingKeySet: Set[TypeValue], indexSet: Set[IndexSpecifier])


class SourceLinksManifest[Link] {

  private var routingMap = BiMultiMap.empty[TypeValue, Link]
  private var indexMap = BiMultiMap.empty[IndexSpecifier, Link]

  def routingKeys: Map[TypeValue, Set[Link]] = routingMap.keyToVal
  def indexes: Map[IndexSpecifier, Set[Link]] = indexMap.keyToVal

  def handleUpdate(link: Link, manifest: PeerManifest): Unit = {
    routingMap = routingMap.reverseAdd(link, manifest.routingKeySet)
    indexMap = indexMap.reverseAdd(link, manifest.indexSet)
  }

  def linkRemoved(link: Link): Unit = {
    routingMap = routingMap.removeValue(link)
    indexMap = indexMap.removeValue(link)
  }
}

class LocalManifest[Publisher] {

}