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
package io.greenbus.edge.api

/*sealed trait EndpointId
case class NamedEndpointId(name: Path) extends EndpointId
case class UuidEndpointId(uuid: UUID, display: Path) extends EndpointId*/

case class EndpointId(path: Path)

case class EndpointPath(endpoint: EndpointId, key: Path)

object Path {
  def isPrefixOf(l: Path, r: Path): Boolean = {
    val lIter = l.parts.iterator
    val rIter = r.parts.iterator
    var okay = true
    var continue = true
    while (continue) {
      (lIter.hasNext, rIter.hasNext) match {
        case (true, true) =>
          if (lIter.next() != rIter.next()) {
            okay = false
            continue = false
          }
        case (false, true) => continue = false
        case (true, false) =>
          okay = false; continue = false
        case (false, false) => continue = false
      }
    }
    okay
  }

  def apply(part: String): Path = {
    Path(Seq(part))
  }
}
case class Path(parts: Seq[String])