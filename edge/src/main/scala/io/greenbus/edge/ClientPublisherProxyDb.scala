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

trait ClientPublisherProxyDb {
  def getForEndpointId(id: EndpointId): Seq[ClientPublisherProxy]
  def get(session: SessionId, id: EndpointId): Option[ClientPublisherProxy]
  def add(session: SessionId, id: EndpointId, proxy: ClientPublisherProxy): Unit
  def remove(proxy: ClientPublisherProxy): Unit
}

class ClientPublisherProxyDbImpl extends ClientPublisherProxyDb {

  private var map = Map.empty[EndpointId, Map[SessionId, ClientPublisherProxy]]
  private var reverse = Map.empty[ClientPublisherProxy, (EndpointId, SessionId)]

  def getForEndpointId(id: EndpointId): Seq[ClientPublisherProxy] = {
    map.get(id).map(_.values.toVector).getOrElse(Seq())
  }

  def get(session: SessionId, id: EndpointId): Option[ClientPublisherProxy] = {
    for { sessMap <- map.get(id); proxy <- sessMap.get(session) } yield proxy
  }

  def add(session: SessionId, id: EndpointId, proxy: ClientPublisherProxy): Unit = {
    remove(proxy)
    reverse += (proxy -> (id, session))
    map.get(id) match {
      case None => map += (id -> Map(session -> proxy))
      case Some(sessMap) => map += (id -> (sessMap + (session -> proxy)))
    }
  }

  def remove(proxy: ClientPublisherProxy): Unit = {
    reverse.get(proxy).foreach {
      case (id, session) =>
        map.get(id).foreach { sessMap =>
          val removed = sessMap - session
          if (removed.nonEmpty) {
            map += (id -> removed)
          } else {
            map -= id
          }
        }
    }
  }
}