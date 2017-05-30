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
package io.greenbus.edge.stream.gateway3

import io.greenbus.edge.stream.engine2.TargetQueueMgr
import io.greenbus.edge.stream.gateway.RouteServiceRequest
import io.greenbus.edge.stream.{ GatewayProxyChannel, ServiceResponse, TypeValue }
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable

trait GatewayEventHandler {
  def handleEvent(event: ProducerEvent): Unit
}

class GatewayEngine(engineThread: CallMarshaller) extends GatewayEventHandler {

  private val mgr = new ProducerMgr
  private val queueSet = mutable.Map.empty[TargetQueueMgr, GatewayProxyChannel]

  def handleEvent(event: ProducerEvent): Unit = {
    engineThread.marshal(
      mgr.handleEvent(event))
  }

  def connected(channel: GatewayProxyChannel): Unit = {

    val queueMgr = new TargetQueueMgr
    queueSet += (queueMgr -> channel)

    channel.subscriptions.bind { rows =>
      val observers = queueMgr.subscriptionUpdate(rows)
      mgr.targetSubscriptionUpdate(queueMgr, observers)
    }

    channel.requests.bind(requests => {
      val wrapped = requests.map { req =>
        def respond(tv: TypeValue): Unit = {
          val resp = ServiceResponse(req.row, tv, req.correlation)
          channel.responses.push(Seq(resp))
        }

        (req.row.routingKey, RouteServiceRequest(req.row.tableRow, req.value, respond))
      }

      mgr.handleRequests(wrapped)
    })

    channel.onClose.subscribe(() => {
      targetRemoved(queueMgr)
    })
  }

  private def targetRemoved(queueMgr: TargetQueueMgr): Unit = {
    mgr.targetRemoved(queueMgr)
    queueSet -= queueMgr
  }
}
