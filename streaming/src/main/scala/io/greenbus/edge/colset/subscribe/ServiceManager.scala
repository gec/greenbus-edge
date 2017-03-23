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
package io.greenbus.edge.colset.subscribe

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.colset._
import io.greenbus.edge.flow._
import io.greenbus.edge.thread.CallMarshaller

import scala.util.{ Success, Try }

case class UserServiceRequest(row: RowId, value: TypeValue)
case class UserServiceResponse(row: RowId, value: TypeValue)

trait StreamServiceClient extends Sender[UserServiceRequest, UserServiceResponse] with CloseObservable

class StreamServiceClientImpl(proxy: PeerLinkProxyChannel, eventThread: CallMarshaller) extends StreamServiceClient with LazyLogging {

  private val correlator = new Correlator[(Try[UserServiceResponse]) => Unit]

  proxy.responses.bind(resps => handleResponses(resps))

  def send(obj: UserServiceRequest, handleResponse: (Try[UserServiceResponse]) => Unit): Unit = {
    eventThread.marshal {
      val correlation = correlator.add(handleResponse)
      proxy.requests.push(Seq(ServiceRequest(obj.row, obj.value, Int64Val(correlation))))
    }
  }

  private def handleResponses(responses: Seq[ServiceResponse]): Unit = {
    eventThread.marshal {
      responses.foreach { response =>
        response.correlation match {
          case Int64Val(ourCorrelation) => {
            correlator.pop(ourCorrelation) match {
              case Some(handler) => handler(Success(UserServiceResponse(response.row, response.value)))
              case None =>
                logger.debug(s"Saw missing correlation id for ${response.row}")
            }
          }
          case _ =>
            logger.warn(s"Saw unhandled correlation type: ${response.correlation}")
        }
      }
    }
  }

  def onClose: LatchSubscribable = proxy.onClose
}

/*
trait ServiceManager {
  def connected(proxy: PeerLinkProxyChannel): Unit
}

class ServiceManagerImpl(eventThread: CallMarshaller) {

  private var connectionOpt = Option.empty[PeerLinkProxy]

  def connected(proxy: PeerLinkProxyChannel): Unit = {
    eventThread.marshal {
      connectionOpt = Some(proxy)
    }
    proxy.onClose.subscribe(() => eventThread.marshal { disconnected() })
    proxy.responses.bind(resps => eventThread.marshal { handleResponses(resps) })
  }

  /*def request(request: ServiceRequest): Unit = {
    request.
  }*/

  protected def handleResponses(responses: Seq[ServiceResponse]): Unit = {

  }

  private def disconnected(): Unit = {
    connectionOpt = None
  }
}
*/
