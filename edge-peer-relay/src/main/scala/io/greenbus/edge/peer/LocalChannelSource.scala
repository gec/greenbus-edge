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
package io.greenbus.edge.peer

import io.greenbus.edge.flow._
import io.greenbus.edge.stream._
import io.greenbus.edge.thread.CallMarshaller

import scala.util.{ Success, Try }

object LocalChannelSource {

  class SubAdapter(client: CallMarshaller, server: CallMarshaller) extends PeerLinkProxyChannel {

    private val subQueue = new ThreadTraversingQueue[Set[RowId]](client, server)
    private val eventQueue = new ThreadTraversingQueue[Seq[StreamEvent]](server, client)
    private val requestsQueue = new ThreadTraversingQueue[Seq[ServiceRequest]](client, server)
    private val responsesQueue = new ThreadTraversingQueue[Seq[ServiceResponse]](server, client)

    def subscriptions: Sink[Set[RowId]] = subQueue
    def events: Source[Seq[StreamEvent]] = eventQueue
    def requests: Sink[Seq[ServiceRequest]] = requestsQueue
    def responses: Source[Seq[ServiceResponse]] = responsesQueue

    def close(): Unit = {}
    def onClose: LatchSubscribable = new NullLatchSubscribable

    val subProxy: SubscriberProxyChannel = new SubscriberProxyChannel {

      def subscriptions: Source[Set[RowId]] = subQueue
      def events: Sink[Seq[StreamEvent]] = eventQueue
      def requests: Source[Seq[ServiceRequest]] = requestsQueue
      def responses: Sink[Seq[ServiceResponse]] = responsesQueue

      def onClose: LatchSubscribable = new NullLatchSubscribable
      def close(): Unit = {}
    }
  }

  class GatewayAdapter(client: CallMarshaller, server: CallMarshaller) extends GatewayProxyChannel {

    private val subQueue = new ThreadTraversingQueue[Set[RowId]](server, client)
    private val eventQueue = new ThreadTraversingQueue[GatewayEvents](client, server)
    private val requestsQueue = new ThreadTraversingQueue[Seq[ServiceRequest]](server, client)
    private val responsesQueue = new ThreadTraversingQueue[Seq[ServiceResponse]](client, server)

    def subscriptions: Source[Set[RowId]] = subQueue

    def events: Sender[GatewayEvents, Boolean] = new Sender[GatewayEvents, Boolean] {
      def send(obj: GatewayEvents, handleResponse: (Try[Boolean]) => Unit): Unit = {
        eventQueue.push(obj)
        client.marshal {
          handleResponse(Success(true))
        }
      }
    }

    def requests: Source[Seq[ServiceRequest]] = requestsQueue
    def responses: Sink[Seq[ServiceResponse]] = responsesQueue

    def close(): Unit = {}
    def onClose: LatchSubscribable = new NullLatchSubscribable

    val gateProxy: GatewayClientProxyChannel = new GatewayClientProxyChannel {

      def subscriptions: Sink[Set[RowId]] = subQueue
      def events: Source[GatewayEvents] = eventQueue
      def requests: Sink[Seq[ServiceRequest]] = requestsQueue
      def responses: Source[Seq[ServiceResponse]] = responsesQueue

      def close(): Unit = {}
      def onClose: LatchSubscribable = new NullLatchSubscribable
    }
  }

  class NullLatchSubscribable extends LatchSubscribable {
    def subscribe(handler: LatchHandler): Closeable = {
      new Closeable {
        def close(): Unit = {}
      }
    }
  }

}
class LocalChannelSource(machine: PeerChannelHandler) {
  import LocalChannelSource._
  def subscribe(client: CallMarshaller, server: CallMarshaller): PeerLinkProxyChannel = {
    val adapter = new SubAdapter(client, server)
    machine.subscriberOpened(adapter.subProxy)
    adapter
  }

  def gateway(client: CallMarshaller, server: CallMarshaller): GatewayProxyChannel = {
    val adapter = new GatewayAdapter(client, server)
    machine.gatewayClientOpened(adapter.gateProxy)
    adapter
  }
}
