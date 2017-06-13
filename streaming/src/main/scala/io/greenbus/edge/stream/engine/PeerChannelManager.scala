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
package io.greenbus.edge.stream.engine

import io.greenbus.edge.flow._
import io.greenbus.edge.stream._
import io.greenbus.edge.thread.CallMarshaller

class PeerChannelManager(generic: GenericChannelEngine, marshalOpt: Option[CallMarshaller]) extends PeerChannelHandler {
  def peerOpened(peerSessionId: PeerSessionId, proxy: PeerLinkProxyChannel): Unit = {
    val channel = new PeerLinkSourceChannel(peerSessionId, proxy)

    val wrapped = marshalOpt match {
      case None =>
        channel.init()
        channel
      case Some(eventThread) => {
        eventThread.marshal {
          channel.init()
        }
        new MarshaledGenericSource(channel, eventThread)
      }
    }

    generic.sourceChannel(wrapped)
  }

  def subscriberOpened(proxy: SubscriberProxyChannel): Unit = {
    val target = new SubscriberProxyTarget(proxy)
    generic.targetChannel(target)
  }

  def gatewayClientOpened(clientProxy: GatewayClientProxyChannel): Unit = {
    val channel = new GatewaySourceChannel(clientProxy)
    generic.sourceChannel(channel)
  }
}

class SubscriberProxyTarget(proxy: SubscriberProxyChannel) extends GenericTargetChannel {
  def onClose: LatchSubscribable = proxy.onClose

  def subscriptions: Source[Set[RowId]] = proxy.subscriptions

  def events: Sink[Seq[StreamEvent]] = proxy.events

  def requests: Source[Seq[ServiceRequest]] = proxy.requests

  def responses: Sink[Seq[ServiceResponse]] = proxy.responses
}

class MarshaledSource[A](eventThread: CallMarshaller, source: Source[A]) extends Source[A] {
  def bind(handler: Handler[A]): Unit = {
    source.bind(obj => eventThread.marshal {
      handler.handle(obj)
    })
  }
}

class MarshaledGenericSource(source: GenericSourceChannel, eventThread: CallMarshaller) extends GenericSourceChannel {

  def onClose: LatchSubscribable = {
    new LatchSubscribable {
      def subscribe(handler: LatchHandler): Closeable = {
        val sub = source.onClose.subscribe { () =>
          eventThread.marshal { handler.handle() }
        }
        new Closeable {
          def close(): Unit = sub.close()
        }
      }
    }
  }

  def subscriptions: Sink[Set[RowId]] = source.subscriptions

  def events: Source[SourceEvents] = new MarshaledSource[SourceEvents](eventThread, source.events)

  def requests: Sink[Seq[ServiceRequest]] = source.requests

  def responses: Source[Seq[ServiceResponse]] = new MarshaledSource[Seq[ServiceResponse]](eventThread, source.responses)
}
