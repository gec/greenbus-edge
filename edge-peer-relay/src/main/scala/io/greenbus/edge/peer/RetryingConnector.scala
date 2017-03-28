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

import io.greenbus.edge.amqp.AmqpService
import io.greenbus.edge.amqp.colset.{ ChannelDescriberImpl, ClientResponseParser }
import io.greenbus.edge.colset.client.{ MultiChannelStreamClientImpl, StreamClient }
import io.greenbus.edge.colset.proto.provider.ProtoSerializationProvider
import io.greenbus.edge.colset.{ GatewayProxyChannel, PeerLinkProxyChannel, PeerSessionId }
import io.greenbus.edge.flow.{ Closeable, CloseableComponent, LatchSubscribable }
import io.greenbus.edge.thread.SchedulableCallMarshaller

import scala.concurrent.{ ExecutionContext, Future }

object RetryingConnector {

  case class StreamClientResult(conn: Closeable, client: StreamClient) extends CloseableComponent {
    def onClose: LatchSubscribable = client.onClose

    def close(): Unit = {
      conn.close()
    }
  }

  case class PeerLinkResult(session: PeerSessionId, channel: PeerLinkProxyChannel) extends CloseableComponent {
    def onClose: LatchSubscribable = channel.onClose

    def close(): Unit = channel.close()
  }

  def client(service: AmqpService, host: String, port: Int)(implicit ec: ExecutionContext): Future[StreamClientResult] = {
    service.connect(host, port, 5000).flatMap { clientSource =>

      val describer = new ChannelDescriberImpl
      val responseParser = new ClientResponseParser
      val serialization = new ProtoSerializationProvider

      clientSource.open(describer, responseParser, serialization)
        .map(session => StreamClientResult(clientSource, new MultiChannelStreamClientImpl(session)))
    }
  }
}
class RetryingConnector(eventThread: SchedulableCallMarshaller, service: AmqpService, host: String, port: Int, retryIntervalMs: Long)(implicit ec: ExecutionContext) {
  import RetryingConnector._

  private var sequence: Long = 0
  private var connectionOpt = Option.empty[StreamClientResult]

  private var gatewayMap = Map.empty[GatewayLinkObserver, Option[Retrier[GatewayProxyChannel]]]
  private var peerLinkMap = Map.empty[PeerLinkObserver, Option[Retrier[PeerLinkResult]]]

  private val retrier = new Retrier[StreamClientResult](
    s"[$host:$port]",
    eventThread,
    () => RetryingConnector.client(service, host, port),
    handle,
    retryIntervalMs)

  def start(): Unit = {
    retrier.start()
  }

  def close(): Unit = {
    retrier.close()
  }

  def bindGatewayObserver(obs: GatewayLinkObserver): Closeable = {

    eventThread.marshal {
      gatewayMap.get(obs).foreach(_.foreach(_.close()))
      connectionOpt match {
        case None => gatewayMap += (obs -> None)
        case Some(conn) =>
          val seq = sequence
          gatewayMap += (obs -> Some(gatewayRetrier(seq, obs, conn)))
      }
    }

    new Closeable {
      def close() = eventThread.marshal(unbindGatewayObserver(obs))
    }
  }

  private def unbindGatewayObserver(obs: GatewayLinkObserver): Unit = {
    gatewayMap.get(obs).foreach(_.foreach(_.close()))
    gatewayMap -= obs
  }

  def bindPeerLinkObserver(obs: PeerLinkObserver): Closeable = {

    eventThread.marshal {
      peerLinkMap.get(obs).foreach(_.foreach(_.close()))
      connectionOpt match {
        case None => peerLinkMap += (obs -> None)
        case Some(conn) =>
          val seq = sequence
          peerLinkMap += (obs -> Some(peerLinkRetrier(seq, obs, conn)))
      }
    }

    new Closeable {
      def close() = eventThread.marshal(unbindPeerLinkObserver(obs))
    }
  }

  private def unbindPeerLinkObserver(obs: PeerLinkObserver): Unit = {
    peerLinkMap.get(obs).foreach(_.foreach(_.close()))
    peerLinkMap -= obs
  }

  private def handle(result: StreamClientResult): Unit = {
    val seq = sequence + 1
    sequence = seq
    connectionOpt = Some(result)

    gatewayMap = gatewayMap.map {
      case (obs, currRetrierOpt) =>
        currRetrierOpt.foreach(_.close())
        val nextRetrier = gatewayRetrier(seq, obs, result)
        nextRetrier.start()
        (obs, Some(nextRetrier))
    }

    peerLinkMap = peerLinkMap.map {
      case (obs, currRetrierOpt) =>
        currRetrierOpt.foreach(_.close())
        val nextRetrier = peerLinkRetrier(seq, obs, result)
        nextRetrier.start()
        (obs, Some(nextRetrier))
    }
  }

  private def gatewayRetrier(seq: Long, observer: GatewayLinkObserver, result: StreamClientResult): Retrier[GatewayProxyChannel] = {
    new Retrier[GatewayProxyChannel](
      s"Gateway [$host:$port]",
      eventThread,
      () => result.client.openGatewayChannel(),
      gatewaySuccess(seq, observer, _),
      retryIntervalMs)
  }

  private def gatewaySuccess(seq: Long, observer: GatewayLinkObserver, channel: GatewayProxyChannel): Unit = {
    if (seq == sequence) {
      observer.connected(channel)
    } else {
      channel.close()
    }
  }

  private def peerLinkRetrier(seq: Long, observer: PeerLinkObserver, result: StreamClientResult): Retrier[PeerLinkResult] = {
    new Retrier[PeerLinkResult](
      s"Peer link [$host:$port]",
      eventThread,
      () => result.client.openPeerLinkClient().map { case (sess, ch) => PeerLinkResult(sess, ch) },
      peerLinkSuccess(seq, observer, _),
      retryIntervalMs)
  }

  private def peerLinkSuccess(seq: Long, observer: PeerLinkObserver, channel: PeerLinkResult): Unit = {
    if (seq == sequence) {
      observer.connected(channel.session, channel.channel)
    } else {
      channel.channel.close()
    }
  }

}
