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
package io.greenbus.edge.stream.peer

import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.flow._
import io.greenbus.edge.stream._
import io.greenbus.edge.stream.consume._
import io.greenbus.edge.stream.engine._
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.mutable

trait StreamUserSubscription {
  def events: Source[Seq[RowUpdate]]
  def close(): Unit
}

class StreamSubHandle(synth: UserSubscriptionSynth, dist: Source[Seq[StreamEvent]], onClose: () => Unit) extends StreamUserSubscription {
  def events: Source[Seq[RowUpdate]] = {
    (handler: Handler[Seq[RowUpdate]]) =>
      {
        dist.bind { events =>
          val rowUpdates = synth.handle(events)
          handler.handle(rowUpdates)
        }
      }
  }

  def close(): Unit = {
    onClose()
  }
}

class StreamSub(queue: TargetQueueMgr, dist: Sink[Seq[StreamEvent]]) {
  def flush(): Unit = {
    val events = queue.dequeue()
    if (events.nonEmpty) {
      dist.push(events)
    }
  }
}

class StreamPeer(id: String, sessionId: PeerSessionId, engineThread: CallMarshaller, remoteIo: Boolean = true, appendLimitDefault: Int) extends LazyLogging {
  private val streamEngine = new StreamEngine(id, sessionId, appendLimitDefault)
  private val serviceEngine = new ServiceEngine(id, streamEngine.getSourcing)

  private val channelManager = new GenericChannelEngine(streamEngine, serviceEngine, eventNotify)
  private val handler = new PeerChannelManager(channelManager, if (remoteIo) Some(engineThread) else None)
  private val userHandles = mutable.Set.empty[StreamSub]
  private val servClient = new ConsumerServiceClient(serviceEngine, engineThread)

  def session: PeerSessionId = sessionId

  private def eventNotify(): Unit = {
    logger.debug(s"eventNotify()")
    userHandles.foreach(_.flush())
    channelManager.flush()
  }

  def connectRemotePeer(sessionId: PeerSessionId, channel: PeerLinkProxyChannel): Unit = {
    engineThread.marshal {
      handler.peerOpened(sessionId, channel)
    }
  }

  def channelHandler: PeerChannelHandler = handler

  def serviceClient: StreamServiceClient = servClient

  def subscribe(rows: Set[RowId]): StreamUserSubscription = {
    val target = new TargetQueueMgr
    val dist = new RemoteBoundQueuedDistributor[Seq[StreamEvent]](engineThread)
    val sub = new StreamSub(target, dist)

    engineThread.marshal {
      val observers = target.subscriptionUpdate(rows)

      streamEngine.targetSubscriptionUpdate(target, observers)
      logger.debug(s"Sub.flush()")
      sub.flush()

      userHandles += sub
    }

    def onClose(): Unit = {
      engineThread.marshal {
        userHandles -= sub
        streamEngine.targetRemoved(target)
      }
    }

    new StreamSubHandle(UserSubscriptionSynth.build(rows), dist, onClose)
  }
}
