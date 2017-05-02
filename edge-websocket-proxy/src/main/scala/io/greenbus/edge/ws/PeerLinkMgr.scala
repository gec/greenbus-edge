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
package io.greenbus.edge.ws

import akka.actor.{ Actor, ActorRef, PoisonPill, Props }
import com.google.protobuf.util.JsonFormat
import com.typesafe.scalalogging.LazyLogging
import io.greenbus.edge.api._
import io.greenbus.edge.api.consumer.proto.convert.ConsumerConversions
import io.greenbus.edge.api.consumer.proto.{ ClientOutputRequest, ClientToServerMessage, EdgeUpdateSet, ServerToClientMessage }
import io.greenbus.edge.api.proto.convert.{ Conversions, OutputConversions }
import io.greenbus.edge.peer.ConsumerServices
import io.greenbus.edge.thread.CallMarshaller

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success, Try }

object PeerLinkMgr {

  case class SocketConnected(socket: Socket)
  case class SocketDisconnected(socket: Socket)
  case class SocketMessage(text: String, socket: Socket)

  def props(services: ConsumerServices): Props = {
    Props(classOf[PeerLinkMgr], services)
  }
}
class PeerLinkMgr(services: ConsumerServices) extends Actor with LazyLogging {
  import PeerLinkMgr._

  private var linkMap = Map.empty[Socket, ActorRef]

  def receive = {
    case SocketConnected(sock) => {
      logger.info("Got socket connected " + sock)
      val linkActor = context.actorOf(PeerLink.props(sock, services))
      linkMap += (sock -> linkActor)
    }
    case SocketDisconnected(sock) => {
      logger.info("Got socket disconnected " + sock)
      linkMap.get(sock).foreach { ref =>
        ref ! PoisonPill
        linkMap -= sock
      }
    }
    case SocketMessage(text, sock) => {
      linkMap.get(sock).foreach(_ ! PeerLink.FromSocket(text))
    }
  }
}

class PeerSubMgr(events: CallMarshaller, subClient: EdgeSubscriptionClient, socket: Socket, printer: JsonFormat.Printer)(implicit e: ExecutionContext) extends LazyLogging {

  private var paramsMap = Map.empty[Long, SubscriptionParams]
  private var subsMap = Map.empty[Long, EdgeSubscription]

  private def doSubscription(key: Long, params: SubscriptionParams): Unit = {
    val sub = subClient.subscribe(params)
    subsMap += (key -> sub)
    sub.updates.bind { not =>
      try {
        val b = ServerToClientMessage.newBuilder()

        val setBuilder = EdgeUpdateSet.newBuilder()
        not.map(ConsumerConversions.toProto).foreach(setBuilder.addUpdates)
        b.putSubscriptionNotification(key, setBuilder.build())
        //println(setBuilder.build())
        val msg = b.build()

        val json = printer.print(msg)
        socket.send(json)
      } catch {
        case ex: Throwable =>
          logger.error("Problem writing proto message: " + ex)
      }
    }
  }

  def add(key: Long, params: SubscriptionParams): Unit = {
    paramsMap += (key -> params)
    subsMap.get(key).foreach(_.close())
    doSubscription(key, params)
  }
  def remove(key: Long): Unit = {
    paramsMap -= key
    subsMap.get(key).foreach(_.close())
    subsMap -= key
  }

  def close(): Unit = {
    subsMap.values.foreach(_.close())
  }

}

class PeerOutputMgr(events: CallMarshaller, serviceClient: ServiceClient, socket: Socket, printer: JsonFormat.Printer)(implicit e: ExecutionContext) extends LazyLogging {

  private def respond(corr: Long, result: Try[OutputResult]): Unit = {
    try {
      val res = result match {
        case Success(r) => r
        case Failure(ex) => OutputFailure(ex.getMessage)
      }

      val msg = ServerToClientMessage.newBuilder()
        .putOutputResponses(corr, OutputConversions.toProto(res))
        .build()

      val json = printer.print(msg)
      socket.send(json)
    } catch {
      case ex: Throwable =>
        logger.error("Problem writing proto message: " + ex)
    }
  }

  def doRequest(request: ClientOutputRequest): Unit = {
    logger.debug("Issuing: " + request)

    if (request.hasId && request.hasRequest) {

      val reqOpt = for {
        id <- Conversions.fromProto(request.getId)
        params <- OutputConversions.fromProto(request.getRequest)
      } yield {
        (id, params)
      }

      reqOpt match {
        case Left(err) =>
        case Right((id, params)) =>
          val corr = request.getCorrelation

          serviceClient.send(OutputRequest(id, params), respond(corr, _))
      }
    } else {
      logger.warn(s"Request lacked id or params")
    }
  }
}

object PeerLink {

  case class FromSocket(text: String)

  def props(socket: Socket, services: ConsumerServices): Props = {
    Props(classOf[PeerLink], socket, services)
  }
}
class PeerLink(socket: Socket, services: ConsumerServices) extends Actor with CallMarshalActor with LazyLogging {
  import PeerLink._
  import context.dispatcher

  private val printer = JsonFormat.printer()
  private val subMgr = new PeerSubMgr(this.marshaller, services.subscriptionClient, socket, printer)
  private var outputMgr = new PeerOutputMgr(this.marshaller, services.queuingServiceClient, socket, printer)

  private val parser = JsonFormat.parser()

  def receive = {
    case FromSocket(text) => {
      logger.debug("Got socket text: " + text + ", " + socket)

      val b = ClientToServerMessage.newBuilder()
      try {
        parser.merge(text, b)
        val proto = b.build()
        //println(proto)

        proto.getSubscriptionsRemovedList.foreach(k => subMgr.remove(k))

        proto.getSubscriptionsAddedMap.foreach {
          case (key, paramsProto) =>
            ConsumerConversions.fromProto(paramsProto) match {
              case Left(err) => logger.error("Could not parse params proto: " + err)
              case Right(obj) => subMgr.add(key, obj)
            }
        }

        proto.getOutputRequestsList.foreach(outputMgr.doRequest)

      } catch {
        case ex: Throwable =>
          logger.warn("Error parsing json: " + ex)
      }
    }
    case MarshalledCall(f) => f()
  }

  override def postStop(): Unit = {
    subMgr.close()
  }
}

trait CallMarshalActor {
  self: Actor =>

  private val actorSelf = this.self

  case class MarshalledCall(f: () => Unit)

  protected def marshaller: CallMarshaller = new CallMarshaller {
    def marshal(f: => Unit) = actorSelf ! MarshalledCall(() => f)
  }
}

class GuiSocketMgr(mgr: ActorRef) extends SocketMgr with LazyLogging {
  def connected(socket: Socket): Unit = {
    mgr ! PeerLinkMgr.SocketConnected(socket)
  }

  def disconnected(socket: Socket): Unit = {
    mgr ! PeerLinkMgr.SocketDisconnected(socket)
  }

  def handle(text: String, socket: Socket): Unit = {
    mgr ! PeerLinkMgr.SocketMessage(text, socket)
  }
}
