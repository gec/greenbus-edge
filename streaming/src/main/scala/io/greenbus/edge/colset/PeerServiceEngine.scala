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
package io.greenbus.edge.colset

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

class PeerServiceEngine(logId: String, routing: RoutingManager) extends ServiceIssuer with LazyLogging {

  private val correlator = new KeyedCorrelator[(ServiceIssuer, TypeValue), ServiceIssuer]

  def requestsIssued(issuer: ServiceIssuer, requests: Seq[ServiceRequest]): Unit = {
    logger.trace(s"Requests issued: $requests")
    requests.groupBy(_.row.routingKey).foreach {
      case (route, routeRequests) =>
        routing.routeToSourcing.get(route) match {
          case None =>
          case Some(sourcing) =>
            if (sourcing.serviceTargets.nonEmpty) {

              val registered = routeRequests.map { req =>
                val issuerCorrelation = req.correlation
                val ourCorrelation = correlator.add(issuer, (issuer, issuerCorrelation))
                ServiceRequest(req.row, req.value, Int64Val(ourCorrelation))
              }

              sourcing.serviceTargets.foreach(_.issueServiceRequests(registered))

            } else {
              logger.debug(s"$logId saw requests for route $route that was unavailable")
            }
        }
    }
  }

  def handleResponses(responses: Seq[ServiceResponse]): Unit = {
    logger.trace(s"Responses received: $responses")
    val correlated = responses.flatMap { resp =>
      resp.correlation match {
        case Int64Val(ourCorrelation) => {
          correlator.pop(ourCorrelation) match {
            case Some((issuer, corr)) => Some((issuer, corr, resp))
            case None =>
              logger.debug(s"$logId saw missing correlation id for ${resp.row}")
              None
          }
        }
        case _ =>
          logger.warn(s"$logId saw unhandled correlation type: ${resp.correlation}")
          None
      }
    }

    correlated.groupBy(_._1).foreach {
      case (issuer, correlatedResps) => {
        val mapped = correlatedResps.map {
          case (_, issuerCorrelation, response) => ServiceResponse(response.row, response.value, issuerCorrelation)
        }
        issuer.handleResponses(mapped)
      }
    }
  }

  def issuerClosed(issuer: ServiceIssuer): Unit = {
    correlator.remove(issuer)
  }
}

class KeyedCorrelator[A, K] {

  private var sequence: Long = 0
  private val correlationMap = mutable.LongMap.empty[(A, K)]
  private val keyMap = mutable.Map.empty[K, mutable.Set[Long]]

  def add(key: K, obj: A): Long = {
    val next = sequence
    sequence += 1

    correlationMap += (next -> (obj, key))
    val keySet = keyMap.getOrElseUpdate(key, mutable.Set.empty[Long])
    keySet += next

    next
  }

  def pop(correlator: Long): Option[A] = {
    val resultOpt = correlationMap.get(correlator)
    correlationMap -= correlator

    resultOpt.foreach {
      case (_, key) =>
        keyMap.get(key).foreach { keySet =>
          keySet -= correlator
          if (keySet.isEmpty) {
            keyMap -= key
          }
        }
    }

    resultOpt.map(_._1)
  }

  def remove(key: K): Unit = {
    keyMap.get(key).foreach { keySet =>
      keySet.foreach { dead =>
        correlationMap -= dead
      }
      keyMap -= key
    }
  }
}

class Correlator[A] {

  private var sequence: Long = 0
  private val correlationMap = mutable.LongMap.empty[A]

  def add(obj: A): Long = {
    val next = sequence
    sequence += 1

    correlationMap += (next -> obj)

    next
  }

  def pop(correlator: Long): Option[A] = {
    val result = correlationMap.get(correlator)
    correlationMap -= correlator
    result
  }
}