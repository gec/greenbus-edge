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

case class ServiceRequest(row: RowId, value: TypeValue, correlation: TypeValue)
case class ServiceResponse(row: RowId, value: TypeValue, correlation: TypeValue)

trait ServiceIssuer {
  def handleResponses(responses: Seq[ServiceResponse]): Unit
}

class PeerServiceEngine(logId: String, routing: RoutingManager) extends LazyLogging {

  private val correlator = new Correlator[(ServiceIssuer, TypeValue)]

  def requestsIssued(issuer: ServiceIssuer, requests: Seq[ServiceRequest]): Unit = {
    requests.groupBy(_.row.routingKey).foreach {
      case (route, routeRequests) =>
        routing.routeToSourcing.get(route) match {
          case None =>
          case Some(sourcing) =>
            if (sourcing.serviceTargets.nonEmpty) {

              val registered = routeRequests.map { req =>
                val issuerCorrelation = req.correlation
                val ourCorrelation = correlator.add((issuer, issuerCorrelation))
                ServiceRequest(req.row, req.value, UInt64Val(ourCorrelation))
              }

              sourcing.serviceTargets.foreach(_.issueServiceRequests(registered))

            } else {
              logger.debug(s"$logId saw requests for route $route that was unavailable")
            }
        }
    }
  }

  def handleResponses(responses: Seq[ServiceResponse]): Unit = {
    val correlated = responses.flatMap { resp =>
      resp.correlation match {
        case UInt64Val(ourCorrelation) => {
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
