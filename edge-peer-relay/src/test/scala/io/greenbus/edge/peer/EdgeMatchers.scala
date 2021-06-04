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

import io.greenbus.edge.api.{ DataKeyUpdate, EndpointDynamicPath, EndpointPath, IdDataKeyUpdate, IdDynamicDataKeyUpdate, IdOutputKeyUpdate, IdentifiedEdgeUpdate, OutputKeyUpdate, ResolvedValue, SeriesUpdate }
import io.greenbus.edge.data.ValueDouble

object EdgeMatchers {
  import EdgeSubHelpers._

  def idDataKeyResolved(endPath: EndpointPath)(f: DataKeyUpdate => Boolean): PartialFunction[IdentifiedEdgeUpdate, Boolean] = {
    case up: IdDataKeyUpdate =>
      val valueMatched = up.data match {
        case ResolvedValue(v) =>
          v match {
            case v: DataKeyUpdate => f(v)
            case _ => false
          }
        case _ => false
      }

      up.id == endPath && valueMatched
  }
  def idOutputKeyResolved(endPath: EndpointPath)(f: OutputKeyUpdate => Boolean): PartialFunction[IdentifiedEdgeUpdate, Boolean] = {
    case up: IdOutputKeyUpdate =>
      val valueMatched = up.data match {
        case ResolvedValue(v) =>
          v match {
            case v: OutputKeyUpdate => f(v)
            case _ => false
          }
        case _ => false
      }

      up.id == endPath && valueMatched
  }
  def idDynamicDataKeyResolved(endPath: EndpointDynamicPath)(f: DataKeyUpdate => Boolean): PartialFunction[IdentifiedEdgeUpdate, Boolean] = {
    case up: IdDynamicDataKeyUpdate =>
      val valueMatched = up.data match {
        case ResolvedValue(v) =>
          v match {
            case v: DataKeyUpdate => f(v)
            case _ => false
          }
        case _ => false
      }

      up.id == endPath && valueMatched
  }

  def dataKeyResolved(f: DataKeyUpdate => Boolean): PartialFunction[IdentifiedEdgeUpdate, Boolean] = {
    case up: IdDataKeyUpdate =>
      up.data match {
        case ResolvedValue(v) =>
          v match {
            case v: DataKeyUpdate => f(v)
            case _ => false
          }
        case _ => false
      }
  }

  def matchSeriesUpdates(seq: Seq[(Double, Long)]) = {
    seq.map {
      case (v, t) => fixed {
        dataKeyResolved { up: DataKeyUpdate => up.value == SeriesUpdate(ValueDouble(v), t) }
      }
    }
  }
}