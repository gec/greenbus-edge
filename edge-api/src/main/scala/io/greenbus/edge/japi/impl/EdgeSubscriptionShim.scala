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
package io.greenbus.edge.japi.impl

import java.util
import java.util.function.Consumer

import io.greenbus.edge.api
import io.greenbus.edge.japi
import io.greenbus.edge.japi.{ EdgeSubscription, EdgeSubscriptionClient, IdentifiedEdgeUpdate, SubscriptionParams }
import io.greenbus.edge.japi.flow.Source

import scala.collection.JavaConverters._

class EdgeSubscriptionShim(sub: api.EdgeSubscription) extends japi.EdgeSubscription {
  def updates(): Source[util.List[IdentifiedEdgeUpdate]] = {
    new Source[util.List[IdentifiedEdgeUpdate]] {
      def bind(handler: Consumer[util.List[IdentifiedEdgeUpdate]]): Unit = {
        sub.updates.bind { updates =>
          handler.accept(updates.map(Conversions.convertIdentifiedEdgeUpdateToJava).asJava)
        }
      }
    }
  }

  def close(): Unit = {
    sub.close()
  }
}

class EdgeSubscriptionClientShim(client: api.EdgeSubscriptionClient) extends japi.EdgeSubscriptionClient {
  def subscribe(params: SubscriptionParams): EdgeSubscription = {
    new EdgeSubscriptionShim(client.subscribe(Conversions.convertSubscriptionParamsToScala(params)))
  }
}

class ConsumerServiceShim(services: api.ConsumerService) extends japi.ConsumerService {
  def subscriptionClient(): EdgeSubscriptionClient = {
    new EdgeSubscriptionClientShim(services.subscriptionClient)
  }
}
