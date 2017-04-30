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

import io.greenbus.edge.api.{ EndpointId, EndpointPath, Path }
import io.greenbus.edge.api.stream.KeyMetadata
import io.greenbus.edge.data.{ ValueDouble, ValueString }

object TestModel {

  class Producer1(services: EdgeServices) {

    val endpointId = EndpointId(Path("my-endpoint"))
    val builder = services.producer.endpointBuilder(endpointId)

    val dataKey = Path("series-double-1")
    val endDataKey = EndpointPath(endpointId, dataKey)

    val series1 = builder.seriesValue(Path("series-double-1"), KeyMetadata(indexes = Map(Path("index1") -> ValueString("value 1"))))
    val buffer = builder.build(seriesBuffersSize = 100, eventBuffersSize = 100)

    def updateAndFlush(v: Double, time: Long): Unit = {
      series1.update(ValueDouble(v), time)
      buffer.flush()
    }

    def close(): Unit = {
      buffer.close()
    }
  }

}
