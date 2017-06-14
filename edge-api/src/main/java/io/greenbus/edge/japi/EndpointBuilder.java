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
package io.greenbus.edge.japi;

import io.greenbus.edge.data.japi.Value;
import io.greenbus.edge.japi.flow.Receiver;

import java.util.Map;

public interface EndpointBuilder {

    void setMetadata(Map<Path, Value> metadata);

    SeriesValueHandle addSeries(Path path, Map<Path, Value> metadata);
    LatestKeyValueHandle addKeyValue(Path path, Map<Path, Value> metadata);
    TopicEventHandle addEvents(Path path, Map<Path, Value> metadata);
    ActiveSetHandle addActiveSet(Path path, Map<Path, Value> metadata);
    OutputStatusHandle addOutputStatus(Path path, Map<Path, Value> metadata);

    Receiver<OutputParams, OutputResult> registerOutput(Path path);

    ProducerHandle build();
}
