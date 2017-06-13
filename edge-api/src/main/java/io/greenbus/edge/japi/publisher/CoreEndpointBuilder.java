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
package io.greenbus.edge.japi.publisher;

import io.greenbus.edge.data.japi.Value;
import io.greenbus.edge.japi.*;
import io.greenbus.edge.japi.flow.Receiver;

import java.util.HashMap;
import java.util.Map;

public class CoreEndpointBuilder {
    private final EndpointBuilder impl;
    private final Map<Path, Value> metadata = new HashMap<>();

    private CoreEndpointBuilder(EndpointBuilder impl) {
        this.impl = impl;
    }

    public static CoreEndpointBuilder newBuilder(EndpointBuilder impl) {
        return new CoreEndpointBuilder(impl);
    }

    public void addMetadata(Path path, Value value) {
        metadata.put(path, value);
    }

    public DoubleStatusBuilder addDoubleStatusSeries(Path path) {
        return new DoubleStatusBuilder(impl, path);
    }

    public DoubleSampleBuilder addDoubleSampleSeries(Path path) {
        return new DoubleSampleBuilder(impl, path);
    }

    public FloatStatusBuilder addFloatStatusSeries(Path path) {
        return new FloatStatusBuilder(impl, path);
    }

    public FloatSampleBuilder addFloatSampleSeries(Path path) {
        return new FloatSampleBuilder(impl, path);
    }

    public IntStatusBuilder addIntStatusSeries(Path path) {
        return new IntStatusBuilder(impl, path);
    }

    public IntSampleBuilder addIntSampleSeries(Path path) {
        return new IntSampleBuilder(impl, path);
    }

    public LongStatusBuilder addLongStatusSeries(Path path) {
        return new LongStatusBuilder(impl, path);
    }

    public LongSampleBuilder addLongSampleSeries(Path path) {
        return new LongSampleBuilder(impl, path);
    }

    public BoolStatusBuilder addBoolStatusSeries(Path path) {
        return new BoolStatusBuilder(impl, path);
    }

    public IntEnumBuilder addIntEnumSeries(Path path) {
        return new IntEnumBuilder(impl, path);
    }

    public TopicEventHandle addEvents(Path path, Map<Path, Value> metadata) {
        return impl.addEvents(path, metadata);
    }

    public ActiveSetHandle addActiveSet(Path path, Map<Path, Value> metadata) {
        return impl.addActiveSet(path, metadata);
    }

    public SimpleIndicationBuilder addSimpleIndicationOutput(Path path) {
        return new SimpleIndicationBuilder(impl, path);
    }
    public BooleanSetpointBuilder addBooleanSetpointOutput(Path path) {
        return new BooleanSetpointBuilder(impl, path);
    }
    public EnumerationSetpointBuilder addIntegerEnumerationOutput(Path path) {
        return new EnumerationSetpointBuilder(impl, path);
    }
    public AnalogSetpointBuilder addAnalogSetpointBuilder(Path path) {
        return new AnalogSetpointBuilder(impl, path);
    }

    public Receiver<OutputParams, OutputResult> registerOutput(Path path) {
        return impl.registerOutput(path);
    }

    public ProducerHandle build() {
        return impl.build();
    }
}
