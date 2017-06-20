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
import io.greenbus.edge.edm.core.japi.JavaCoreModel;
import io.greenbus.edge.edm.core.japi.OutputType;
import io.greenbus.edge.japi.*;
import io.greenbus.edge.japi.flow.Receiver;

import java.util.HashMap;
import java.util.Map;

public class EnumerationSetpointBuilder {
    private final EndpointBuilder builder;
    private final Path path;
    private final Map<Long, String> labels = new HashMap<>();
    private final Map<Path, Value> metadata = new HashMap<>();

    public EnumerationSetpointBuilder(EndpointBuilder builder, Path path) {
        this.builder = builder;
        this.path = path;
    }

    public EnumerationSetpointBuilder addLabel(long key, String label) {
        labels.put(key, label);
        return this;
    }

    public EnumerationSetpointBuilder addMetadata(Path path, Value value) {
        metadata.put(path, value);
        return this;
    }
    public EnumerationSetpointBuilder addMetadata(Map<Path, Value> metadata) {
        this.metadata.putAll(metadata);
        return this;
    }

    public OutputEntry build() {
        metadata.put(JavaCoreModel.outputTypeKey(), JavaCoreModel.outputTypeValue(OutputType.EnumerationSetpoint));

        if (!labels.isEmpty()) {
            metadata.put(JavaCoreModel.requestIntegerLabelsKey(), JavaCoreModel.requestIntegerLabelsValue(labels));
        }

        final OutputStatusHandle handle = builder.addOutputStatus(path, metadata);
        final Receiver<OutputParams, OutputResult> receiver = builder.registerOutput(path);
        return new OutputEntry(handle, receiver);
    }
}
