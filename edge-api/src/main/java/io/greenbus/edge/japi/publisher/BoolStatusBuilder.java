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
import io.greenbus.edge.edm.core.japi.SeriesType;
import io.greenbus.edge.japi.EndpointBuilder;
import io.greenbus.edge.japi.Path;
import io.greenbus.edge.japi.publisher.impl.BoolSeriesWrapper;

import java.util.HashMap;
import java.util.Map;

public class BoolStatusBuilder {
    private final EndpointBuilder builder;
    private final Path path;
    private String truthLabel = null;
    private String falseLabel = null;
    private final Map<Path, Value> metadata = new HashMap<>();

    public BoolStatusBuilder(EndpointBuilder builder, Path path) {
        this.builder = builder;
        this.path = path;
    }

    public BoolStatusBuilder setLabels(String truthLabel, String falseLabel) {
        this.truthLabel = truthLabel;
        this.falseLabel = falseLabel;
        return this;
    }
    public BoolStatusBuilder addMetadata(Path path, Value value) {
        metadata.put(path, value);
        return this;
    }
    public BoolStatusBuilder addMetadata(Map<Path, Value> metadata) {
        this.metadata.putAll(metadata);
        return this;
    }

    public BoolSeriesHandle build() {
        metadata.put(JavaCoreModel.seriesTypeKey(), JavaCoreModel.seriesTypeValue(SeriesType.BooleanStatus));

        if (truthLabel != null && falseLabel != null) {
            metadata.put(JavaCoreModel.labeledBooleanKey(), JavaCoreModel.labeledBooleanValue(truthLabel, falseLabel));
        }

        return new BoolSeriesWrapper(builder.addSeries(path, metadata));
    }
}
