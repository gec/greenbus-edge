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
package io.greenbus.edge.japi.publisher.impl;

import io.greenbus.edge.data.japi.Value;
import io.greenbus.edge.edm.core.japi.JavaCoreModel;
import io.greenbus.edge.edm.core.japi.SeriesType;
import io.greenbus.edge.japi.EndpointBuilder;
import io.greenbus.edge.japi.Path;
import io.greenbus.edge.japi.SeriesValueHandle;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractAnalogBuilder<Builder extends AbstractAnalogBuilder<Builder, ?>, Handle> {
    EndpointBuilder builder;
    Path path;
    SeriesType type;
    String unit = null;
    Integer decimalPoints = null;
    Map<Path, Value> metadata = new HashMap<>();

    public AbstractAnalogBuilder(EndpointBuilder builder, Path path, SeriesType type) {
        this.builder = builder;
        this.path = path;
        this.type = type;
    }

    public Builder setUnit(String unit) {
        this.unit = unit;
        return (Builder)this;
    }
    public Builder setDecimalPoints(int decimalPoints) {
        this.decimalPoints = decimalPoints;
        return (Builder)this;
    }
    public Builder addMetadata(Path path, Value value) {
        metadata.put(path, value);
        return (Builder)this;
    }
    public Builder addMetadata(Map<Path, Value> metadata) {
        this.metadata.putAll(metadata);
        return (Builder)this;
    }

    protected abstract Handle buildHandle(SeriesValueHandle impl);

    public Handle build() {
        metadata.put(JavaCoreModel.seriesTypeKey(), JavaCoreModel.seriesTypeValue(type));
        if (unit != null ) {
            metadata.put(JavaCoreModel.unitKey(), JavaCoreModel.unitValue(unit));
        }
        if (decimalPoints != null) {
            metadata.put(JavaCoreModel.analogDecimalPointsKey(), JavaCoreModel.analogDecimalPointsValue(decimalPoints));
        }

        return buildHandle(builder.addSeries(path, metadata));
    }
}
