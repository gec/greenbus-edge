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

import java.util.Map;

public class EndpointDescriptor {
    private final Map<Path, Value> metadata;
    private final Map<Path, DataKeyDescriptor> dataKeySet;
    private final Map<Path, OutputKeyDescriptor> outputKeySet;

    public EndpointDescriptor(Map<Path, Value> metadata, Map<Path, DataKeyDescriptor> dataKeySet, Map<Path, OutputKeyDescriptor> outputKeySet) {
        this.metadata = metadata;
        this.dataKeySet = dataKeySet;
        this.outputKeySet = outputKeySet;
    }

    public Map<Path, Value> getMetadata() {
        return metadata;
    }

    public Map<Path, DataKeyDescriptor> getDataKeySet() {
        return dataKeySet;
    }

    public Map<Path, OutputKeyDescriptor> getOutputKeySet() {
        return outputKeySet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EndpointDescriptor that = (EndpointDescriptor) o;

        if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
        if (dataKeySet != null ? !dataKeySet.equals(that.dataKeySet) : that.dataKeySet != null) return false;
        return outputKeySet != null ? outputKeySet.equals(that.outputKeySet) : that.outputKeySet == null;
    }

    @Override
    public int hashCode() {
        int result = metadata != null ? metadata.hashCode() : 0;
        result = 31 * result + (dataKeySet != null ? dataKeySet.hashCode() : 0);
        result = 31 * result + (outputKeySet != null ? outputKeySet.hashCode() : 0);
        return result;
    }
}
