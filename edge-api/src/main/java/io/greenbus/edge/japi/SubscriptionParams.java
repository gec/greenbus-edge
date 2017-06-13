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

import java.util.List;

public class SubscriptionParams {
    private final List<Path> endpointPrefixSet;
    private final List<EndpointId> endpointDescriptors;
    private final List<EndpointPath> dataKeys;
    private final List<EndpointPath> outputKeys;

    public SubscriptionParams(List<Path> endpointPrefixSet, List<EndpointId> endpointDescriptors, List<EndpointPath> dataKeys, List<EndpointPath> outputKeys) {
        this.endpointPrefixSet = endpointPrefixSet;
        this.endpointDescriptors = endpointDescriptors;
        this.dataKeys = dataKeys;
        this.outputKeys = outputKeys;
    }

    public List<Path> getEndpointPrefixSet() {
        return endpointPrefixSet;
    }

    public List<EndpointId> getEndpointDescriptors() {
        return endpointDescriptors;
    }

    public List<EndpointPath> getDataKeys() {
        return dataKeys;
    }

    public List<EndpointPath> getOutputKeys() {
        return outputKeys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubscriptionParams that = (SubscriptionParams) o;

        if (endpointPrefixSet != null ? !endpointPrefixSet.equals(that.endpointPrefixSet) : that.endpointPrefixSet != null)
            return false;
        if (endpointDescriptors != null ? !endpointDescriptors.equals(that.endpointDescriptors) : that.endpointDescriptors != null)
            return false;
        if (dataKeys != null ? !dataKeys.equals(that.dataKeys) : that.dataKeys != null) return false;
        return outputKeys != null ? outputKeys.equals(that.outputKeys) : that.outputKeys == null;
    }

    @Override
    public int hashCode() {
        int result = endpointPrefixSet != null ? endpointPrefixSet.hashCode() : 0;
        result = 31 * result + (endpointDescriptors != null ? endpointDescriptors.hashCode() : 0);
        result = 31 * result + (dataKeys != null ? dataKeys.hashCode() : 0);
        result = 31 * result + (outputKeys != null ? outputKeys.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SubscriptionParams{" +
                "endpointPrefixSet=" + endpointPrefixSet +
                ", endpointDescriptors=" + endpointDescriptors +
                ", dataKeys=" + dataKeys +
                ", outputKeys=" + outputKeys +
                '}';
    }
}
