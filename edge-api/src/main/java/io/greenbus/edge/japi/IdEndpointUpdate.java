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

import java.util.Optional;

public class IdEndpointUpdate implements IdentifiedEdgeUpdate {
    private final EndpointId id;
    private final EdgeDataStatus status;
    private final EndpointDescriptor update;

    public IdEndpointUpdate(EndpointId prefix, EdgeDataStatus status, EndpointDescriptor update) {
        this.id = prefix;
        this.status = status;
        this.update = update;
    }

    public EndpointId getId() {
        return id;
    }

    public EdgeDataStatus getStatus() {
        return status;
    }

    public Optional<EndpointDescriptor> getUpdate() {
        return Optional.ofNullable(update);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdEndpointUpdate that = (IdEndpointUpdate) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (status != that.status) return false;
        return update != null ? update.equals(that.update) : that.update == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (update != null ? update.hashCode() : 0);
        return result;
    }
}
