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

public class IdEndpointPrefixUpdate implements IdentifiedEdgeUpdate {
    private final Path prefix;
    private final EdgeDataStatus status;
    private final EndpointSetUpdate update;

    public IdEndpointPrefixUpdate(Path prefix, EdgeDataStatus status, EndpointSetUpdate update) {
        this.prefix = prefix;
        this.status = status;
        this.update = update;
    }

    public Path getPrefix() {
        return prefix;
    }

    public EdgeDataStatus getStatus() {
        return status;
    }

    public Optional<EndpointSetUpdate> getUpdate() {
        return Optional.ofNullable(update);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdEndpointPrefixUpdate that = (IdEndpointPrefixUpdate) o;

        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) return false;
        if (status != that.status) return false;
        return update != null ? update.equals(that.update) : that.update == null;
    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (update != null ? update.hashCode() : 0);
        return result;
    }
}
