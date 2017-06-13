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

import java.util.Set;

public class EndpointSetUpdate {
    private final Set<EndpointId> value;
    private final Set<EndpointId> removes;
    private final Set<EndpointId> adds;

    public EndpointSetUpdate(Set<EndpointId> value, Set<EndpointId> removes, Set<EndpointId> adds) {
        this.value = value;
        this.removes = removes;
        this.adds = adds;
    }

    public Set<EndpointId> getValue() {
        return value;
    }

    public Set<EndpointId> getRemoves() {
        return removes;
    }

    public Set<EndpointId> getAdds() {
        return adds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EndpointSetUpdate that = (EndpointSetUpdate) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;
        if (removes != null ? !removes.equals(that.removes) : that.removes != null) return false;
        return adds != null ? adds.equals(that.adds) : that.adds == null;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (removes != null ? removes.hashCode() : 0);
        result = 31 * result + (adds != null ? adds.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EndpointSetUpdate{" +
                "value=" + value +
                ", removes=" + removes +
                ", adds=" + adds +
                '}';
    }
}
