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

import io.greenbus.edge.data.japi.IndexableValue;
import io.greenbus.edge.data.japi.Value;

import java.util.Map;
import java.util.Set;

public class ActiveSetUpdate implements DataUpdate {
    private final Map<IndexableValue, Value> value;
    private final Set<IndexableValue> removes;
    private final Map<IndexableValue, Value> added;
    private final Map<IndexableValue, Value> modified;

    public ActiveSetUpdate(Map<IndexableValue, Value> value, Set<IndexableValue> removes, Map<IndexableValue, Value> added, Map<IndexableValue, Value> modified) {
        this.value = value;
        this.removes = removes;
        this.added = added;
        this.modified = modified;
    }

    public Map<IndexableValue, Value> getValue() {
        return value;
    }

    public Set<IndexableValue> getRemoves() {
        return removes;
    }

    public Map<IndexableValue, Value> getAdded() {
        return added;
    }

    public Map<IndexableValue, Value> getModified() {
        return modified;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActiveSetUpdate that = (ActiveSetUpdate) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;
        if (removes != null ? !removes.equals(that.removes) : that.removes != null) return false;
        if (added != null ? !added.equals(that.added) : that.added != null) return false;
        return modified != null ? modified.equals(that.modified) : that.modified == null;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (removes != null ? removes.hashCode() : 0);
        result = 31 * result + (added != null ? added.hashCode() : 0);
        result = 31 * result + (modified != null ? modified.hashCode() : 0);
        return result;
    }
}
