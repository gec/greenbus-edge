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

public class DataKeyUpdate {
    private final DataKeyDescriptor descriptor;
    private final DataUpdate update;

    public DataKeyUpdate(DataKeyDescriptor descriptor, DataUpdate update) {
        this.descriptor = descriptor;
        this.update = update;
    }

    public DataKeyUpdate(DataUpdate update) {
        this.descriptor = null;
        this.update = update;
    }

    public Optional<DataKeyDescriptor> getDescriptor() {
        return Optional.ofNullable(descriptor);
    }

    public DataUpdate getUpdate() {
        return update;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataKeyUpdate that = (DataKeyUpdate) o;

        if (descriptor != null ? !descriptor.equals(that.descriptor) : that.descriptor != null) return false;
        return update != null ? update.equals(that.update) : that.update == null;
    }

    @Override
    public int hashCode() {
        int result = descriptor != null ? descriptor.hashCode() : 0;
        result = 31 * result + (update != null ? update.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DataKeyUpdate{" +
                "descriptor=" + descriptor +
                ", update=" + update +
                '}';
    }
}
