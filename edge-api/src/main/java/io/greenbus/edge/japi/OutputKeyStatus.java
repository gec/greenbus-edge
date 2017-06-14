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

import java.util.UUID;

public class OutputKeyStatus {
    private final UUID session;
    private final long sequence;
    private final Value value;

    public OutputKeyStatus(UUID session, long sequence, Value value) {
        this.session = session;
        this.sequence = sequence;
        this.value = value;
    }

    public OutputKeyStatus(UUID session, long sequence) {
        this.session = session;
        this.sequence = sequence;
        this.value = null;
    }

    public UUID getSession() {
        return session;
    }

    public long getSequence() {
        return sequence;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OutputKeyStatus that = (OutputKeyStatus) o;

        if (sequence != that.sequence) return false;
        if (session != null ? !session.equals(that.session) : that.session != null) return false;
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = session != null ? session.hashCode() : 0;
        result = 31 * result + (int) (sequence ^ (sequence >>> 32));
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
