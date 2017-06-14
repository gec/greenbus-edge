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

public class OutputParams {
    private final UUID session;
    private final Long sequence;
    private final Value compareValue;
    private final Value outputValue;

    public OutputParams(UUID session, Long sequence, Value compareValue, Value outputValue) {
        this.session = session;
        this.sequence = sequence;
        this.compareValue = compareValue;
        this.outputValue = outputValue;
    }

    public OutputParams() {
        this.session = null;
        this.sequence = null;
        this.compareValue = null;
        this.outputValue = null;
    }

    public UUID getSession() {
        return session;
    }

    public Long getSequence() {
        return sequence;
    }

    public Value getCompareValue() {
        return compareValue;
    }

    public Value getOutputValue() {
        return outputValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OutputParams that = (OutputParams) o;

        if (session != null ? !session.equals(that.session) : that.session != null) return false;
        if (sequence != null ? !sequence.equals(that.sequence) : that.sequence != null) return false;
        if (compareValue != null ? !compareValue.equals(that.compareValue) : that.compareValue != null) return false;
        return outputValue != null ? outputValue.equals(that.outputValue) : that.outputValue == null;
    }

    @Override
    public int hashCode() {
        int result = session != null ? session.hashCode() : 0;
        result = 31 * result + (sequence != null ? sequence.hashCode() : 0);
        result = 31 * result + (compareValue != null ? compareValue.hashCode() : 0);
        result = 31 * result + (outputValue != null ? outputValue.hashCode() : 0);
        return result;
    }
}
