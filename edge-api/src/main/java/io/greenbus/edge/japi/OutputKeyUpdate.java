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

public class OutputKeyUpdate {
    private final OutputKeyDescriptor descriptor;
    private final OutputKeyStatus update;

    public OutputKeyUpdate(OutputKeyDescriptor descriptor, OutputKeyStatus update) {
        this.descriptor = descriptor;
        this.update = update;
    }

    public OutputKeyUpdate(OutputKeyStatus update) {
        this.descriptor = null;
        this.update = update;
    }

    public Optional<OutputKeyDescriptor> getDescriptor() {
        return Optional.ofNullable(descriptor);
    }

    public OutputKeyStatus getUpdate() {
        return update;
    }

    @Override
    public String toString() {
        return "OutputKeyUpdate{" +
                "descriptor=" + descriptor +
                ", update=" + update +
                '}';
    }
}
