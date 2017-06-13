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
package io.greenbus.edge.japi.publisher.impl;

import io.greenbus.edge.data.japi.ValueDouble;
import io.greenbus.edge.japi.SeriesValueHandle;
import io.greenbus.edge.japi.publisher.DoubleSeriesHandle;

public class DoubleSeriesWrapper implements DoubleSeriesHandle {
    private final SeriesValueHandle handle;

    public DoubleSeriesWrapper(SeriesValueHandle handle) {
        this.handle = handle;
    }

    @Override
    public void update(double value, long timeMs) {
        handle.update(new ValueDouble(value), timeMs);
    }
}
