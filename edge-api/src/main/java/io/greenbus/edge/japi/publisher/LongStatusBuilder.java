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
package io.greenbus.edge.japi.publisher;

import io.greenbus.edge.edm.core.japi.SeriesType;
import io.greenbus.edge.japi.EndpointBuilder;
import io.greenbus.edge.japi.Path;
import io.greenbus.edge.japi.SeriesValueHandle;
import io.greenbus.edge.japi.publisher.impl.AbstractAnalogBuilder;
import io.greenbus.edge.japi.publisher.impl.IntSeriesWrapper;
import io.greenbus.edge.japi.publisher.impl.LongSeriesWrapper;

public class LongStatusBuilder extends AbstractAnalogBuilder<LongStatusBuilder, LongSeriesHandle> {
    public LongStatusBuilder(EndpointBuilder builder, Path path) {
        super(builder, path, SeriesType.AnalogStatus);
    }

    @Override
    protected LongSeriesHandle buildHandle(SeriesValueHandle impl) {
        return new LongSeriesWrapper(impl);
    }
}
