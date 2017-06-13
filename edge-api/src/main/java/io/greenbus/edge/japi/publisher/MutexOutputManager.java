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

import io.greenbus.edge.japi.*;
import io.greenbus.edge.japi.flow.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class MutexOutputManager {
    private static Logger logger = LoggerFactory.getLogger(MutexOutputManager.class);

    private final Object mutex;
    private final Path path;
    private final OutputStatusHandle handle;
    private final Receiver<OutputParams, OutputResult> receiver;
    private final UUID session = UUID.randomUUID();
    private final AtomicLong sequence = new AtomicLong(1);
    //private final AtomicReference<ProducerHandle> buffer = new AtomicReference<>(null);
    //private final ProducerHandle buffer = null;

    public MutexOutputManager(Object mutex, Path path, OutputStatusHandle handle, Receiver<OutputParams, OutputResult> receiver) {
        this.mutex = mutex;
        this.path = path;
        this.handle = handle;
        this.receiver = receiver;
    }

    public void register(final ProducerHandle buffer) {
        receiver.bind((params, handler) -> {
            synchronized (mutex) {
                logger.info("Handling output: " + params);

                if ((params.getSession() == null || params.getSession().equals(session)) &&
                        (params.getSequence() == null || params.getSequence() == sequence.get())) {

                }

                //outputCountHandle.update(new ValueInt64(counter.getAndIncrement()), System.currentTimeMillis());
                handle.update(new OutputKeyStatus(session, sequence.getAndIncrement()));
                buffer.flush();
            }
        });
    }
}
