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
package io.greenbus.edge.japi.subscribe;

import io.greenbus.edge.japi.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class SubscriptionHandlers {

    private final Map<Path, List<Consumer<IdEndpointPrefixUpdate>>> prefixHandlers;
    private final Map<EndpointId, List<Consumer<IdEndpointUpdate>>> endpointHandlers;
    private final Map<EndpointPath, List<Consumer<IdDataKeyUpdate>>> dataHandlers;
    private final Map<EndpointPath, List<Consumer<IdOutputKeyUpdate>>> outputHandlers;

    private SubscriptionHandlers(Map<Path, List<Consumer<IdEndpointPrefixUpdate>>> prefixHandlers, Map<EndpointId, List<Consumer<IdEndpointUpdate>>> endpointHandlers, Map<EndpointPath, List<Consumer<IdDataKeyUpdate>>> dataHandlers, Map<EndpointPath, List<Consumer<IdOutputKeyUpdate>>> outputHandlers) {
        this.prefixHandlers = prefixHandlers;
        this.endpointHandlers = endpointHandlers;
        this.dataHandlers = dataHandlers;
        this.outputHandlers = outputHandlers;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public void handle(List<IdentifiedEdgeUpdate> updates) {
        System.err.println(updates);
        for (IdentifiedEdgeUpdate update : updates) {
            if (update instanceof IdEndpointPrefixUpdate) {
                IdEndpointPrefixUpdate prefixUpdate = (IdEndpointPrefixUpdate) update;
                List<Consumer<IdEndpointPrefixUpdate>> consumers = prefixHandlers.get(prefixUpdate.getPrefix());
                if (consumers != null) {
                    for (Consumer<IdEndpointPrefixUpdate> consumer : consumers) {
                        consumer.accept(prefixUpdate);
                    }
                }
            } else if (update instanceof IdEndpointUpdate) {
                IdEndpointUpdate endpointUpdate = (IdEndpointUpdate) update;
                List<Consumer<IdEndpointUpdate>> consumers = endpointHandlers.get(endpointUpdate.getId());
                if (consumers != null) {
                    for (Consumer<IdEndpointUpdate> consumer : consumers) {
                        consumer.accept(endpointUpdate);
                    }
                }
            } else if (update instanceof IdDataKeyUpdate) {
                IdDataKeyUpdate dataKeyUpdate = (IdDataKeyUpdate) update;
                List<Consumer<IdDataKeyUpdate>> consumers = dataHandlers.get(dataKeyUpdate.getId());
                if (consumers != null) {
                    for (Consumer<IdDataKeyUpdate> consumer : consumers) {
                        consumer.accept(dataKeyUpdate);
                    }
                }
            } else if (update instanceof IdOutputKeyUpdate) {
                IdOutputKeyUpdate outputKeyUpdate = (IdOutputKeyUpdate) update;
                List<Consumer<IdOutputKeyUpdate>> consumers = outputHandlers.get(outputKeyUpdate.getId());
                if (consumers != null) {
                    for (Consumer<IdOutputKeyUpdate> consumer : consumers) {
                        consumer.accept(outputKeyUpdate);
                    }
                }
            }
        }
    }

    public static class Builder {
        private final Map<Path, List<Consumer<IdEndpointPrefixUpdate>>> prefixHandlers = new HashMap<>();
        private final Map<EndpointId, List<Consumer<IdEndpointUpdate>>> endpointHandlers = new HashMap<>();
        private final Map<EndpointPath, List<Consumer<IdDataKeyUpdate>>> dataHandlers = new HashMap<>();
        private final Map<EndpointPath, List<Consumer<IdOutputKeyUpdate>>> outputHandlers = new HashMap<>();

        public Builder addPrefixHandler(Path path, Consumer<IdEndpointPrefixUpdate> handler) {
            if (prefixHandlers.containsKey(path)) {
                prefixHandlers.get(path).add(handler);
            } else {
                final ArrayList<Consumer<IdEndpointPrefixUpdate>> set = new ArrayList<>();
                set.add(handler);
                prefixHandlers.put(path, set);
            }
            return this;
        }
        public Builder addEndpointHandler(EndpointId path, Consumer<IdEndpointUpdate> handler) {
            if (endpointHandlers.containsKey(path)) {
                endpointHandlers.get(path).add(handler);
            } else {
                final ArrayList<Consumer<IdEndpointUpdate>> set = new ArrayList<>();
                set.add(handler);
                endpointHandlers.put(path, set);
            }
            return this;
        }
        public Builder addDataKeyUpdateHandler(EndpointPath path, Consumer<IdDataKeyUpdate> handler) {
            if (dataHandlers.containsKey(path)) {
                dataHandlers.get(path).add(handler);
            } else {
                final ArrayList<Consumer<IdDataKeyUpdate>> set = new ArrayList<>();
                set.add(handler);
                dataHandlers.put(path, set);
            }
            return this;
        }
        public Builder addOutputKeyUpdateHandler(EndpointPath path, Consumer<IdOutputKeyUpdate> handler) {
            if (outputHandlers.containsKey(path)) {
                outputHandlers.get(path).add(handler);
            } else {
                final ArrayList<Consumer<IdOutputKeyUpdate>> set = new ArrayList<>();
                set.add(handler);
                outputHandlers.put(path, set);
            }
            return this;
        }

        public SubscriptionHandlers build() {
            return new SubscriptionHandlers(prefixHandlers, endpointHandlers, dataHandlers, outputHandlers);
        }
    }

    public static Consumer<IdEndpointPrefixUpdate> onResolvedEndpointSet(Consumer<EndpointSetUpdate> handler) {
        return update -> {
            if (update.getStatus() == EdgeDataStatus.Resolved) {
                if (update.getUpdate().isPresent()) {
                    handler.accept(update.getUpdate().get());
                }
            }
        };
    }

    public static Consumer<IdEndpointUpdate> onResolvedEndpointDescriptor(Consumer<EndpointDescriptor> handler) {
        return update -> {
            if (update.getStatus() == EdgeDataStatus.Resolved) {
                if (update.getUpdate().isPresent()) {
                    handler.accept(update.getUpdate().get());
                }
            }
        };
    }

    public static Consumer<IdDataKeyUpdate> onResolvedDataKeyUpdate(Consumer<DataKeyUpdate> handler) {
        return update -> {
            if (update.getStatus() == EdgeDataStatus.Resolved) {
                if (update.getUpdate().isPresent()) {
                    handler.accept(update.getUpdate().get());
                }
            }
        };
    }

    public static Consumer<IdOutputKeyUpdate> onResolvedOutputKeyUpdate(Consumer<OutputKeyUpdate> handler) {
        return update -> {
            if (update.getStatus() == EdgeDataStatus.Resolved) {
                if (update.getUpdate().isPresent()) {
                    handler.accept(update.getUpdate().get());
                }
            }
        };
    }

    public static Consumer<DataKeyUpdate> onSeriesUpdate(Consumer<SeriesUpdate> handler) {
        return update -> {
            if (update.getUpdate() instanceof SeriesUpdate) {
                handler.accept((SeriesUpdate) update.getUpdate());
            }
        };
    }

    public static Consumer<DataKeyUpdate> onKeyValueUpdate(Consumer<KeyValueUpdate> handler) {
        return update -> {
            if (update.getUpdate() instanceof KeyValueUpdate) {
                handler.accept((KeyValueUpdate) update.getUpdate());
            }
        };
    }

    public static Consumer<DataKeyUpdate> onTopicEventUpdate(Consumer<TopicEventUpdate> handler) {
        return update -> {
            if (update.getUpdate() instanceof TopicEventUpdate) {
                handler.accept((TopicEventUpdate) update.getUpdate());
            }
        };
    }

    public static Consumer<DataKeyUpdate> onActiveSetUpdate(Consumer<ActiveSetUpdate> handler) {
        return update -> {
            if (update.getUpdate() instanceof ActiveSetUpdate) {
                handler.accept((ActiveSetUpdate) update.getUpdate());
            }
        };
    }
}
