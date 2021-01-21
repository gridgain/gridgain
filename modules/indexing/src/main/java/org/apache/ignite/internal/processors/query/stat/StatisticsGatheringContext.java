/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Statistics gathering context.
 */
public class StatisticsGatheringContext {
    /** Gathering id. */
    private final UUID gatId;

    /** Keys to collect statistics by. */
    private final Set<StatisticsKeyMessage> keys;

    /** Amount of remaining partitions */
    private int remainingParts;

    /** Collected local statistics. */
    private final Map<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> collectedStatistics;

    /** Done future adapter. */
    private final StatisticsGatheringFutureAdapter<Map<StatisticsTarget, ObjectStatistics>> doneFut;

    /**
     * Constructor in case when there are already gathering id generated.
     *
     * @param gatId Gathering id.
     * @param keys Collection of keys to collect statistics by.
     * @param remainingParts Number of partition to be collected by gathering context.
     */
    public StatisticsGatheringContext(UUID gatId, Set<StatisticsKeyMessage> keys, int remainingParts) {
        this.gatId = gatId;
        collectedStatistics = new HashMap<>();
        this.keys = keys;
        this.remainingParts = remainingParts;
        StatisticsTarget[] targets = keys.stream().map(key -> new StatisticsTarget(key.schema(), key.obj(),
            key.colNames().toArray(new String[0]))).toArray(StatisticsTarget[]::new);
        this.doneFut = new StatisticsGatheringFutureAdapter<>(gatId, targets);
    }

    /**
     * @return Collected statistics map.
     */
    public Map<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> collectedStatistics() {
        return collectedStatistics;
    }

    /**
     * @return Collection of keys to collect statistics by.
     */
    public Collection<StatisticsKeyMessage> keys() {
        return keys;
    }

    /**
     * Register collected object statistics data.
     *
     * @param objsData Object statistics data to register.
     * @param parts Total number of partitions in collected data.
     * @return {@code true} if all required partition collected, {@code false} otherwise.
     */
    public synchronized boolean registerCollected(
        Map<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> objsData,
        int parts
    ) {
        for (Map.Entry<StatisticsKeyMessage, Collection<ObjectStatisticsImpl>> objData : objsData.entrySet())
            collectedStatistics.computeIfAbsent(objData.getKey(), k -> new ArrayList<>()).addAll(objData.getValue());

        return decrement(parts);
    }

    /**
     * Decrement remaining parts and tell if all required partitions are collected.
     *
     * @param parts Decrement.
     * @return {@code true} if all required partition collected, {@code false} otherwise.
     */
    private synchronized boolean decrement(int parts) {
        this.remainingParts -= parts;
        return remainingParts == 0;
    }

    /**
     * @return Gathering (whole process) id.
     */
    public UUID gatheringId() {
        return gatId;
    }

    /**
     * @return Collection control future.
     */
    public StatisticsGatheringFutureAdapter<Map<StatisticsTarget, ObjectStatistics>> doneFuture() {
        return doneFut;
    }
}
