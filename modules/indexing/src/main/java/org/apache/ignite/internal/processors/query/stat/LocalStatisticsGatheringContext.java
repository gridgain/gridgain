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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.internal.processors.query.stat.config.ObjectStatisticsConfiguration;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Statistics gathering context.
 */
public class LocalStatisticsGatheringContext {
    /** Keys to collect statistics by. */
    private final Set<ObjectStatisticsConfiguration> objStatCfgs;

    /** Amount of remaining partitions */
    private int remainingParts;

    /** Collected local statistics. */
    private final Map<StatisticsKey, Collection<ObjectStatisticsImpl>> collectedStatistics;

    /** Done future adapter. */
    private final GridFutureAdapter<Void> fut;

    /** */
    public LocalStatisticsGatheringContext(Set<ObjectStatisticsConfiguration> objStatCfgs, int remainingParts) {
        collectedStatistics = new HashMap<>();
        this.objStatCfgs = objStatCfgs;
        this.remainingParts = remainingParts;
        this.fut = new GridFutureAdapter<>();
    }

    /** */
    public Set<ObjectStatisticsConfiguration> objectStatisticsConfigurations() {
        return objStatCfgs;
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
     * @return Collection control future.
     */
    public GridFutureAdapter<Void> future() {
        return fut;
    }
}
