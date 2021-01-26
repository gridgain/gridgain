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

import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Collector which scan local data to gather statistics.
 */
public interface StatisticsGathering {
    /**
     * Collect local statistics by specified keys and partitions
     * and pass it to router to send in response to specified reqId.
     *
     * @param reqId Request id.
     * @param keys Keys to collect statistics by.
     * @param parts Partitions to collect statistics by.
     * @param cancelled Supplier to track cancelled state.
     */
    public void collectLocalObjectsStatisticsAsync(
        UUID reqId,
        Set<StatisticsKeyMessage> keys,
        int[] parts,
        Supplier<Boolean> cancelled
    );
}
