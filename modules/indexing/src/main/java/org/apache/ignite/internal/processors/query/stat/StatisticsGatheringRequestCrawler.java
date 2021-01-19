/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Crawler to track and handle any requests, related to statistics.
 * Crawler tracks requests and call back statistics manager to process failed requests.
 */
public interface StatisticsGatheringRequestCrawler {
    /**
     * Send specified requests.
     *
     * @param getId Gathering id.
     * @param keys Keys to collect statistics by.
     * @param failedParts Failed partitions or {@code null} if needs to send requests to by all partitions.
     */
    public void sendGatheringRequestsAsync(
        UUID gatId,
        Collection<StatisticsKeyMessage> keys,
        Collection<Integer> failedParts
    );

    /**
     * Send statistics gathering response async.
     *
     * @param reqId Request to response to.
     * @param statistics Collected statistics by keys.
     * @param parts Partitions, included in collected statistics.
     */
    public void sendGatheringResponseAsync(
        UUID reqId,
        Map<StatisticsKeyMessage, ObjectStatisticsImpl> statistics,
        int[] parts
    );

    /**
     * Send cancel statistics gathering requests.
     *
     * @param gatId gathering id to cancel.
     */
    public void sendCancelGatheringAsync(UUID gatId);

    /**
     * Send statistics clear request.
     *
     * @param keys keys to clear statistics by.
     */
    public void sendClearStatisticsAsync(Collection<StatisticsKeyMessage> keys);

    /**
     * Send statistics propagation message to nodes with backups.
     *
     * @param key Statistics key.
     * @param objStats Objects statistics to propagate.
     */
    public void sendPartitionStatisticsToBackupNodesAsync(
        StatisticsKeyMessage key,
        Collection<ObjectPartitionStatisticsImpl> objStats
    );

    /**
     * Send specified global statistics to all server nodes.
     *
     * @param globalStat Global statistics to send.
     */
    public void sendGlobalStatAsync(Map<StatisticsKeyMessage, ObjectStatisticsImpl> globalStat);
}
