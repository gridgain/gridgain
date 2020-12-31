package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.lang.IgniteBiTuple;

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
