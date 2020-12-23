package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Statistics collection context by local or global request.
 */
public class StatisticsCollectionContext {
    /** Collection id. */
    private final UUID colId;

    /** Keys to collect statistics by. */
    private Collection<StatisticsKeyMessage> keys;

    /** Map request id to collection request. */
    private final ConcurrentMap<UUID, StatisticssAddrRequest<StatisticsGatheringRequest>> remainingColReqs;

    /** Collected local statistics. */
    private final List<StatisticsGatheringResponse> locStatistics;

    /** Done future adapter. */
    private final StatisticsCollectionFutureAdapter doneFut;

    /**
     * Constructor.
     *
     * @param colId Collection id.
     * @param keys Keys to collect statistics by.
     * @param remainingColReqs Collection of remaining requests. If {@code null} - it's local collection task.
     */
    public StatisticsCollectionContext(
            UUID colId,
            Collection<StatisticsKeyMessage> keys,
            Map<UUID, StatisticssAddrRequest<StatisticsGatheringRequest>> remainingColReqs
    ) {
        this.colId = colId;
        this.keys = keys;
        this.remainingColReqs = (remainingColReqs == null) ? null : new ConcurrentHashMap<>(remainingColReqs);
        locStatistics = (remainingColReqs == null) ? null : Collections.synchronizedList(
                new ArrayList<>(remainingColReqs.size()));
        this.doneFut = new StatisticsCollectionFutureAdapter(colId);;
    }

    /**
     * @return Collection of keys to collect statistics by.
     */
    public Collection<StatisticsKeyMessage> keys() {
        return keys;
    }

    /**
     * Register collected response. If the response contains not all requested partitions - replace should be called
     * instead.
     *
     * @param resp Collection response to register.
     * @return {@code true} if all request finished and global statistics could be aggregated,
     *     {@code false} - otherwise.
     */
    public boolean registerCollected(StatisticsGatheringResponse resp) {
        assert colId.equals(resp.colId());

        locStatistics.add(resp);
        remainingColReqs.remove(resp.reqId());
        return remainingColReqs.isEmpty();
    }

    /**
     * Replace collection of old requests with new ones. Should be called on receiving response with not all requested
     * partitions.
     *
     * @param oldReqIds Old request id to remove from state.
     * @param newReqs new requests to add to the state.
     * @param resp Collected response to add to the state.
     */
    public void replaceStatsCollectionRequest(
        UUID oldReqId,
        Map<UUID, StatisticssAddrRequest<StatisticsGatheringRequest>> newReqs,
        StatisticsGatheringResponse resp
    ) {
        locStatistics.add(resp);
        remainingColReqs.putAll(newReqs);
        remainingColReqs.remove(oldReqId);
    }

    /**
     * Remove all requests to specified node id (due to its failure).
     *
     * @param nodeId node id to remove requests by.
     * @return Collection of removed requests.
     */
    public Collection<StatisticsGatheringRequest> removeNodeRequest(UUID nodeId) {
        Collection<StatisticsGatheringRequest> res = new ArrayList<>();

        remainingColReqs.values().removeIf(req -> {
            if (nodeId.equals(req.nodeId())) {
                res.add(req.req());

                return true;
            }
            return false;
        });

        return res;
    }

    /**
     * @return Collection id.
     */
    public UUID colId() {
        return colId;
    }

    /**
     * @return Map request id to collection request.
     */
    public Map<UUID, StatisticssAddrRequest<StatisticsGatheringRequest>> remainingCollectionReqs() {
        return remainingColReqs;
    }

    /**
     * @return Collected local statistics.
     */
    public List<StatisticsGatheringResponse> localStatistics() {
        return locStatistics;
    }

    /**
     * @return Collection control future.
     */
    public StatisticsCollectionFutureAdapter doneFut() {
        return doneFut;
    }
}
