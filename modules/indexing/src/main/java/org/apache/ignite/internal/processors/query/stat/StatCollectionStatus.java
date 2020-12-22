package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatsKeyMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Statistics collection status by local or global request.
 */
public class StatCollectionStatus {
    /** Collection id. */
    private final UUID colId;

    /** Keys to collect statistics by. */
    private Collection<StatsKeyMessage> keys;

    /** Map request id to collection request. */
    private final ConcurrentMap<UUID, StatsAddrRequest<StatsCollectionRequest>> remainingColReqs;

    /** Collected local statistics. */
    private final List<StatsCollectionResponse> locStatistics;

    /** Done future adapter. */
    private final StatsCollectionFutureAdapter doneFut;

    /**
     * Constructor.
     *
     * @param colId Collection id.
     * @param keys Keys to collect statistics by.
     * @param remainingColReqs Collection of remaining requests. If {@code null} - it's local collection task.
     */
    public StatCollectionStatus(
            UUID colId,
            Collection<StatsKeyMessage> keys,
            Map<UUID, StatsAddrRequest<StatsCollectionRequest>> remainingColReqs
    ) {
        this.colId = colId;
        this.keys = keys;
        this.remainingColReqs = (remainingColReqs == null) ? null : new ConcurrentHashMap<>(remainingColReqs);
        locStatistics = (remainingColReqs == null) ? null : Collections.synchronizedList(
                new ArrayList<>(remainingColReqs.size()));
        this.doneFut = new StatsCollectionFutureAdapter(colId);;
    }

    /**
     * @return Collection of keys to collect statistics by.
     */
    public Collection<StatsKeyMessage> keys() {
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
    public boolean registerCollected(StatsCollectionResponse resp) {
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
        Map<UUID, StatsAddrRequest<StatsCollectionRequest>> newReqs,
        StatsCollectionResponse resp
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
    public Collection<StatsCollectionRequest> removeNodeRequest(UUID nodeId) {
        Collection<StatsCollectionRequest> res = new ArrayList<>();

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
    public Map<UUID, StatsAddrRequest<StatsCollectionRequest>> remainingCollectionReqs() {
        return remainingColReqs;
    }

    /**
     * @return Collected local statistics.
     */
    public List<StatsCollectionResponse> localStatistics() {
        return locStatistics;
    }

    /**
     * @return Collection control future.
     */
    public StatsCollectionFutureAdapter doneFut() {
        return doneFut;
    }
}
