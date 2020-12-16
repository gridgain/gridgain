package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionRequest;

import java.util.UUID;

/**
 * Statistics collection request with target node id.
 */
public class StatsCollectionAddrRequest {
    /** Wrapped request. */
    private final StatsCollectionRequest req;

    /** Destination node id. */
    private final UUID targetNodeId;

    /**
     * Constructor.
     *
     * @param req Wrapped request.
     * @param targetNodeId Target node id.
     */
    public StatsCollectionAddrRequest(StatsCollectionRequest req, UUID targetNodeId) {
        this.req = req;
        this.targetNodeId = targetNodeId;
    }

    /**
     * @return Wrapped request.
     */
    public StatsCollectionRequest req() {
        return req;
    }

    /**
     * @return Target node id.
     */
    public UUID nodeId() {
        return targetNodeId;
    }
}