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

import java.util.UUID;

/**
 * Statistics request with target node id.
 */
public class StatisticsAddrRequest<T> {
    /** Wrapped request. */
    private final T req;

    /** Destination node id. */
    private final UUID targetNodeId;

    /** Sender node id. */
    private final UUID senderNodeId;

    /**
     * Constructor.
     *
     * @param req Wrapped request.
     * @param senderNodeId Sender node id.
     * @param targetNodeId Target node id.
     */
    public StatisticsAddrRequest(T req, UUID senderNodeId, UUID targetNodeId) {
        this.req = req;
        this.senderNodeId = senderNodeId;
        this.targetNodeId = targetNodeId;
    }

    /**
     * @return Wrapped request.
     */
    public T req() {
        return req;
    }

    /**
     * @return Sender node id.
     */
    public UUID senderNodeId() {
        return senderNodeId;
    }

    /**
     * @return Target node id.
     */
    public UUID targetNodeId() {
        return targetNodeId;
    }
}
