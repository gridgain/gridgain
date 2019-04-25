/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.discovery.zk.internal;

/**
 *
 */
class ZkDiscoveryNodeFailEventData extends ZkDiscoveryEventData {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long failedNodeInternalId;

    /**
     * @param evtId Event ID.
     * @param topVer Topology version.
     * @param failedNodeInternalId Failed node ID.
     */
    ZkDiscoveryNodeFailEventData(long evtId, long topVer, long failedNodeInternalId) {
        super(evtId, ZK_EVT_NODE_FAILED, topVer);

        this.failedNodeInternalId = failedNodeInternalId;
    }

    /**
     * @return Failed node ID.
     */
    long failedNodeInternalId() {
        return failedNodeInternalId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ZkDiscoveryNodeFailEventData [" +
            "evtId=" + eventId() +
            ", topVer=" + topologyVersion() +
            ", nodeId=" + failedNodeInternalId + ']';
    }
}
