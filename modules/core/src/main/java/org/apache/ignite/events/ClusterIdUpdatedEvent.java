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
package org.apache.ignite.events;

import org.apache.ignite.cluster.ClusterNode;

import java.util.UUID;

/**
 * Event type indicating that cluster ID has been updated.
 *
 * @see EventType#EVT_CLUSTER_ID_UPDATED
 */
public class ClusterIdUpdatedEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Previous ID. */
    private final UUID previousId;

    /** New ID. */
    private final UUID newId;

    /**
     * @param node Node on which the event was fired.
     * @param msg Optional event message.
     * @param previousId Previous cluster ID replaced during update.
     * @param newId New cluster ID.
     */
    public ClusterIdUpdatedEvent(ClusterNode node, String msg, UUID previousId, UUID newId) {
        super(node, msg, EventType.EVT_CLUSTER_ID_UPDATED);
        this.previousId = previousId;
        this.newId = newId;
    }

    /**
     * Value of cluster ID before update request that triggered this event.
     *
     * @return Previous value of ID.
     */
    public UUID previousId() {
        return previousId;
    }

    /**
     * Value of cluster ID after update request that triggered this event.
     *
     * @return New value of ID.
     */
    public UUID newId() {
        return newId;
    }
}
