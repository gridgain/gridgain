/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.ignite.events.EventType.EVT_PARTITIONS_STATE_VALIDATION_FAILED;
import static org.apache.ignite.events.EventType.EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED;

/**
 * Grid partitions validation event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by design Ignite keeps all events
 * generated on the local node locally and it provides APIs for performing a distributed queries across multiple nodes:
 * <ul>
 * <li>
 * {@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)} -
 * asynchronously querying events occurred on the nodes specified, including remote nodes.
 * </li>
 * <li>
 * {@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)} - querying only
 * local events stored on this local node.
 * </li>
 * <li>
 * {@link org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)} - listening to
 * local grid events (events from remote nodes not included).
 * </li>
 * </ul>
 * User can also wait for events using method {@link org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate,
 * int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored by whatever event storage
 * SPI is configured. Ignite can and often does generate thousands events per seconds under the load and therefore it
 * creates a significant additional load on the system. If these events are not needed by the application this load is
 * unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires by using {@link
 * org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration. Note that
 * certain events are required for Ignite's internal operations and such events will still be generated but not stored
 * by event storage SPI if they are disabled in Ignite configuration.
 *
 * @see EventType#EVT_PARTITIONS_STATE_VALIDATION_FAILED
 * @see EventType#EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED
 */
public class PartitionsStateValidationEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Map<String, Set<Integer>> parts;

    /** */
    private final AffinityTopologyVersion topVer;

    /**
     * Creates failed partition validation state event.
     *
     * @param node Node that raised this event.
     * @param parts Map of cache or group names to the set of partition ids which failed validation.
     * @param topVer Topology version on which the validation failed.
     * @return Partitions state validation event.
     */
    public static PartitionsStateValidationEvent failedEvent(ClusterNode node, Map<String, Set<Integer>> parts,
                                                             AffinityTopologyVersion topVer) {
        return new PartitionsStateValidationEvent(
            node,
            "Partitions state validation failed.",
            EVT_PARTITIONS_STATE_VALIDATION_FAILED,
            parts,
            topVer
        );
    }

    /**
     * Creates succeeded partition validation state event.
     *
     * @param node Node that raised this event.
     * @param topVer Topology version on which the validation succeeded.
     * @return Partitions state validation event.
     */
    public static PartitionsStateValidationEvent succeededEvent(ClusterNode node, AffinityTopologyVersion topVer) {
        return new PartitionsStateValidationEvent(
            node,
            "Partitions state validation succeeded.",
            EVT_PARTITIONS_STATE_VALIDATION_SUCCEEDED,
            Collections.emptyMap(),
            topVer
        );
    }

    /**
     * Creates partitions validation event with given parameters.
     *
     * @param node Node that raised this event.
     * @param msg Optional message.
     * @param type Event type.
     * @param parts Map of cache or group names to the set of partition ids which failed validation.
     * @param topVer Topology version on which the validation happened.
     */
    public PartitionsStateValidationEvent(ClusterNode node, String msg, int type, Map<String, Set<Integer>> parts,
                                          AffinityTopologyVersion topVer) {
        super(node, msg, type);
        this.parts = Collections.unmodifiableMap(parts);
        this.topVer = topVer;
    }

    /**
     * Gets map of partitions validation result.
     *
     * @return Map of cache or group names to the set of partition ids which failed validation.
     */
    public Map<String, Set<Integer>> parts() {
        return parts;
    }

    /**
     * Gets topology version.
     *
     * @return The topology version on which the event was triggered.
     */
    public AffinityTopologyVersion topVer() {
        return topVer;
    }
}
