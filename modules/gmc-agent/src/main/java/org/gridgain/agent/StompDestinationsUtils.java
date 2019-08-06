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

package org.gridgain.agent;

import java.util.UUID;

/**
 * Stomp destinations utils
 */
public class StompDestinationsUtils {
    /** Cluster topology dest. */
    private static final String CLUSTER_PREFIX_DEST = "/app/agent/cluster";

    /** Cluster topology dest. */
    private static final String CLUSTER_TOPOLOGY_DEST = CLUSTER_PREFIX_DEST + "/cluster-topology/";

    /** Baseline topology dest. */
    private static final String BASELINE_TOPOLOGY_DEST = CLUSTER_PREFIX_DEST + "/baseline-topology/";

    /** Cluster active state dest. */
    private static final String CLUSTER_ACTIVE_STATE_DEST = CLUSTER_PREFIX_DEST + "/active-state/";

    /** Save span destination. */
    private static final String SAVE_SPAN_DEST = "/app/agent/spans/%s/add";

    /** Metrics destination. */
    private static final String METRICS_DEST = "/app/agent/metrics/%s/add";

    /**
     * @param clusterId Cluster id.
     * @return Cluster topology destination.
     */
    public static String buildClusterTopologyDest(UUID clusterId) {
        return CLUSTER_TOPOLOGY_DEST + clusterId;
    }

    /**
     * @param clusterId Cluster id.
     * @return Baseline topology destination.
     */
    public static String buildBaselineTopologyDest(UUID clusterId) {
        return BASELINE_TOPOLOGY_DEST + clusterId;
    }

    /**
     * @param clusterId Cluster id.
     * @return Cluster active state destination.
     */
    public static String buildClusterActiveStateDest(UUID clusterId) {
        return CLUSTER_ACTIVE_STATE_DEST + clusterId;
    }

    /**
     * @param clusterId Cluster id.
     * @return Save span destination.
     */
    public static String buildSaveSpanDest(UUID clusterId) {
        return String.format(SAVE_SPAN_DEST, clusterId);
    }

    /**
     * @return Metrics destination.
     */
    public static String buildMetricsDest(UUID clusterId) {
        return String.format(METRICS_DEST, clusterId);
    }

    /**
     * @return Metrics pull topic.
     */
    public static String buildMetricsPullTopic() {
        return "/topic/agent/metrics/pull";
    }

    /**
     * @return Add cluster destination.
     */
    public static String buildClusterAddDest() {
        return "/app/cluster/add";
    }
}
