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

package org.apache.ignite.internal;

import java.util.BitSet;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.internal.processors.schedule.IgniteNoopScheduleProcessor;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;

/**
 * Defines supported features and check its on other nodes.
 */
public enum IgniteFeatures {
    /**
     * Support of {@link HandshakeWaitMessage} by {@link TcpCommunicationSpi}.
     */
    TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE(0),

    /** Cache metrics v2 support. */
    CACHE_METRICS_V2(1),

    /** Data paket compression. */
    DATA_PACKET_COMPRESSION(3),

    /** Support of different rebalance size for nodes.  */
    DIFFERENT_REBALANCE_POOL_SIZE(4),

    /** Support of splitted cache configurations to avoid broken deserialization on non-affinity nodes. */
    SPLITTED_CACHE_CONFIGURATIONS(5),

    /**
     * Support of providing thread dump of thread that started transaction. Used for dumping
     * long running transactions.
     */
    TRANSACTION_OWNER_THREAD_DUMP_PROVIDING(6),

    /** Displaying versbose transaction information: --info option of --tx control script command. */
    TX_INFO_COMMAND(7),

    /** Command which allow to detect and cleanup garbage which could left after destroying caches in shared groups */
    FIND_AND_DELETE_GARBAGE_COMMAND(8),

    /** Support of cluster read-only mode. */
    CLUSTER_READ_ONLY_MODE(9),

    /** Support of suspend/resume operations for pessimistic transactions. */
    SUSPEND_RESUME_PESSIMISTIC_TX(10),

    /** Distributed metastorage. */
    DISTRIBUTED_METASTORAGE(11),

    /** Supports tracking update counter for transactions. */
    TX_TRACKING_UPDATE_COUNTER(12),

    /** Support new security processor. */
    IGNITE_SECURITY_PROCESSOR(13),

    /** Replacing TcpDiscoveryNode field with nodeId field in discovery messages. */
    TCP_DISCOVERY_MESSAGE_NODE_COMPACT_REPRESENTATION(14),

    /** Indexing enabled. */
    INDEXING(15),

    /** Support of cluster ID and tag. */
    CLUSTER_ID_AND_TAG(16),

    /** LRT system and user time dump settings.  */
    LRT_SYSTEM_USER_TIME_DUMP_SETTINGS(18),

    /** A mode when data nodes throttle update rate regarding to DR sender load. */
    DR_DATA_NODE_SMART_THROTTLING(19),

    /** Support of DR events from  Web Console. */
    WC_DR_EVENTS(20),

    /**
     * Rolling upgrade based on distributed metastorage.
     */
    DISTRIBUTED_ROLLING_UPGRADE_MODE(21),

    /** Support of chain parameter in snapshot delete task for Web Console. */
    WC_SNAPSHOT_CHAIN_MODE(22),

    /** Support of baseline auto adjustment. */
    BASELINE_AUTO_ADJUSTMENT(23),

    /** Scheduling disabled. */
    WC_SCHEDULING_NOT_AVAILABLE(24),

    /** Support of DR-specific visor tasks used by control utility. */
    DR_CONTROL_UTILITY(25),

    /** */
    TRACING(26),

    /** */
    MANAGEMENT_CONSOLE(28),

    /** Cluster has task to clear sender store. */
    WC_DR_CLEAR_SENDER_STORE(29),

    /** Distributed change timeout for dump long operations. */
    DISTRIBUTED_CHANGE_LONG_OPERATIONS_DUMP_TIMEOUT(30),

    /** Cluster has task to get value from cache by key value. */
    WC_GET_CACHE_VALUE(31),

    /** Partition reconciliation utility. */
    PARTITION_RECONCILIATION(34);

    /**
     * Unique feature identifier.
     */
    private final int featureId;

    /**
     * @param featureId Feature ID.
     */
    IgniteFeatures(int featureId) {
        this.featureId = featureId;
    }

    /**
     * @return Feature ID.
     */
    public int getFeatureId() {
        return featureId;
    }

    /**
     * Checks that feature supported by node.
     *
     * @param ctx Kernal context.
     * @param clusterNode Cluster node to check.
     * @param feature Feature to check.
     * @return {@code True} if feature is declared to be supported by remote node.
     */
    public static boolean nodeSupports(GridKernalContext ctx, ClusterNode clusterNode, IgniteFeatures feature) {
        if (ctx != null) {
            RollingUpgradeStatus status = ctx.rollingUpgrade().getStatus();

            if (status.enabled() && !status.forcedModeEnabled())
                return status.supportedFeatures().contains(feature);
        }

        return nodeSupports(clusterNode.attribute(ATTR_IGNITE_FEATURES), feature);
    }

    /**
     * Checks that feature supported by node.
     *
     * @param featuresAttrBytes Byte array value of supported features node attribute.
     * @param feature Feature to check.
     * @return {@code True} if feature is declared to be supported by remote node.
     */
    public static boolean nodeSupports(byte[] featuresAttrBytes, IgniteFeatures feature) {
        if (featuresAttrBytes == null)
            return false;

        int featureId = feature.getFeatureId();

        // Same as "BitSet.valueOf(features).get(featureId)"

        int byteIdx = featureId >>> 3;

        if (byteIdx >= featuresAttrBytes.length)
            return false;

        int bitIdx = featureId & 0x7;

        return (featuresAttrBytes[byteIdx] & (1 << bitIdx)) != 0;
    }

    /**
     * Checks that feature supported by all nodes.
     *
     * @param ctx Kernal context.
     * @param nodes cluster nodes to check their feature support.
     * @return if feature is declared to be supported by all nodes
     */
    public static boolean allNodesSupports(GridKernalContext ctx, Iterable<ClusterNode> nodes, IgniteFeatures feature) {
        if (ctx != null && nodes.iterator().hasNext()) {
            RollingUpgradeStatus status = ctx.rollingUpgrade().getStatus();

            if (status.enabled() && !status.forcedModeEnabled())
                return status.supportedFeatures().contains(feature);
        }

        for (ClusterNode next : nodes) {
            if (!nodeSupports(next.attribute(ATTR_IGNITE_FEATURES), feature))
                return false;
        }

        return true;
    }

    /**
     * Features supported by the current node.
     *
     * @param ctx Kernal context.
     * @return Byte array representing all supported features by current node.
     */
    public static byte[] allFeatures(GridKernalContext ctx) {
        final BitSet set = new BitSet();

        for (IgniteFeatures value : IgniteFeatures.values()) {
            // After rolling upgrade, our security has more strict validation. This may come as a surprise to customers.
            if (IGNITE_SECURITY_PROCESSOR == value && !getBoolean(IGNITE_SECURITY_PROCESSOR.name(), true))
                continue;

            // Add only when indexing is enabled.
            if (INDEXING == value && !ctx.query().moduleEnabled())
                continue;

            // Add only when tracing is enabled.
            if (TRACING == value && !IgniteComponentType.TRACING.inClassPath())
                continue;

            // Add only when Control Center is enabled.
            if (MANAGEMENT_CONSOLE == value && !IgniteComponentType.MANAGEMENT_CONSOLE.inClassPath())
                continue;

            // Add only when scheduling is disabled.
            if (WC_SCHEDULING_NOT_AVAILABLE == value && !(ctx.schedule() instanceof IgniteNoopScheduleProcessor))
                continue;

            final int featureId = value.getFeatureId();

            assert !set.get(featureId) : "Duplicate feature ID found for [" + value + "] having same ID ["
                + featureId + "]";

            set.set(featureId);
        }

        return set.toByteArray();
    }
}
