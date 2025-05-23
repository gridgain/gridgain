/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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
import java.util.Collection;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.ru.IgniteRollingUpgradeStatus;
import org.apache.ignite.internal.processors.ru.RollingUpgradeStatus;
import org.apache.ignite.internal.processors.schedule.IgniteNoopScheduleProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_CLUSTER_ID_AND_TAG_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_PME_FREE_SWITCH_DISABLED;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_SPECIFIED_SEQ_PK_KEYS_DISABLED;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_USE_BACKWARD_COMPATIBLE_CONFIGURATION_SPLITTER;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;

/**
 * Defines supported features and check its on other nodes.
 *
 * Note: for creating a new feature and assigning an ID, please update the
 * <a href ="https://ggsystems.atlassian.net/wiki/spaces/GG/pages/1192198276/Community+edition+features+list">page</a>.
 */
public enum IgniteFeatures {
    /**
     * Support of {@link HandshakeWaitMessage} by {@link TcpCommunicationSpi}.
     */
    TCP_COMMUNICATION_SPI_HANDSHAKE_WAIT_MESSAGE(0),

    /** Cache metrics v2 support. */
    CACHE_METRICS_V2(1),

    /** Data packet compression. */
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

    /** Displaying verbose transaction information: --info option of --tx control script command. */
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

    /** Cluster has task to clear sender store. */
    WC_DR_CLEAR_SENDER_STORE(29),

    /** Distributed change timeout for dump long operations. */
    DISTRIBUTED_CHANGE_LONG_OPERATIONS_DUMP_TIMEOUT(30),

    /** Cluster has task to get value from cache by key value. */
    WC_GET_CACHE_VALUE(31),

    /** Partition Map Exchange-free switch on baseline node left at fully rebalanced cluster. */
    PME_FREE_SWITCH(32),

    /** */
    VOLATILE_DATA_STRUCTURES_REGION(33),

    /** Partition reconciliation utility. */
    PARTITION_RECONCILIATION(34),

    /** Inverse connection: sending a request over discovery to establish a communication connection. */
    INVERSE_TCP_CONNECTION(35),

    /** Check secondary indexes inline size on join/by control utility request. */
    CHECK_INDEX_INLINE_SIZES(36),

    /** Distributed propagation of tx collisions dump interval. */
    DISTRIBUTED_TX_COLLISIONS_DUMP(37),

    /** */
    METASTORAGE_LONG_KEYS(38),

    /** Remove metadata from cluster for specified type. */
    REMOVE_METADATA(39),

    /** Support policy of shutdown. */
    SHUTDOWN_POLICY(40),

    /** New security processor with a security context support. */
    IGNITE_SECURITY_PROCESSOR_V2(41),

    /** Force rebuild, list or request indexes rebuild status from control script. */
    INDEXES_MANIPULATIONS_FROM_CONTROL_SCRIPT(42),

    /** Snapshots without PME. */
    EXCHANGELESS_SNAPSHOT(43),

    /** Optimization of recovery protocol for cluster which doesn't contain MVCC caches. */
    MVCC_TX_RECOVERY_PROTOCOL_V2(44),

    /** Pk index keys are applied in correct order. */
    SPECIFIED_SEQ_PK_KEYS(45),

    /** Compatibility support for new fields which are configured split. */
    SPLITTED_CACHE_CONFIGURATIONS_V2(46),

    /** Snapshots upload via sftp. */
    SNAPSHOT_SFTP_UPLOAD(47),

    /** Master key change. See {@link GridEncryptionManager#changeMasterKey(String)}. */
    MASTER_KEY_CHANGE(48),

    /** Incremental DR. */
    INCREMENTAL_DR(49),

    /** Cache encryption key change. See {@link IgniteEncryption#changeCacheGroupKey(Collection)}. */
    CACHE_GROUP_KEY_CHANGE(50),

    /** Possibility to safe deactivation, take into account pure in memory caches with possible data loss.*/
    SAFE_CLUSTER_DEACTIVATION(51),

    /** Transaction distributed configuration. */
    TRANSACTION_DISTRIBUTED_PROPERTIES(52),

    /** Custom snapshot operations. */
    CUSTOM_SNAPSHOT_OPERATIONS(53),

    /** Point-in-time distributed property. */
    POINT_IN_TIME_DISTRIBUTED_PROPERTY(54),

    /** Statistics collection. */
    STATISTICS_COLLECTION(55),

    /** Warning is shown in server log and after schedule and snapshot commands if PITR is enabled and snapshot schedule
     * is improper for PITR*/
    IMPROPER_SCHEDULE_FOR_PITR_WARNING(56),

    /** Node supports capturing incremental snapshot if previous snapshots mismatch from ones on another nodes. */
    SNAPSHOT_LAST_SNAPSHOTS_MISMATCH_HANDLING_POLICY(57),

    /** Rolling upgrade based on distributed metastorage. Improved handling of changing RU state. */
    DISTRIBUTED_ROLLING_UPGRADE_MODE_V2(58),

    /** Chains of snapshot operations. */
    SNAPSHOT_OPERATIONS_CHAINING(59),

    /** Previous snapshot SFTP upload had a race condition, so it must be disabled in mixed-cluster with older versions. */
    SNAPSHOT_SFTP_UPLOAD_V2(60),

    /**
     * Snapshot operation can handle {@code -excluded_caches} parameter
     * and cache groups in {@code --caches} and {@code --excluded_caches} parameters.
     */
    SNAPSHOT_OPERATION_WITH_EXCLUDE_AND_GROUPS_FILTER(62),

    /**
     * The feature allows authenticating a node remotely.
     * It is need when the one node which does not have an authentication required to authorize itself or another one.
     */
    REMOTE_AUTH(61),

    /** Snapshot operations with ZSTD, LZ4, SNAPPY algorithms. */
    SNAPSHOT_COMPRESSION_EXTENDED_OPTION(63),

    /** Whether the absent PK parts should be filled with defaults or not. */
    FILLS_ABSENT_PKS_WITH_DEFAULTS(64),

    /** Whether there is {@link InvalidUserCommandException} class. */
    HAS_INVALID_USER_COMMAND_EXCEPTION(65),

    /** This feature indicates that {@link CacheAffinityChangeMessage} is mutable. */
    MUTABLE_CACHE_AFFINITY_CHANGE_MESSAGE(66),

    /** New DR FST commands handling approach. */
    NEW_DR_FST_COMMANDS(67),

    /** This feature enables checking a cache generation for incoming cache messages (see GridCacheIdMessage). */
    CHECK_CACHE_GENERATION(68),

    /** This feature enables attributes to be included into TaskEvents. */
    TASK_EVT_ATTRIBUTE_SUPPORT(69),

    /** Allows to node check connections with special heartbeat message to detect network issues rather quickly. */
    TCP_COMMUNICATION_HEARTBEATS(70),

    /** Optimization of compression for encrypted snapshots. */
    OPTIMIZED_COMPRESSED_ENCRYPTED_SNAPSHOTS(71),

    /** This feature allows using exchangeless snapshots for point-in-time recovery. */
    POINT_IN_TIME_RECOVERY_EXCHANGELESS_SUPPORT(72),

    /** Enables compacted topology history. */
    TCP_DISCOVERY_COMPACTED_TOPOLOGY_HISTORY(73),

    /** New partition reconciliation algorithms. */
    PARTITION_RECONCILIATION_LATEST_ALG_UPDATE(74);

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
                return nodeSupports(((IgniteRollingUpgradeStatus)status).supportedFeatures(), feature);
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
    public static boolean allNodesSupports(
        @Nullable GridKernalContext ctx,
        Iterable<? extends ClusterNode> nodes,
        IgniteFeatures feature
    ) {
        if (ctx != null && nodes.iterator().hasNext()) {
            RollingUpgradeStatus status = ctx.rollingUpgrade().getStatus();

            if (status.enabled() && !status.forcedModeEnabled() && status instanceof IgniteRollingUpgradeStatus)
                return nodeSupports(((IgniteRollingUpgradeStatus)status).supportedFeatures(), feature);
        }

        for (ClusterNode next : nodes) {
            if (!nodeSupports(next.attribute(ATTR_IGNITE_FEATURES), feature))
                return false;
        }

        return true;
    }

    /**
     * @param ctx Kernal context.
     * @param feature Feature to check.
     *
     * @return {@code True} if all nodes in the cluster support given feature.
     */
    public static boolean allNodesSupport(GridKernalContext ctx, IgniteFeatures feature) {
        return allNodesSupport(ctx, ctx.config().getDiscoverySpi(), feature);
    }

    /**
     * @param ctx Kernal context (can be {@code null}).
     * @param discoSpi Instance of {@link DiscoverySpi}.
     * @param feature Feature to check.
     * @return {@code True} if all nodes in the cluster support given feature.
     */
    public static boolean allNodesSupport(@Nullable GridKernalContext ctx, DiscoverySpi discoSpi, IgniteFeatures feature) {
        return allNodesSupport(ctx, discoSpi, feature, F.alwaysTrue());
    }

    /**
     * Check that feature is supported by all nodes passing the provided predicate.
     *
     * @param ctx Kernal context.
     * @param feature Feature to check.
     * @param pred Predicate to filter out nodes that should not be checked for feature support.
     * @return {@code True} if all nodes passed the predicate support the feature.
     */
    public static boolean allNodesSupport(GridKernalContext ctx, IgniteFeatures feature, IgnitePredicate<ClusterNode> pred) {
        return allNodesSupport(ctx, ctx.config().getDiscoverySpi(), feature, pred);
    }

    /**
     * Check that feature is supported by all nodes passing the provided predicate.
     *
     * @param ctx Kernal context (can be null).
     * @param discoSpi Discovery SPI implementation.
     * @param feature Feature to check.
     * @param pred Predicate to filter out nodes that should not be checked for feature support.
     * @return {@code True} if all nodes passed the predicate support the feature.
     */
    public static boolean allNodesSupport(
        @Nullable GridKernalContext ctx,
        DiscoverySpi discoSpi,
        IgniteFeatures feature,
        IgnitePredicate<ClusterNode> pred
    ) {
        if (discoSpi instanceof IgniteDiscoverySpi)
            return ((IgniteDiscoverySpi)discoSpi).allNodesSupport(feature, pred);
        else
            return allNodesSupports(ctx, F.view(discoSpi.getRemoteNodes(), pred), feature);
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
            if (IGNITE_SECURITY_PROCESSOR == value && !getBoolean(IGNITE_SECURITY_PROCESSOR.name(), false))
                continue;

            if (IGNITE_SECURITY_PROCESSOR_V2 == value && !getBoolean(IGNITE_SECURITY_PROCESSOR_V2.name(), true))
                continue;

            // Add only when indexing is enabled.
            if (INDEXING == value && (ctx.query() == null || !ctx.query().moduleEnabled()))
                continue;

            // Add only when tracing is enabled.
            if (TRACING == value && !IgniteComponentType.TRACING.inClassPath())
                continue;

            // Add only when scheduling is disabled.
            if (WC_SCHEDULING_NOT_AVAILABLE == value && !(ctx.schedule() instanceof IgniteNoopScheduleProcessor))
                continue;

            if (DISTRIBUTED_METASTORAGE == value && !isFeatureEnabled(IGNITE_DISTRIBUTED_META_STORAGE_FEATURE))
                continue;

            if (CLUSTER_ID_AND_TAG == value && !isFeatureEnabled(IGNITE_CLUSTER_ID_AND_TAG_FEATURE))
                continue;

            if (BASELINE_AUTO_ADJUSTMENT == value && !isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
                continue;

            if (SPLITTED_CACHE_CONFIGURATIONS == value && isFeatureEnabled(IGNITE_USE_BACKWARD_COMPATIBLE_CONFIGURATION_SPLITTER))
                continue;

            if (PME_FREE_SWITCH == value && isFeatureEnabled(IGNITE_PME_FREE_SWITCH_DISABLED))
                continue;

            if (SPECIFIED_SEQ_PK_KEYS == value && isFeatureEnabled(IGNITE_SPECIFIED_SEQ_PK_KEYS_DISABLED))
                continue;

            final int featureId = value.getFeatureId();

            assert !set.get(featureId) : "Duplicate feature ID found for [" + value + "] having same ID ["
                + featureId + "]";

            set.set(featureId);
        }

        return set.toByteArray();
    }
}
