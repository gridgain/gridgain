/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Base interface for handling {@link ShutdownPolicy}.
 */
public interface ShutdownPolicyHandler {
    /**
     * Handle shutdown policy.
     */
    void handle();

    /**
     * Allows to interrupt handling process.
     */
    void stopHandling();

    /**
     * Cleanup meta storage keys that are related to the shutdown policies.
     *
     * @param metaStorage Distributed meta storage.
     */
    static void cleanupOnActive(DistributedMetaStorage metaStorage) {
        try {
            metaStorage.remove(GracefulShutdownPolicyHandler.GRACEFUL_SHUTDOWN_METASTORE_KEY);
            metaStorage.remove(GracefulShutdownPolicyHandler.GRACEFUL_CLUSTER_SHUTDOWN_METASTORE_KEY);
        }
        catch (IgniteException | IgniteCheckedException e) {
            // No-op.
        }
    }

    /**
     *
     * @param shutdownPolicy Shutdown policy to create handler for.
     * @param grid0 Ignite instance to handle shutdown for.
     * @param log Logger to use.
     * @return Shutdown policy handler.
     * @throws IgniteException If unknown shutdown policy is provided.
     */
    static ShutdownPolicyHandler create(
        ShutdownPolicy shutdownPolicy,
        IgniteKernal grid0,
        IgniteLogger log
    ) {
        switch (shutdownPolicy) {
            case GRACEFUL: return new GracefulShutdownPolicyHandler(grid0, log);
            case IMMEDIATE: return new ImmediateShutdownPolicyHandler();

            default:
                throw new IgniteException("Unknown shutdown policy [plc=" + shutdownPolicy + ']');
        }
    }

    /** Immediate policy handler. */
    class ImmediateShutdownPolicyHandler implements ShutdownPolicyHandler {
        /** {@inheritDoc} */
        @Override public void handle() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void stopHandling() {
            // No-op.
        }
    }

    /** Graceful policy handler. */
    class GracefulShutdownPolicyHandler implements ShutdownPolicyHandler {
        /** Key to store list of gracefully stopping nodes within metastore. */
        static final String GRACEFUL_SHUTDOWN_METASTORE_KEY =
            DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown";

        /** Meta storage key to store a list of server node Ids that are trying to stop. */
        static final String GRACEFUL_CLUSTER_SHUTDOWN_METASTORE_KEY =
            DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown.intention";

        /** */
        static final int WAIT_FOR_BACKUPS_CHECK_INTERVAL = 1000;

        /** Grid to stop. */
        private final IgniteKernal grid0;

        /**
         * Flag to indicate if shutdown is delayed.
         * This flag can be changed in parallel by calling {@link IgnitionEx#stop(boolean, ShutdownPolicy)}
         * using {@link ShutdownPolicy#IMMEDIATE} policy.
         **/
        private volatile boolean delayedShutdown = true;

        /** Logger to use. */
        private final IgniteLogger log;

        /**
         * Creates a new instance of {@link GracefulShutdownPolicyHandler}.
         *
         * @param grid0 Node to stop.
         * @param log Logger to use.
         */
        GracefulShutdownPolicyHandler(
            IgniteKernal grid0,
            IgniteLogger log
        ) {
            this.grid0 = grid0;
            this.log = log;
        }

        /** {@inheritDoc} */
        @Override public void stopHandling() {
            delayedShutdown = false;
        }

        /**
         * Handles {@link ShutdownPolicy#GRACEFUL} policy.
         * Checks that all caches have sufficient number of backups and doesn't allow stopping the {@code grid0} node if
         * that might lead to data unavailability.
         *
         * This method does nothing if one of the following conditions is met:
         * <ul>
         *     <li>the node is a client</li>
         *     <li>cluster is inactive</li>
         *</ul>
         */
        @Override public void handle() {
            if (grid0.context().clientNode() || !ClusterState.active(grid0.cluster().state()))
                return;

            if (log.isInfoEnabled())
                log.info("Ensuring that caches have sufficient backups and local rebalance completion...");

            DistributedMetaStorage metaStorage = grid0.context().distributedMetastorage();

            while (delayedShutdown) {
                boolean safeToStop = true;

                long topVer = grid0.cluster().topologyVersion();

                switch (handleClusterShutdownIntention(metaStorage, topVer)) {
                    case RETRY:
                        continue;
                    case SAFE_TO_STOP:
                        return;
                    case SAFE_TO_PROCEED:
                        break;
                }

                HashSet<UUID> originalNodesToExclude;
                HashSet<UUID> nodesToExclude;

                try {
                    originalNodesToExclude = metaStorage.read(GRACEFUL_SHUTDOWN_METASTORE_KEY);

                    nodesToExclude = originalNodesToExclude != null ? new HashSet<>(originalNodesToExclude) :
                        new HashSet<>();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Unable to read '" + GRACEFUL_SHUTDOWN_METASTORE_KEY +
                        "' value from metastore.", e);

                    continue;
                }

                Map<UUID, Map<Integer, Set<Integer>>> proposedSuppliers = new HashMap<>();

                for (CacheGroupContext grpCtx : grid0.context().cache().cacheGroups()) {
                    if (grpCtx.isLocal() || grpCtx.systemCache())
                        continue;

                    if (grpCtx.config().getCacheMode() == PARTITIONED && grpCtx.config().getBackups() == 0) {
                        LT.warn(log, "Ignoring potential data loss on cache without backups [name="
                            + grpCtx.cacheOrGroupName() + "]");

                        continue;
                    }

                    if (topVer != grpCtx.topology().readyTopologyVersion().topologyVersion()) {
                        // At the moment, there is an exchange.
                        safeToStop = false;

                        break;
                    }

                    GridDhtPartitionFullMap fullMap = grpCtx.topology().partitionMap(false);

                    if (fullMap == null) {
                        safeToStop = false;

                        break;
                    }

                    nodesToExclude.retainAll(fullMap.keySet());

                    if (!haveCopyLocalPartitions(grpCtx, nodesToExclude, proposedSuppliers)) {
                        safeToStop = false;

                        if (log.isInfoEnabled()) {
                            LT.info(log, "This node is waiting for backups of local partitions for group [id="
                                + grpCtx.groupId() + ", name=" + grpCtx.cacheOrGroupName() + "]");
                        }

                        break;
                    }

                    if (!isRebalanceCompleted(grpCtx)) {
                        safeToStop = false;

                        if (log.isInfoEnabled()) {
                            LT.info(log, "This node is waiting for completion of rebalance for group [id="
                                + grpCtx.groupId() + ", name=" + grpCtx.cacheOrGroupName() + "]");
                        }

                        break;
                    }
                }

                if (topVer != grid0.cluster().topologyVersion())
                    safeToStop = false;

                if (safeToStop && !proposedSuppliers.isEmpty()) {
                    Set<UUID> supportedPolicyNodes = new HashSet<>();

                    for (UUID nodeId : proposedSuppliers.keySet()) {
                        if (IgniteFeatures.nodeSupports(grid0.context(), grid0.cluster().node(nodeId), IgniteFeatures.SHUTDOWN_POLICY))
                            supportedPolicyNodes.add(nodeId);
                    }

                    if (!supportedPolicyNodes.isEmpty()) {
                        try {
                            safeToStop = grid0
                                .compute(grid0.cluster().forNodeIds(supportedPolicyNodes))
                                .execute(CheckCpHistTask.class, proposedSuppliers);
                        }
                        catch (ClusterTopologyException cte) {
                            // Topology has changed or cluster group is empty.
                            safeToStop = false;
                        }
                    }
                }

                if (safeToStop) {
                    try {
                        HashSet<UUID> newNodesToExclude = new HashSet<>(nodesToExclude);
                        newNodesToExclude.add(grid0.getLocalNodeId());

                        if (metaStorage.compareAndSet(GRACEFUL_SHUTDOWN_METASTORE_KEY, originalNodesToExclude,
                            newNodesToExclude))
                            break;
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Unable to write '" + GRACEFUL_SHUTDOWN_METASTORE_KEY +
                            "' value to metastore.", e);

                        continue;
                    }
                }

                try {
                    IgniteUtils.sleep(WAIT_FOR_BACKUPS_CHECK_INTERVAL);
                }
                catch (IgniteInterruptedCheckedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        /** Represents a result returned by {@link #handleClusterShutdownIntention(DistributedMetaStorage, long)}. */
        private enum ShutdownIntentionResult {
            /** Meta storage key {@link #GRACEFUL_CLUSTER_SHUTDOWN_METASTORE_KEY} cannot be read or wrote. */
            RETRY,

            /** All server nodes wrote the intention and cluster can be stopped. */
            SAFE_TO_STOP,

            /** Server node updated its own intention. */
            SAFE_TO_PROCEED
        }

        /**
         * Handles cluster shutdown intention.
         *
         * @param metaStorage Meta storage.
         * @param topVer Current topology version.
         * @return Result of handling.
         */
        private ShutdownIntentionResult handleClusterShutdownIntention(
            DistributedMetaStorage metaStorage,
            long topVer
        ) {
            HashSet<UUID> originalClusterShutdownIntention;
            HashSet<UUID> clusterShutdownIntention;

            try {
                originalClusterShutdownIntention = metaStorage.read(GRACEFUL_CLUSTER_SHUTDOWN_METASTORE_KEY);

                clusterShutdownIntention = originalClusterShutdownIntention == null ?
                    new HashSet<>() :
                    new HashSet<>(originalClusterShutdownIntention);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Unable to read '" + GRACEFUL_CLUSTER_SHUTDOWN_METASTORE_KEY +
                    "' value from metastore.", e);

                return ShutdownIntentionResult.RETRY;
            }

            if (!clusterShutdownIntention.contains(grid0.getLocalNodeId())) {
                try {
                    clusterShutdownIntention.add(grid0.getLocalNodeId());

                    boolean updated = metaStorage.compareAndSet(
                        GRACEFUL_CLUSTER_SHUTDOWN_METASTORE_KEY,
                        originalClusterShutdownIntention,
                        clusterShutdownIntention);

                    if (!updated)
                        return ShutdownIntentionResult.RETRY;
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Unable to write '" + GRACEFUL_CLUSTER_SHUTDOWN_METASTORE_KEY +
                        "' value to metastore.", e);

                    return ShutdownIntentionResult.RETRY;
                }
            }

            Set<UUID> actualIds = grid0
                .cluster()
                .forServers()
                .nodes()
                .stream()
                .map(ClusterNode::id)
                .collect(Collectors.toSet());

            if (clusterShutdownIntention.containsAll(actualIds) && topVer == grid0.cluster().topologyVersion()) {
                // it is safe to stop
                return ShutdownIntentionResult.SAFE_TO_STOP;
            }

            return ShutdownIntentionResult.SAFE_TO_PROCEED;
        }

        /**
         * Checks that the cluster has another copy of each local partition for the specified group.
         * Also, this method collects all nodes that can be used as suppliers for local partitions.
         *
         * @param grpCtx Cache group.
         * @param nodesToExclude Nodes to exclude from check.
         * @param proposedSuppliers Map of proposed suppliers for cache groups.
         * @return {@code true} if all local partitions of the specified cache group have a copy in the cluster,
         *                      and {@code false} otherwise.
         */
        private boolean haveCopyLocalPartitions(
            CacheGroupContext grpCtx,
            Set<UUID> nodesToExclude,
            Map<UUID, Map<Integer, Set<Integer>>> proposedSuppliers
        ) {
            GridDhtPartitionFullMap fullMap = grpCtx.topology().partitionMap(false);

            if (fullMap == null)
                return false;

            UUID localNodeId = grid0.getLocalNodeId();

            GridDhtPartitionMap localPartMap = fullMap.get(localNodeId);

            int parts = grpCtx.topology().partitions();

            List<List<ClusterNode>> idealAssignment = grpCtx.affinity().idealAssignmentRaw();

            for (int p = 0; p < parts; p++) {
                if (localPartMap.get(p) != GridDhtPartitionState.OWNING)
                    continue;

                boolean foundCopy = false;

                for (Map.Entry<UUID, GridDhtPartitionMap> entry : fullMap.entrySet()) {
                    if (localNodeId.equals(entry.getKey()) || nodesToExclude.contains(entry.getKey()))
                        continue;

                    // This remote node does not present in ideal assignment.
                    if (!idealAssignment.get(p).stream().anyMatch(node -> node.id().equals(entry.getKey())))
                        continue;

                    // Rebalance is in progress.
                    if (entry.getValue().hasMovingPartitions())
                        continue;

                    if (entry.getValue().get(p) == GridDhtPartitionState.OWNING) {
                        proposedSuppliers.computeIfAbsent(entry.getKey(), (nodeId) -> new HashMap<>())
                            .computeIfAbsent(grpCtx.groupId(), grpId -> new HashSet<>())
                            .add(p);

                        foundCopy = true;
                    }
                }

                if (!foundCopy)
                    return false;
            }

            return true;
        }

        /**
         * Check is rebalance completed for specific group on this node or not.
         * It checks Demander and Supplier contexts.
         *
         * @param grpCtx Group context.
         * @return True if rebalance completed, false otherwise.
         */
        private boolean isRebalanceCompleted(CacheGroupContext grpCtx) {
            if (!grpCtx.preloader().rebalanceFuture().isDone())
                return false;

            grpCtx.preloader().pause();

            try {
                return !((GridDhtPreloader)grpCtx.preloader()).supplier().isSupply();
            }
            finally {
                grpCtx.preloader().resume();
            }
        }
    }
}
