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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 * Cache group metrics.
 */
public class CacheGroupMetricsImpl {
    /** Cache group metrics prefix. */
    public static final String CACHE_GROUP_METRICS_PREFIX = "cacheGroups";

    /** Number of partitions need processed for finished indexes create or rebuilding. */
    private final AtomicLongMetric idxBuildCntPartitionsLeft;

    /** Cache group context. */
    private final CacheGroupContext ctx;

    /** */
    private final LongMetric storageSize;

    /** */
    private final LongMetric sparseStorageSize;

    /** Number of local partitions initialized on current node. */
    private final AtomicLongMetric initLocPartitionsNum;

    /**
     * Memory page metrics. Will be {@code null} on client nodes.
     */
    @Nullable
    private final PageMetrics pageMetrics;

    /** */
    public CacheGroupMetricsImpl(CacheGroupContext ctx) {
        this.ctx = ctx;

        CacheConfiguration<?, ?> cacheCfg = ctx.config();

        GridKernalContext kernalCtx = ctx.shared().kernalContext();

        DataStorageConfiguration dsCfg = kernalCtx.config().getDataStorageConfiguration();

        boolean persistenceEnabled = !kernalCtx.clientNode() && CU.isPersistentCache(cacheCfg, dsCfg);

        MetricRegistry mreg = kernalCtx.metric().registry(metricGroupName());

        mreg.registerOrReplace("Caches", this::getCaches, List.class, null);

        storageSize = mreg.registerOrReplace("StorageSize",
            () -> persistenceEnabled ? database().forGroupPageStores(ctx, PageStore::size) : 0,
            "Storage space allocated for group, in bytes.");

        sparseStorageSize = mreg.registerOrReplace("SparseStorageSize",
            () -> persistenceEnabled ? database().forGroupPageStores(ctx, PageStore::getSparseSize) : 0,
            "Storage space allocated for group adjusted for possible sparsity, in bytes.");

        idxBuildCntPartitionsLeft = mreg.longMetric("IndexBuildCountPartitionsLeft",
            "Number of partitions need processed for finished indexes create or rebuilding.");

        initLocPartitionsNum = mreg.longMetric("InitializedLocalPartitionsNumber",
            "Number of local partitions initialized on current node.");

        // disable memory page metrics for client nodes (dataRegion is null on client nodes)
        pageMetrics = ctx.dataRegion() == null ?
            null :
            ctx.dataRegion().metrics().cacheGrpPageMetrics(ctx.groupId());
    }

    /** Callback for initializing metrics after topology was initialized. */
    public void onTopologyInitialized() {
        MetricRegistry mreg = ctx.shared().kernalContext().metric().registry(metricGroupName());

        mreg.registerOrReplace("MinimumNumberOfPartitionCopies",
            this::getMinimumNumberOfPartitionCopies,
            "Minimum number of partition copies for all partitions of this cache group.");

        mreg.registerOrReplace("MaximumNumberOfPartitionCopies",
            this::getMaximumNumberOfPartitionCopies,
            "Maximum number of partition copies for all partitions of this cache group.");

        mreg.registerOrReplace("LocalNodeOwningPartitionsCount",
            this::getLocalNodeOwningPartitionsCount,
            "Count of partitions with state OWNING for this cache group located on this node.");

        mreg.registerOrReplace("LocalNodeMovingPartitionsCount",
            this::getLocalNodeMovingPartitionsCount,
            "Count of partitions with state MOVING for this cache group located on this node.");

        mreg.registerOrReplace("LocalNodeRentingPartitionsCount",
            this::getLocalNodeRentingPartitionsCount,
            "Count of partitions with state RENTING for this cache group located on this node.");

        mreg.registerOrReplace("LocalNodeRentingEntriesCount",
            this::getLocalNodeRentingEntriesCount,
            "Count of entries remains to evict in RENTING partitions located on this node for this cache group.");

        mreg.registerOrReplace("OwningPartitionsAllocationMap",
            this::getOwningPartitionsAllocationMap,
            Map.class,
            "Allocation map of partitions with state OWNING in the cluster.");

        mreg.registerOrReplace("MovingPartitionsAllocationMap",
            this::getMovingPartitionsAllocationMap,
            Map.class,
            "Allocation map of partitions with state MOVING in the cluster.");

        mreg.registerOrReplace("AffinityPartitionsAssignmentMap",
            this::getAffinityPartitionsAssignmentMap,
            Map.class,
            "Affinity partitions assignment map.");

        mreg.registerOrReplace("PartitionIds",
            this::getPartitionIds,
            List.class,
            "Local partition ids.");

        mreg.registerOrReplace("TotalAllocatedSize",
            this::getTotalAllocatedSize,
            "Total size of memory allocated for group, in bytes.");

        mreg.registerOrReplace("Tombstones",
            this::getTombstones,
            "Number of tombstone entries.");

        if (ctx.config().isEncryptionEnabled()) {
            mreg.registerOrReplace("ReencryptionFinished",
                () -> !ctx.shared().kernalContext().encryption().reencryptionInProgress(ctx.groupId()),
                "The flag indicates whether reencryption is finished or not.");

            mreg.registerOrReplace("ReencryptionBytesLeft",
                () -> ctx.shared().kernalContext().encryption().getBytesLeftForReencryption(ctx.groupId()),
                "The number of bytes left for re-ecryption.");
        }
    }

    /** */
    public long getIndexBuildCountPartitionsLeft() {
        return idxBuildCntPartitionsLeft.value();
    }

    /** Decrement number of partitions need processed for finished indexes create or rebuilding. */
    public void decrementIndexBuildCountPartitionsLeft() {
        idxBuildCntPartitionsLeft.decrement();
    }

    /**
     * Add number of partitions before processed indexes create or rebuilding.
     * @param partitions Count partition for add.
     */
    public void addIndexBuildCountPartitionsLeft(long partitions) {
        idxBuildCntPartitionsLeft.add(partitions);
    }

    /** Increments number of local partitions initialized on current node. */
    public void incrementInitializedLocalPartitions() {
        initLocPartitionsNum.increment();
    }

    /** Decrements number of local partitions initialized on current node. */
    public void decrementInitializedLocalPartitions() {
        initLocPartitionsNum.decrement();
    }

    /** */
    public int getGroupId() {
        return ctx.groupId();
    }

    /** */
    public String getGroupName() {
        return ctx.name();
    }

    /** */
    public List<String> getCaches() {
        List<String> caches = new ArrayList<>(ctx.caches().size());

        for (GridCacheContext cache : ctx.caches())
            caches.add(cache.name());

        Collections.sort(caches);

        return caches;
    }

    /** */
    public int getBackups() {
        return ctx.config().getBackups();
    }

    /** */
    public int getPartitions() {
        return ctx.topology().partitions();
    }

    /**
     * Calculates the number of partition copies for all partitions of this cache group and filter values by the
     * predicate.
     *
     * @param loc Local falg.
     * @param conv Predicate.
     */
    private int numberOfPartitionCopies(boolean loc, BiFunction<Integer, Integer, Integer> conv) {
        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(false);

        if (partFullMap == null)
            return 0;

        int parts = ctx.topology().partitions();

        int res = -1;

        for (int part = 0; part < parts; part++) {
            if (loc && (ctx.topology().localPartition(part) == null ||
                ctx.topology().localPartition(part).state() != GridDhtPartitionState.OWNING))
                continue;

            int cnt = 0;

            for (Map.Entry<UUID, GridDhtPartitionMap> entry : partFullMap.entrySet()) {
                if (entry.getValue().get(part) == GridDhtPartitionState.OWNING)
                    cnt++;
            }

            if (res == -1)
                res = cnt;

            res = conv.apply(res, cnt);
        }

        return res;
    }

    /** */
    public int getMinimumNumberOfPartitionCopies() {
        return numberOfPartitionCopies(false, Math::min);
    }

    /** */
    public int getMaximumNumberOfPartitionCopies() {
        return numberOfPartitionCopies(false, Math::max);
    }

    /** */
    public int getLocalNodeMinimumNumberOfPartitionCopies() {
        return numberOfPartitionCopies(true, Math::min);
    }

    /**
     * Count of partitions with a given state on the node.
     *
     * @param nodeId Node id.
     * @param state State.
     */
    private int nodePartitionsCountByState(UUID nodeId, GridDhtPartitionState state) {
        int parts = ctx.topology().partitions();

        GridDhtPartitionMap partMap = ctx.topology().partitionMap(false).get(nodeId);

        int cnt = 0;

        for (int part = 0; part < parts; part++)
            if (partMap.get(part) == state)
                cnt++;

        return cnt;
    }

    /**
     * Count of partitions with a given state in the entire cluster.
     *
     * @param state State.
     */
    private int clusterPartitionsCountByState(GridDhtPartitionState state) {
        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(true);

        int cnt = 0;

        for (UUID nodeId : partFullMap.keySet())
            cnt += nodePartitionsCountByState(nodeId, state);

        return cnt;
    }

    /**
     * Count of partitions with a given state on the local node.
     *
     * @param state State.
     */
    private int localNodePartitionsCountByState(GridDhtPartitionState state) {
        int cnt = 0;

        for (GridDhtLocalPartition part : ctx.topology().localPartitions()) {
            if (part.state() == state)
                cnt++;
        }

        return cnt;
    }

    /** */
    public int getLocalNodeOwningPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /** */
    public int getLocalNodeMovingPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /** */
    public int getLocalNodeRentingPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.RENTING);
    }

    /** */
    public long getLocalNodeRentingEntriesCount() {
        long entriesCnt = 0;

        for (GridDhtLocalPartition part : ctx.topology().localPartitions()) {
            if (part.state() == GridDhtPartitionState.RENTING)
                entriesCnt += part.dataStore().fullSize();
        }

        return entriesCnt;
    }

    /** */
    public int getClusterOwningPartitionsCount() {
        return clusterPartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /** */
    public int getClusterMovingPartitionsCount() {
        return clusterPartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /**
     * Gets partitions allocation map with a given state.
     *
     * @param state State.
     * @return Partitions allocation map.
     */
    private Map<Integer, Set<String>> clusterPartitionsMapByState(GridDhtPartitionState state) {
        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(false);

        if (partFullMap == null)
            return Collections.emptyMap();

        int parts = ctx.topology().partitions();

        Map<Integer, Set<String>> partsMap = new LinkedHashMap<>();

        for (int part = 0; part < parts; part++) {
            Set<String> partNodesSet = new HashSet<>();

            for (Map.Entry<UUID, GridDhtPartitionMap> entry : partFullMap.entrySet()) {
                if (entry.getValue().get(part) == state)
                    partNodesSet.add(entry.getKey().toString());
            }

            partsMap.put(part, partNodesSet);
        }

        return partsMap;
    }

    /** */
    public Map<Integer, Set<String>> getOwningPartitionsAllocationMap() {
        return clusterPartitionsMapByState(GridDhtPartitionState.OWNING);
    }

    /** */
    public Map<Integer, Set<String>> getMovingPartitionsAllocationMap() {
        return clusterPartitionsMapByState(GridDhtPartitionState.MOVING);
    }

    /** */
    public Map<Integer, List<String>> getAffinityPartitionsAssignmentMap() {
        if (ctx.affinity().lastVersion().topologyVersion() < 0)
            return Collections.emptyMap();

        AffinityAssignment assignment = ctx.affinity().cachedAffinity(AffinityTopologyVersion.NONE);

        int part = 0;

        Map<Integer, List<String>> assignmentMap = new LinkedHashMap<>();

        for (List<ClusterNode> partAssignment : assignment.assignment()) {
            List<String> partNodeIds = new ArrayList<>(partAssignment.size());

            for (ClusterNode node : partAssignment)
                partNodeIds.add(node.id().toString());

            assignmentMap.put(part, partNodeIds);

            part++;
        }

        return assignmentMap;
    }

    /** */
    public String getType() {
        CacheMode type = ctx.config().getCacheMode();

        return String.valueOf(type);
    }

    /** */
    public List<Integer> getPartitionIds() {
        List<GridDhtLocalPartition> parts = ctx.topology().localPartitions();

        List<Integer> partsRes = new ArrayList<>(parts.size());

        for (GridDhtLocalPartition part : parts)
            partsRes.add(part.id());

        return partsRes;
    }

    /** */
    public long getTotalAllocatedPages() {
        return pageMetrics == null ? 0 : pageMetrics.totalPages().value();
    }

    /** */
    public long getTotalAllocatedSize() {
        return ctx.shared().kernalContext().clientNode() ?
            0 :
            getTotalAllocatedPages() * ctx.dataRegion().pageMemory().pageSize();
    }

    /** */
    public long getStorageSize() {
        return storageSize == null ? 0 : storageSize.value();
    }

    /** */
    public long getSparseStorageSize() {
        return sparseStorageSize == null ? 0 : sparseStorageSize.value();
    }

    /** */
    public long getTombstones() {
        return ctx.offheap().tombstonesCount();
    }

    /** Removes all metric for cache group. */
    public void remove() {
        if (ctx.shared().kernalContext().isStopping())
            return;

        if (ctx.config().getNearConfiguration() != null)
            ctx.shared().kernalContext().metric().remove(cacheMetricsRegistryName(ctx.config().getName(), true));

        ctx.shared().kernalContext().metric().remove(cacheMetricsRegistryName(ctx.config().getName(), false));

        ctx.shared().kernalContext().metric().remove(metricGroupName());
    }

    /**
     * @return Database.
     */
    private GridCacheDatabaseSharedManager database() {
        return (GridCacheDatabaseSharedManager)ctx.shared().database();
    }

    /** @return Metric group name. */
    private String metricGroupName() {
        return MetricUtils.cacheGroupMetricsRegistryName(ctx.cacheOrGroupName());
    }
}
