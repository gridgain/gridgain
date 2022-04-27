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

package org.apache.ignite.internal.visor.dr;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.logMapped;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.TombstoneCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailMetricsAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryDetailMetricsKey;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.verify.VisorDrValidateCacheEntryJobResult;
import org.apache.ignite.internal.visor.verify.VisorDrValidateCacheTaskArg;
import org.apache.ignite.internal.visor.verify.VisorDrValidateCachesTaskResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task to collect cache query metrics.
 */
@GridInternal
public class VisorRepairPartitionCountersTask extends VisorMultiNodeTask<VisorDrValidateCacheTaskArg,
        VisorDrRepairPartitionCountersTaskResult, Collection<VisorDrValidateCacheEntryJobResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorDrValidateCacheTaskArg, Collection<VisorDrValidateCacheEntryJobResult>> job(
            VisorDrValidateCacheTaskArg arg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid,
            VisorTaskArgument<VisorDrValidateCacheTaskArg> arg) {
        VisorDrValidateCacheTaskArg argument = arg.getArgument();

        Set<String> caches = argument.getCaches();

        if (caches == null || caches.isEmpty())
            caches = ignite.context().cache().cacheDescriptors().entrySet()
                    .stream().filter(e -> e.getValue().cacheType() == CacheType.USER)
                            .map(Entry::getKey).collect(Collectors.toSet());

        caches.forEach(ignite::cache);

        Set<Integer> groups = new HashSet<>();

        List<GridCacheContext> contexts = caches.stream()
                .map(name -> ignite.context().cache().cache(name).context())
                .filter(cctx -> groups.add(cctx.groupId()))
                .collect(Collectors.toList());

        Map<ClusterNode, Map<String, Set<Integer>>> nodeCacheMap = new HashMap<>();

        for (GridCacheContext cctx : contexts) {
            int parts = cctx.affinity().partitions();

            AffinityAssignment assignment = cctx.affinity()
                    .assignment(cctx.affinity().affinityTopologyVersion());

            for (int p = 0; p < parts; p++) {
                Collection<ClusterNode> nodes = ignite.cluster()
                        .forNodes(assignment.assignment().get(p)).nodes();

                for (ClusterNode node : nodes) {
                    String cache = cctx.group().cacheOrGroupName();

                    nodeCacheMap
                            .computeIfAbsent(node, n -> new HashMap<>())
                            .computeIfAbsent(cache, c -> new HashSet<>()).add(p);
                }
            }
        }

        Map<ComputeJob, ClusterNode> map = new HashMap<>();

        for (ClusterNode clusterNode : nodeCacheMap.keySet()) {
            Map<String, Set<Integer>> cachePartsMap = nodeCacheMap.get(clusterNode);
            map.put(new DrCacheEntryValidateJob(argument, cachePartsMap, debug), clusterNode);
        }

        try {
            if (map.isEmpty())
                ignite.log().warning(NO_SUITABLE_NODE_MESSAGE + ": [task=" + getClass().getName() +
                        ", topVer=" + ignite.cluster().topologyVersion() +
                        ", subGrid=" + U.toShortString(subgrid) + "]");

            return map;
        }
        finally {
            if (debug)
                logMapped(ignite.log(), getClass(), map.values());
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorDrRepairPartitionCountersTaskResult reduce0(List<ComputeJobResult> results)
            throws IgniteException {
        Map<GridCacheQueryDetailMetricsKey, GridCacheQueryDetailMetricsAdapter> taskRes = new HashMap<>();

        Map<UUID, Collection<VisorDrValidateCacheEntryJobResult>> nodeMetricsMap = new HashMap<>();
        Map<UUID, Exception> exceptions = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                exceptions.put(res.getNode().id(), res.getException());
            } else {
                Collection<VisorDrValidateCacheEntryJobResult> metrics = res.getData();

                nodeMetricsMap.put(res.getNode().id(), metrics);
            }
        }

       return new VisorDrRepairPartitionCountersTaskResult(nodeMetricsMap, exceptions);
    }

    /**
     * Job that will actually validate entries in dr caches.
     */
    private static class DrCacheEntryValidateJob
            extends VisorJob<VisorDrValidateCacheTaskArg, Collection<VisorDrValidateCacheEntryJobResult>> {
        /** Serial number */
        private static final long serialVersionUID = 0L;

        /** */
        private final Map<String, Set<Integer>> cachesWithPartitions;

        /**
         * Create job with specified argument.
         *
         * @param arg Last time when metrics were collected.
         * @param debug Debug flag.
         */
        protected DrCacheEntryValidateJob(@Nullable VisorDrValidateCacheTaskArg arg, Map<String, Set<Integer>> cachesWithPartitions, boolean debug) {
            super(arg, debug);

            this.cachesWithPartitions = cachesWithPartitions;
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorDrValidateCacheEntryJobResult> run(
                @Nullable VisorDrValidateCacheTaskArg arg
        ) throws IgniteException {
            CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(CU.cacheId(cache));
            GridDhtLocalPartition locPart = reservePartition(arg);

            try {
                try {
                    Set<Long> counters = new HashSet<>();
                    ArrayList<GridCacheEntryInfo> batch = new ArrayList<>(100);

                    GridIterator<CacheDataRow> iter = grpCtx.offheap()
                            .partitionIterator(part, IgniteCacheOffheapManager.DATA_AND_TOMBSTONES);

                    while (iter.hasNext()) {
                        fillBatch(grpCtx.shared(), iter, batch, counters);

                        repairEntries(grpCtx, batch);

                        if (System.currentTimeMillis() - 30_000L > updTime) {
                            updTime = System.currentTimeMillis();
                            log.info("Partition repair in progress: " + getDetails());
                        }

                        batch.clear();
                    }
                }
                finally {
                    locPart.release();

                    metrics.duration = System.currentTimeMillis() - startTime;
                }

                log.info("Partition repair finished: " + getDetails());
            }
            catch (IgniteCheckedException e) {
                log.error("Partition repair failed: " + getDetails(), e);

                throw new IgniteException(e);
            }


        }

        private void fillBatch(GridCacheSharedContext ctx, GridIterator<CacheDataRow> iter,
                ArrayList<GridCacheEntryInfo> batch, Set<Long> counters) {
            while (iter.hasNext() && batch.size() < 100) {
                ctx.database().checkpointReadLock();
                try {
                    int cnt = 100;
                    while (iter.hasNext() && (cnt--) > 0) {
                        CacheDataRow row = iter.next();

                        metrics.entriesProcessed++;

                        if (!counters.add(row.version().updateCounter())) {
                            metrics.brokenEntriesFound++;

                            batch.add(extractEntryInfo(row));
                        }
                    }
                }
                finally {
                    ctx.database().checkpointReadUnlock();
                }
            }
        }

        private GridCacheEntryInfo extractEntryInfo(CacheDataRow row) {
            GridCacheEntryInfo info = new GridCacheEntryInfo();

            info.key(row.key());
            info.cacheId(row.cacheId());
            info.setDeleted(row.value() == TombstoneCacheObject.INSTANCE);

            info.value(row.value());
            info.version(row.version());
            info.expireTime(row.expireTime());

            return info;
        }


        void repairEntries(
                CacheGroupContext grp,
                List<GridCacheEntryInfo> infos
        ) throws IgniteCheckedException {
            if (infos.isEmpty())
                return;

            grp.shared().database().checkpointReadLock();
            try {
                GridCacheAdapter<Object, Object> cache = grp.sharedGroup() ? null : grp.shared().cache().context().cacheContext(grp.groupId()).cache();

                // Loop through all received entries and try to preload them.
                for (GridCacheEntryInfo info : infos) {
                    if (cache == null || grp.sharedGroup() && cache.context().cacheId() != info.cacheId()) {
                        GridCacheContext cctx = grp.shared().cacheContext(info.cacheId());

                        if (cctx == null) {
                            log.warning("Failed to fix entry (cache has gone?): cacheId=" + info.cacheId() + ", part=" +
                                    part + ", key=" + info.key());

                            continue;
                        }

                        cache = cctx.cache();
                    }

                    GridDhtCacheEntry entry = (GridDhtCacheEntry)cache.entryEx(info.key());

                    if (entry == null) {
                        log.warning("Failed to fix entry (concurrently removed?): cacheName=" + cache.name() + ", part=" +
                                part + ", key=" + info.key());

                        continue;
                    }

                    if (info.isDeleted()) {
                        if (entry.clearInternal(info.version()))
                            metrics.tombstoneCleared++;
                        else
                            log.warning("Failed to cleanup tombstone (concurrently removed?): cacheName=" + cache.name() +
                                    ", part=" + part + ", key=" + info.key());

                        continue;
                    }

                    Object key = entry.key();
                    Object val = keepBinaryIfNeeded(cache).get(key);

                    if (val == null) {
                        log.warning("Failed to fix entry (concurrently removed?): cacheName=" + cache.name() + ", part=" +
                                part + ", key=" + info.key());

                        continue;
                    }
                    else
                        keepBinaryIfNeeded(cache).replace(key, val, val);

                    metrics.entriesFixed++;
                }
            }
            finally {
                grp.shared().database().checkpointReadUnlock();
            }
        }

        @NotNull
        private IgniteInternalCache<Object, Object> keepBinaryIfNeeded(GridCacheAdapter<Object, Object> cache) {
            return (keepBinary) ? cache.keepBinary() : cache;
        }

        private GridDhtLocalPartition reservePartition(int part, @Nullable CacheGroupContext grpCtx, String cache) {
            if (grpCtx == null)
                throw new IllegalArgumentException("Cache group not found: " + cache);

            GridDhtLocalPartition locPart = grpCtx.topology().localPartition(part);

            if (locPart == null || !locPart.reserve())
                throw new IllegalArgumentException("Failed to reserve partition for group: groupName=" + cache +
                        ", part=" + part);

            if (locPart.state() != OWNING || grpCtx.topology().stopping()) {
                locPart.release();

                throw new IllegalArgumentException("Failed to reserve non-owned partition for group: groupName=" +
                        cache + ", part=" + part);
            }

            return locPart;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DrCacheEntryValidateJob.class, this);
        }
    }
}
