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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
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
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.logMapped;

/**
 * Task for check partition counter in DR entries.
 */
@GridInternal
public class VisorDrCheckPartitionCountersTask extends VisorMultiNodeTask<VisorDrCheckPartitionCountersTaskArg,
        VisorDrCheckPartitionCountersTaskResult, Collection<VisorDrCheckPartitionCountersJobResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorDrCheckPartitionCountersTaskArg, Collection<VisorDrCheckPartitionCountersJobResult>> job(
            VisorDrCheckPartitionCountersTaskArg arg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid,
            VisorTaskArgument<VisorDrCheckPartitionCountersTaskArg> arg) {
        VisorDrCheckPartitionCountersTaskArg argument = arg.getArgument();

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
    @Nullable @Override protected VisorDrCheckPartitionCountersTaskResult reduce0(List<ComputeJobResult> results)
            throws IgniteException {
        Map<GridCacheQueryDetailMetricsKey, GridCacheQueryDetailMetricsAdapter> taskRes = new HashMap<>();

        Map<UUID, Collection<VisorDrCheckPartitionCountersJobResult>> nodeMetricsMap = new HashMap<>();
        Map<UUID, Exception> exceptions = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                exceptions.put(res.getNode().id(), res.getException());
            } else {
                Collection<VisorDrCheckPartitionCountersJobResult> metrics = res.getData();

                nodeMetricsMap.put(res.getNode().id(), metrics);
            }
        }

       return new VisorDrCheckPartitionCountersTaskResult(nodeMetricsMap, exceptions);
    }

    /**
     * Job that will actually validate entries in dr caches.
     */
    private static class DrCacheEntryValidateJob
            extends VisorJob<VisorDrCheckPartitionCountersTaskArg, Collection<VisorDrCheckPartitionCountersJobResult>> {
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
        protected DrCacheEntryValidateJob(@Nullable VisorDrCheckPartitionCountersTaskArg arg, Map<String, Set<Integer>> cachesWithPartitions, boolean debug) {
            super(arg, debug);

            this.cachesWithPartitions = cachesWithPartitions;
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorDrCheckPartitionCountersJobResult> run(
                @Nullable VisorDrCheckPartitionCountersTaskArg arg
        ) throws IgniteException {
            assert arg != null;

            int checkFirst = arg.getCheckFirst();

            List<VisorDrCheckPartitionCountersJobResult> metrics = new ArrayList<>();

            for (Entry<String, Set<Integer>> cachesWithParts : cachesWithPartitions.entrySet()) {
                metrics.add(
                        calculateForCache(cachesWithParts.getKey(), cachesWithParts.getValue(), checkFirst)
                );
            }

            return metrics;
        }

        private VisorDrCheckPartitionCountersJobResult calculateForCache(String cache, Set<Integer> parts, int checkFirst) {
            ignite.cache(cache);

            CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(CU.cacheId(cache));

            if (grpCtx == null)
                grpCtx = ignite.cachex(cache).context().group();

            int size = 0;
            int entriesProcessed = 0;
            int brokenEntriesFound = 0;
            Set<Integer> affectedCaches = new HashSet<>();
            Set<Integer> affectedPartitions = new HashSet<>();

            for (Integer part : parts) {
                DrCachePartitionMetrics partMetrics = calculateForPartition(grpCtx, cache, part, checkFirst);

                size += partMetrics.getSize();
                entriesProcessed += partMetrics.getEntriesProcessed();

                if (partMetrics.getBrokenEntriesFound() > 0) {
                    affectedCaches.addAll(partMetrics.getAffectedCaches());
                    affectedPartitions.add(part);
                    brokenEntriesFound += partMetrics.getBrokenEntriesFound();
                }
            }

            return new VisorDrCheckPartitionCountersJobResult(cache, size, affectedCaches, affectedPartitions, entriesProcessed, brokenEntriesFound);
        }

        private DrCachePartitionMetrics calculateForPartition(CacheGroupContext grpCtx, String cache, int part, int checkFirst) {
            GridDhtLocalPartition locPart = reservePartition(part, grpCtx, cache);

            int entriesProcessed = 0;
            int brokenEntriesFound = 0;
            Set<Integer> affectedCaches = new HashSet<>();

            try {
                GridIterator<CacheDataRow> it = grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA_AND_TOMBSTONES);

                Set<Long> usedCounters = new HashSet<>();

                while (it.hasNext()) {
                    grpCtx.shared().database().checkpointReadLock();
                    try {
                        while (it.hasNext() && (checkFirst--) > 0) {
                            CacheDataRow next = it.next();

                            entriesProcessed++;

                            if (!usedCounters.add(next.version().updateCounter())) {
                                brokenEntriesFound++;
                                affectedCaches.add(next.cacheId());
                            }
                        }
                    }
                    finally {
                        grpCtx.shared().database().checkpointReadUnlock();
                    }
                }

            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                locPart.release();
            }

            return new DrCachePartitionMetrics(locPart.fullSize(), affectedCaches, entriesProcessed, brokenEntriesFound);
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
