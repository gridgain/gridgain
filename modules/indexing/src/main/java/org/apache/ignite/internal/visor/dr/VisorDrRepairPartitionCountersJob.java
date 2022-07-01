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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
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
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Repair partition counters job.
 */
class VisorDrRepairPartitionCountersJob extends VisorDrPartitionCountersJob<VisorDrRepairPartitionCountersTaskArg, Collection<VisorDrRepairPartitionCountersJobResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Map with cache-partitions pairs, assigned for node. */
    private final Map<String, Set<Integer>> cachePartsMap;

    /** Batch size. */
    private final int batchSize;

    /** Keep binary flag. */
    private final boolean keepBinary;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Create job with specified argument.
     *
     * @param arg Task arguments.
     * @param cachePartsMap Map with cache-partitions pairs.
     * @param debug Debug flag.
     */
    public VisorDrRepairPartitionCountersJob(@NotNull VisorDrRepairPartitionCountersTaskArg arg,
            Map<String, Set<Integer>> cachePartsMap, boolean debug) {
        super(arg, debug);

        this.cachePartsMap = cachePartsMap;
        this.keepBinary = arg.isKeepBinary();
        this.batchSize = arg.getBatchSize();
    }

    /** {@inheritDoc} */
    @Override protected Collection<VisorDrRepairPartitionCountersJobResult> run(
            @Nullable VisorDrRepairPartitionCountersTaskArg arg
    ) throws IgniteException {
        assert arg != null;

        List<VisorDrRepairPartitionCountersJobResult> results = new ArrayList<>();

        for (String cache : cachePartsMap.keySet()) {

            results.add(executeForCache(cache));
        }

        return results;
    }

    private VisorDrRepairPartitionCountersJobResult executeForCache(String cache) {
        Set<Integer> parts = cachePartsMap.get(cache);

        Set<Integer> affectedPartitions = new HashSet<>();
        Set<Integer> affectedCaches = new HashSet<>();
        long entriesProcessed = 0;
        long brokenEntriesFound = 0;
        long tombstonesCleared = 0;
        long tombstonesFailedToClear = 0;
        long entriesFixed = 0;
        long entriesFailedToFix = 0;
        long size = 0;

        for (Integer part : parts) {
            PartitionRepairMetrics metrics = executeForPartition(cache, part);

            if (metrics.brokenEntriesFound > 0) {
                affectedPartitions.add(part);
            }
            affectedCaches.addAll(metrics.affectedCaches);
            entriesProcessed += metrics.entriesProcessed;
            brokenEntriesFound += metrics.brokenEntriesFound;
            entriesFixed += metrics.entriesFixed;
            entriesFailedToFix += metrics.entriesFailedToFix;
            tombstonesCleared += metrics.tombstonesCleared;
            tombstonesFailedToClear += metrics.tombstonesFailedToClear;
            size += metrics.size;
        }

        return new VisorDrRepairPartitionCountersJobResult(cache, size, affectedCaches,
                affectedPartitions, entriesProcessed, brokenEntriesFound, tombstonesCleared,
                tombstonesFailedToClear, entriesFixed, entriesFailedToFix);
    }

    private PartitionRepairMetrics executeForPartition(String cache, Integer part) {
        CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(CU.cacheId(cache));

        GridDhtLocalPartition locPart = reservePartition(part, grpCtx, cache);

        PartitionRepairMetrics metrics = new PartitionRepairMetrics();

        metrics.size = locPart.fullSize();

        try {
            try {
                Set<Long> counters = new HashSet<>();
                ArrayList<GridCacheEntryInfo> batch = new ArrayList<>(100);

                GridIterator<CacheDataRow> iter = grpCtx.offheap()
                        .partitionIterator(part, IgniteCacheOffheapManager.DATA_AND_TOMBSTONES);

                while (iter.hasNext()) {
                    fillBatch(metrics, grpCtx.shared(), iter, batch, counters);

                    repairEntries(metrics, grpCtx, batch, part);

                    batch.clear();
                }
            } finally {
                locPart.release();
            }
        } catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return metrics;
    }

    private void fillBatch(PartitionRepairMetrics metrics, GridCacheSharedContext ctx, GridIterator<CacheDataRow> iter,
            ArrayList<GridCacheEntryInfo> batch, Set<Long> counters) {

        while (iter.hasNext() && batch.size() < batchSize) {
            ctx.database().checkpointReadLock();
            try {
                int cnt = batchSize;
                while (iter.hasNext() && (cnt--) > 0) {
                    CacheDataRow row = iter.next();

                    metrics.entriesProcessed++;

                    if (!counters.add(row.version().updateCounter())) {
                        metrics.affectedCaches.add(row.cacheId());
                        metrics.brokenEntriesFound++;

                        batch.add(extractEntryInfo(row));
                    }
                }
            } finally {
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

    private void repairEntries(PartitionRepairMetrics metrics,
            CacheGroupContext grp, List<GridCacheEntryInfo> infos, int part)
            throws IgniteCheckedException {
        if (infos.isEmpty()) {
            return;
        }
        grp.shared().database().checkpointReadLock();
        try {
            GridCacheAdapter<Object, Object> cache = grp.sharedGroup() ? null
                    : grp.shared().cache().context().cacheContext(grp.groupId()).cache();

            // Loop through all received entries and try to preload them.
            for (GridCacheEntryInfo info : infos) {
                if (cache == null
                        || grp.sharedGroup() && cache.context().cacheId() != info.cacheId()) {
                    GridCacheContext cctx = grp.shared().cacheContext(info.cacheId());

                    if (cctx == null) {
                        metrics.entriesFailedToFix++;

                        log.warning(
                                "Failed to fix entry (cache has gone?): cacheId=" + info.cacheId()
                                        + ", part=" +
                                        part + ", key=" + info.key());

                        continue;
                    }

                    cache = cctx.cache();
                }

                GridDhtCacheEntry entry = (GridDhtCacheEntry) cache.entryEx(info.key());

                if (entry == null) {
                    log.warning(
                            "Failed to fix entry (concurrently removed?): cacheName=" + cache.name()
                                    + ", part=" +
                                    part + ", key=" + info.key());

                    continue;
                }

                if (info.isDeleted()) {
                    if (entry.clearInternal(info.version())) {
                        metrics.tombstonesCleared++;
                    } else {
                        metrics.tombstonesFailedToClear++;
                        log.warning(
                                "Failed to cleanup tombstone (concurrently removed?): cacheName="
                                        + cache.name() +
                                        ", part=" + part + ", key=" + info.key());
                    }

                    continue;
                }

                Object key = entry.key();
                Object val = keepBinaryIfNeeded(cache).get(key);

                if (val == null) {
                    metrics.entriesFailedToFix++;
                    log.warning(
                            "Failed to fix entry (concurrently removed?): cacheName=" + cache.name()
                                    + ", part=" +
                                    part + ", key=" + info.key());

                    continue;
                } else {
                    keepBinaryIfNeeded(cache).replace(key, val, val);
                }

                metrics.entriesFixed++;
            }
        } finally {
            grp.shared().database().checkpointReadUnlock();
        }
    }

    @NotNull private IgniteInternalCache<Object, Object> keepBinaryIfNeeded(
            GridCacheAdapter<Object, Object> cache) {
        return (keepBinary) ? cache.keepBinary() : cache;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrRepairPartitionCountersJob.class, this);
    }

    /**
     * Repair job metrics for a single partition.
     */
    private static class PartitionRepairMetrics {
        /** */
        public long brokenEntriesFound;

        /** */
        public long tombstonesCleared;

        /** */
        public long tombstonesFailedToClear;

        /** */
        public long entriesFixed;

        /** */
        public long entriesFailedToFix;

        /** */
        public long entriesProcessed;

        /** */
        public long size;

        /** */
        public Set<Integer> affectedCaches = new HashSet<>();
    }
}
