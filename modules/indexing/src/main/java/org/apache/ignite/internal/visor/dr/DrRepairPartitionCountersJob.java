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
 * Job that will actually validate entries in dr caches.
 */
class DrRepairPartitionCountersJob extends VisorDrPartitionCountersJob<VisorDrRepairPartitionCountersTaskArg, Collection<VisorDrRepairPartitionCountersJobResult>> {
    /**
     * Serial number
     */
    private static final long serialVersionUID = 0L;

    private final Map<String, Set<Integer>> cachePartsMap;

    private final boolean keepBinary;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Create job with specified argument.
     *  @param arg   Last time when metrics were collected.
     * @param cachePartsMap
     * @param debug
     * @param keepBinary
     */
    public DrRepairPartitionCountersJob(@NotNull VisorDrRepairPartitionCountersTaskArg arg,
            Map<String, Set<Integer>> cachePartsMap, boolean debug, boolean keepBinary) {
        super(arg, debug);

        this.cachePartsMap = cachePartsMap;
        this.keepBinary = keepBinary;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<VisorDrRepairPartitionCountersJobResult> run(
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

        int size = 0;
        int entriesProcessed = 0;
        int brokenEntriesFound = 0;
        int entriesFixed = 0;
        int tombstoneCleared = 0;

        for (Integer part : parts) {
            executeForPartition(cache, part);
        }

        return new VisorDrRepairPartitionCountersJobResult(cache, size, ,, entriesProcessed, brokenEntriesFound, entriesFixed, tombstoneCleared);
    }

    private void executeForPartition(String cache, Integer part) {
        CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(CU.cacheId(cache));

        GridDhtLocalPartition locPart = reservePartition(part, grpCtx, cache);

        int brokenEntriesFound = 0;
        int
        try {
            try {
                Set<Long> counters = new HashSet<>();
                ArrayList<GridCacheEntryInfo> batch = new ArrayList<>(100);

                GridIterator<CacheDataRow> iter = grpCtx.offheap()
                        .partitionIterator(part, IgniteCacheOffheapManager.DATA_AND_TOMBSTONES);

                while (iter.hasNext()) {
                    brokenEntriesFound += fillBatch(grpCtx.shared(), iter, batch, counters);

                    repairEntries(grpCtx, batch, part);

                    batch.clear();
                }
            } finally {
                locPart.release();
            }

            log.info("Partition repair finished: " + getDetails());
        } catch (IgniteCheckedException e) {
            log.error("Partition repair failed: " + getDetails(), e);

            throw new IgniteException(e);
        }

    }

    private int fillBatch(GridCacheSharedContext ctx, GridIterator<CacheDataRow> iter,
            ArrayList<GridCacheEntryInfo> batch, Set<Long> counters) {
        int brokenEntriesFound = 0;

        while (iter.hasNext() && batch.size() < 100) {
            ctx.database().checkpointReadLock();
            try {
                int cnt = 100;
                while (iter.hasNext() && (cnt--) > 0) {
                    CacheDataRow row = iter.next();

                    if (!counters.add(row.version().updateCounter())) {
                        brokenEntriesFound++;

                        batch.add(extractEntryInfo(row));
                    }
                }
            } finally {
                ctx.database().checkpointReadUnlock();
            }
        }

        return brokenEntriesFound;
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


    private void repairEntries(CacheGroupContext grp, List<GridCacheEntryInfo> infos, int part)
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
                        metrics.tombstoneCleared++;
                    } else {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return S.toString(DrRepairPartitionCountersJob.class, this);
    }
}
