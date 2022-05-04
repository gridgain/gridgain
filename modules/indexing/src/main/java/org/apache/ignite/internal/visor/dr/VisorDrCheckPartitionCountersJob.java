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
import java.util.Map.Entry;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Check partition counters job.
 */
public class VisorDrCheckPartitionCountersJob extends
        VisorDrPartitionCountersJob<VisorDrCheckPartitionCountersTaskArg, Collection<VisorDrCheckPartitionCountersJobResult>> {
    /** Serial number */
    private static final long serialVersionUID = 0L;

    /** Caches with partitions. */
    protected final Map<String, Set<Integer>> cachesWithPartitions;

    /**
     * Create job with specified argument.
     *
     * @param arg Task arguments.
     * @param cachesWithPartitions Map with cache-partitions pairs.
     * @param debug Debug flag.
     */
    public VisorDrCheckPartitionCountersJob(
            @Nullable VisorDrCheckPartitionCountersTaskArg arg, boolean debug,
            Map<String, Set<Integer>> cachesWithPartitions) {
        super(arg, debug);
        this.cachesWithPartitions = cachesWithPartitions;
    }

    /** {@inheritDoc} */
    @Override protected Collection<VisorDrCheckPartitionCountersJobResult> run(
            @Nullable VisorDrCheckPartitionCountersTaskArg arg
    ) throws IgniteException {
        assert arg != null;

        int checkFirst = arg.getCheckFirst();
        boolean scanUntilFirstError = arg.isScanUntilFirstError();

        List<VisorDrCheckPartitionCountersJobResult> metrics = new ArrayList<>();

        for (Entry<String, Set<Integer>> cachesWithParts : cachesWithPartitions.entrySet()) {
            metrics.add(
                    calculateForCache(cachesWithParts.getKey(), cachesWithParts.getValue(),
                            checkFirst, scanUntilFirstError)
            );
        }

        return metrics;
    }

    private VisorDrCheckPartitionCountersJobResult calculateForCache(String cache,
            Set<Integer> parts, int checkFirst, boolean scanUntilFirstError) {
        ignite.cache(cache);

        CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(CU.cacheId(cache));

        if (grpCtx == null) {
            grpCtx = ignite.cachex(cache).context().group();
        }

        int size = 0;
        int entriesProcessed = 0;
        int brokenEntriesFound = 0;
        Set<Integer> affectedCaches = new HashSet<>();
        Set<Integer> affectedPartitions = new HashSet<>();

        for (Integer part : parts) {
            VisorDrCachePartitionMetrics partMetrics = calculateForPartition(grpCtx, cache, part,
                    checkFirst, scanUntilFirstError);

            size += partMetrics.getSize();
            entriesProcessed += partMetrics.getEntriesProcessed();

            if (partMetrics.getBrokenEntriesFound() > 0) {
                affectedCaches.addAll(partMetrics.getAffectedCaches());
                affectedPartitions.add(part);
                brokenEntriesFound += partMetrics.getBrokenEntriesFound();

                if (scanUntilFirstError)
                    break;
            }
        }

        return new VisorDrCheckPartitionCountersJobResult(cache, size, affectedCaches,
                affectedPartitions, entriesProcessed, brokenEntriesFound);
    }

    private VisorDrCachePartitionMetrics calculateForPartition(CacheGroupContext grpCtx,
            String cache, int part, int checkFirst, boolean scanUntilFirstError) {
        GridDhtLocalPartition locPart = reservePartition(part, grpCtx, cache);

        int entriesProcessed = 0;
        int brokenEntriesFound = 0;

        boolean checkFullCache = checkFirst == -1;

        Set<Integer> affectedCaches = new HashSet<>();

        try {
            GridIterator<CacheDataRow> it = grpCtx.offheap()
                    .partitionIterator(part, IgniteCacheOffheapManager.DATA_AND_TOMBSTONES);

            Set<Long> usedCounters = new HashSet<>();

            grpCtx.shared().database().checkpointReadLock();
            try {
                while (it.hasNext()) {
                    if (!checkFullCache && (checkFirst--) < 0)
                        break;
                    CacheDataRow next = it.next();

                    entriesProcessed++;

                    if (!usedCounters.add(next.version().updateCounter())) {
                        brokenEntriesFound++;
                        affectedCaches.add(next.cacheId());

                        if (scanUntilFirstError)
                            break;
                    }
                }
            } finally {
                grpCtx.shared().database().checkpointReadUnlock();
            }
        } catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        } finally {
            locPart.release();
        }

        return new VisorDrCachePartitionMetrics(locPart.fullSize(), affectedCaches, entriesProcessed,
                brokenEntriesFound);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrCheckPartitionCountersJob.class, this);
    }
}
