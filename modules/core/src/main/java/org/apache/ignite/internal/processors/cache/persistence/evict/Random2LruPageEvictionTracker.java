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
package org.apache.ignite.internal.processors.cache.persistence.evict;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class Random2LruPageEvictionTracker extends PageAbstractEvictionTracker {
    /** Evict attempts limit. */
    private static final int EVICT_ATTEMPTS_LIMIT = 15;

    /** LRU Sample size. */
    private static final int SAMPLE_SIZE = 5;

    /** Maximum sample search spin count */
    private static final int SAMPLE_SPIN_LIMIT = SAMPLE_SIZE * 1000;

    /** Number of bytes per page in the tracking array. */
    private static final long TRACKING_BYTES_PER_PAGE = 8L;

    /** Logger. */
    private final IgniteLogger log;

    /** Tracking array ptr. */
    private long trackingArrPtr;

    /**
     * @param pageMem Page memory.
     * @param plcCfg Policy config.
     * @param sharedCtx Shared context.
     */
    public Random2LruPageEvictionTracker(
        PageMemoryNoStoreImpl pageMem,
        DataRegionConfiguration plcCfg,
        GridCacheSharedContext<?, ?> sharedCtx
    ) {
        super(pageMem, plcCfg, sharedCtx);

        DataStorageConfiguration memCfg = sharedCtx.kernalContext().config().getDataStorageConfiguration();

        assert plcCfg.getMaxSize() / memCfg.getPageSize() < Integer.MAX_VALUE;

        log = sharedCtx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        trackingArrPtr = GridUnsafe.allocateMemory(trackingArraySize(trackingSize));

        GridUnsafe.zeroMemory(trackingArrPtr, trackingArraySize(trackingSize));
    }

    /**
     * Returns size of the tracking array for the given page count.
     */
    static long trackingArraySize(int pageCount) {
        return trackingArrayOffset(pageCount);
    }

    /**
     * Returns offset in of the tracking array for the given page index.
     */
    static long trackingArrayOffset(int pageIndex) {
        return pageIndex * TRACKING_BYTES_PER_PAGE;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        GridUnsafe.freeMemory(trackingArrPtr);
    }

    /** {@inheritDoc} */
    @Override public void touchPage(long pageId) throws IgniteCheckedException {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        long latestTs = compactTimestamp(U.currentTimeMillis());

        assert latestTs >= 0 && latestTs < Integer.MAX_VALUE;

        boolean success;

        do {
            int trackingIdx = trackingIdx(pageIdx);

            int firstTs = GridUnsafe.getIntVolatile(null, trackingArrPtr + trackingArrayOffset(trackingIdx));

            int secondTs = GridUnsafe.getIntVolatile(null, trackingArrPtr + trackingArrayOffset(trackingIdx) + 4);

            if (firstTs <= secondTs)
                success = GridUnsafe.compareAndSwapInt(null,
                    trackingArrPtr + trackingArrayOffset(trackingIdx), firstTs, (int)latestTs);
            else {
                success = GridUnsafe.compareAndSwapInt(
                    null, trackingArrPtr + trackingArrayOffset(trackingIdx) + 4, secondTs, (int)latestTs);
            }
        } while (!success);
    }

    /** {@inheritDoc} */
    @Override public void evictDataPage() throws IgniteCheckedException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        int evictAttemptsCnt = 0;

        while (evictAttemptsCnt < EVICT_ATTEMPTS_LIMIT) {
            int lruTrackingIdx = -1;

            int lruCompactTs = Integer.MAX_VALUE;

            int dataPagesCnt = 0;

            int sampleSpinCnt = 0;

            while (dataPagesCnt < SAMPLE_SIZE) {
                int trackingIdx = rnd.nextInt(trackingSize);

                int firstTs = GridUnsafe.getIntVolatile(null, trackingArrPtr + trackingArrayOffset(trackingIdx));

                int secondTs = GridUnsafe.getIntVolatile(null, trackingArrPtr + trackingArrayOffset(trackingIdx) + 4);

                int minTs = Math.min(firstTs, secondTs);

                int maxTs = Math.max(firstTs, secondTs);

                if (maxTs != 0) {
                    // We chose data page with at least one touch.
                    if (minTs < lruCompactTs) {
                        lruTrackingIdx = trackingIdx;

                        lruCompactTs = minTs;
                    }

                    dataPagesCnt++;
                }

                sampleSpinCnt++;

                if (sampleSpinCnt > SAMPLE_SPIN_LIMIT) {
                    LT.warn(log, "Too many attempts to choose data page: " + SAMPLE_SPIN_LIMIT);

                    lruTrackingIdx = -1;
                    break;
                }
            }

            if (lruTrackingIdx < 0)
                break;

            if (evictDataPage(pageIdx(lruTrackingIdx)))
                return;

            evictAttemptsCnt++;
        }

        for (evictAttemptsCnt = 0; evictAttemptsCnt < EVICT_ATTEMPTS_LIMIT; evictAttemptsCnt++) {
            if (evictRandomRow(rnd))
                return;
        }

        LT.warn(log, "Too many failed attempts to evict page: " + EVICT_ATTEMPTS_LIMIT * 2);
    }

    private boolean evictRandomRow(Random rnd) throws IgniteCheckedException {
        List<CacheDataRowAdapter> randomRows = findRandomRows(rnd, SAMPLE_SIZE);

        if (randomRows.isEmpty())
            return false;

        int lruCompactTs = Integer.MAX_VALUE;

        CacheDataRowAdapter minRow = randomRows.get(0);

        for (CacheDataRowAdapter randomRow : randomRows) {
            int trackingIdx = trackingIdx(PageIdUtils.pageIndex(randomRow.link()));

            int firstTs = GridUnsafe.getIntVolatile(null, trackingArrPtr + trackingArrayOffset(trackingIdx));

            int secondTs = GridUnsafe.getIntVolatile(null, trackingArrPtr + trackingArrayOffset(trackingIdx) + 4);

            int minTs = Math.min(firstTs, secondTs);

            int maxTs = Math.max(firstTs, secondTs);

            if (maxTs != 0) {
                // We chose data page with at least one touch.
                if (minTs < lruCompactTs) {
                    lruCompactTs = minTs;

                    minRow = randomRow;
                }
            }
        }

        return evictDataPage(PageIdUtils.pageIndex(minRow.link()));
    }

    /** {@inheritDoc} */
    @Override protected boolean checkTouch(long pageId) {
        int trackingIdx = trackingIdx(PageIdUtils.pageIndex(pageId));

        int firstTs = GridUnsafe.getIntVolatile(null, trackingArrPtr + trackingArrayOffset(trackingIdx));

        return firstTs != 0;
    }

    /** {@inheritDoc} */
    @Override public void forgetPage(long pageId) {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        int trackingIdx = trackingIdx(pageIdx);

        GridUnsafe.putLongVolatile(null, trackingArrPtr + trackingArrayOffset(trackingIdx), 0L);
    }

}
