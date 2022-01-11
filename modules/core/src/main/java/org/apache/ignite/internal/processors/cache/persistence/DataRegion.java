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
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.evict.PageEvictionTracker;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.DataRegionConfiguration.DFLT_EMPTY_PAGES_POOL_SIZE;

/**
 * Data region provides access to objects configured with {@link DataRegionConfiguration} configuration.
 */
public class DataRegion {
    /** */
    @Nullable private final PageMemory pageMem;

    /** */
    private final DataRegionMetricsImpl metrics;

    /** */
    @Nullable private final DataRegionConfiguration cfg;

    /** */
    @Nullable private final PageEvictionTracker evictionTracker;

    /** Current size of empty pages pool. */
    private volatile int emptyPagesPoolSize;

    /**
     * @param pageMem PageMemory instance.
     * @param metrics DataRegionMetrics instance.
     * @param cfg Configuration of given DataRegion.
     * @param evictionTracker Eviction tracker.
     */
    public DataRegion(
        @Nullable PageMemory pageMem,
        @Nullable DataRegionConfiguration cfg,
        @Nullable DataRegionMetricsImpl metrics,
        @Nullable PageEvictionTracker evictionTracker
    ) {
        this.pageMem = pageMem;
        this.metrics = metrics;
        this.cfg = cfg;
        this.evictionTracker = evictionTracker;
        this.emptyPagesPoolSize = cfg == null ? DFLT_EMPTY_PAGES_POOL_SIZE : cfg.getEmptyPagesPoolSize();
    }

    /**
     *
     */
    public PageMemory pageMemory() {
        return pageMem;
    }

    /**
     * @return Config.
     */
    public DataRegionConfiguration config() {
        return cfg;
    }

    /**
     * @return Memory Metrics.
     */
    public DataRegionMetricsImpl metrics() {
        return metrics;
    }

    /**
     *
     */
    public PageEvictionTracker evictionTracker() {
        return evictionTracker;
    }

    /**
     * Increase the pool of empty pages, if it is too small.
     *
     * @return {@code true} if increased.
     */
    public synchronized boolean increaseEmptyPagesPool() {
        // Can't make empty pages pool bigger than 10% of total pages count.
        int rightBorder = (int) Long.min(cfg.getMaxSize() / pageMem.systemPageSize() / 10, Integer.MAX_VALUE);

        if (cfg != null && pageMem != null && emptyPagesPoolSize < rightBorder) {
            emptyPagesPoolSize = Integer.min(emptyPagesPoolSize * 2, rightBorder);

            return true;
        }
        else
            return false;
    }

    /** Current size of empty pages pool. */
    public int emptyPagesPoolSize() {
        return emptyPagesPoolSize;
    }
}
