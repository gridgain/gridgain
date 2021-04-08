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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.MemoryPageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.MemoryPageMetricsImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderWithDelegateMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.MetricsMxBean;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheGroupMetricsRegistryName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 *
 */
public class DataRegionMetricsImpl implements DataRegionMetrics {
    private static final class LongAdderMetricDelegate implements LongAdderWithDelegateMetric.Delegate {
        private final LongAdderMetric delegate;

        public LongAdderMetricDelegate(LongAdderMetric delegate) {
            this.delegate = delegate;
        }

        @Override public void increment() {
            delegate.increment();
        }

        @Override public void decrement() {
            delegate.decrement();
        }

        @Override public void add(long x) {
            delegate.add(x);
        }
    }

    /**
     * Data region metrics prefix.
     * Full name will contain {@link DataRegionConfiguration#getName()} also.
     * {@code "io.dataregion.default"}, for example.
     */
    public static final String DATAREGION_METRICS_PREFIX = metricName("io", "dataregion");

    /** */
    private final DataRegionMetricsProvider dataRegionMetricsProvider;

    /** */
    private final LongAdderMetric totalAllocatedPages;

    private final ConcurrentMap<Integer, MemoryPageMetrics> cacheGrpMetrics = new ConcurrentHashMap<>();

    private final MemoryPageMetrics dataRegionMemoryPageMetrics;

    /**
     * Counter for number of pages occupied by large entries (one entry is larger than one page).
     */
    private final LongAdderMetric largeEntriesPages;

    /** Counter for number of dirty pages. */
    private final LongAdderMetric dirtyPages;

    /** */
    private final LongAdderMetric readPages;

    /** */
    private final LongAdderMetric writtenPages;

    /** */
    private final LongAdderMetric replacedPages;

    /** */
    private final AtomicLongMetric offHeapSize;

    /** */
    private final AtomicLongMetric checkpointBufSize;

    /** */
    private volatile boolean metricsEnabled;

    /** */
    private boolean persistenceEnabled;

    /** */
    private final int subInts;

    /** Allocation rate calculator. */
    private final HitRateMetric allocRate;

    /** Eviction rate calculator. */
    private final HitRateMetric evictRate;

    /** */
    private final HitRateMetric pageReplaceRate;

    /** */
    private final HitRateMetric pageReplaceAge;

    /** Total throttling threads time in milliseconds. */
    private final LongAdderMetric totalThrottlingTime;

    /** */
    private final DataRegionConfiguration memPlcCfg;

    /** */
    private PageMemory pageMem;

    private final GridKernalContext kernalContext;

    /** Time interval (in milliseconds) when allocations/evictions are counted to calculate rate. */
    private volatile long rateTimeInterval;

    /**
     * @param memPlcCfg DataRegionConfiguration.
     * @param kernalCtx Kernal context.
     * @param dataRegionMetricsProvider Data region metrics provider.
     */
    public DataRegionMetricsImpl(
        DataRegionConfiguration memPlcCfg,
        GridKernalContext kernalCtx,
        DataRegionMetricsProvider dataRegionMetricsProvider
    ) {
        this.memPlcCfg = memPlcCfg;
        this.dataRegionMetricsProvider = dataRegionMetricsProvider;
        this.kernalContext = kernalCtx;

        metricsEnabled = memPlcCfg.isMetricsEnabled();

        persistenceEnabled = memPlcCfg.isPersistenceEnabled();

        rateTimeInterval = memPlcCfg.getMetricsRateTimeInterval();

        subInts = memPlcCfg.getMetricsSubIntervalCount();

        MetricRegistry mreg = kernalCtx.metric().registry(metricName(DATAREGION_METRICS_PREFIX, memPlcCfg.getName()));

        allocRate = mreg.hitRateMetric("AllocationRate",
            "Allocation rate (pages per second) averaged across rateTimeInternal.",
            60_000,
            5);

        totalAllocatedPages = mreg.longAdderMetric(
            "TotalAllocatedPages",
            new LongAdderWithDelegateMetric.Delegate() {
                @Override public void increment() {
                    allocRate.increment();
                }

                @Override public void add(long x) {
                    allocRate.add(x);
                }

                @Override public void decrement() {}
            },
            "Total number of allocated pages."
        );

        evictRate = mreg.hitRateMetric("EvictionRate",
            "Eviction rate (pages per second).",
            60_000,
            5);

        pageReplaceRate = mreg.hitRateMetric("PagesReplaceRate",
            "Rate at which pages in memory are replaced with pages from persistent storage (pages per second).",
            60_000,
            5);

        pageReplaceAge = mreg.hitRateMetric("PagesReplaceAge",
            "Average age at which pages in memory are replaced with pages from persistent storage (milliseconds).",
            60_000,
            5);

        largeEntriesPages = mreg.longAdderMetric("LargeEntriesPagesCount",
            "Count of pages that fully ocupied by large entries that go beyond page size");

        dirtyPages = mreg.longAdderMetric("DirtyPages",
            "Number of pages in memory not yet synchronized with persistent storage.");

        readPages = mreg.longAdderMetric("PagesRead",
            "Number of pages read from last restart.");

        writtenPages = mreg.longAdderMetric("PagesWritten",
            "Number of pages written from last restart.");

        replacedPages = mreg.longAdderMetric("PagesReplaced",
            "Number of pages replaced from last restart.");

        offHeapSize = mreg.longMetric("OffHeapSize",
            "Offheap size in bytes.");

        checkpointBufSize = mreg.longMetric("CheckpointBufferSize",
            "Checkpoint buffer size in bytes.");

        mreg.register("EmptyDataPages",
            dataRegionMetricsProvider::emptyDataPages,
            "Calculates empty data pages count for region. It counts only totally free pages that can be reused " +
                "(e. g. pages that are contained in reuse bucket of free list).");

        totalThrottlingTime = mreg.longAdderMetric("TotalThrottlingTime",
            "Total throttling threads time in milliseconds. The Ignite throttles threads that generate " +
                "dirty pages during the ongoing checkpoint.");

        dataRegionMemoryPageMetrics = MemoryPageMetricsImpl.builder(mreg)
            .dataPagesSizeCallback(new LongAdderMetricDelegate(totalAllocatedPages))
            .build();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return U.maskName(memPlcCfg.getName());
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return totalAllocatedPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedPages() {
        return getTotalAllocatedPages() - dataRegionMetricsProvider.emptyDataPages();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        return getTotalAllocatedPages() * (persistenceEnabled ? pageMem.pageSize() : pageMem.systemPageSize());
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedSize() {
        return getTotalUsedPages() * (persistenceEnabled ? pageMem.pageSize() : pageMem.systemPageSize());
    }

    /** {@inheritDoc} */
    @Override public float getAllocationRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)allocRate.value() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)evictRate.value() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        if (!metricsEnabled)
            return 0;

        return totalAllocatedPages.value() != 0 ? (float)largeEntriesPages.value() / totalAllocatedPages.value() : 0;
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        if (!metricsEnabled)
            return 0;

        long freeSpace = dataRegionMetricsProvider.partiallyFilledPagesFreeSpace();

        long totalAllocated = getPageSize() * totalAllocatedPages.value();

        return totalAllocated != 0 ? (float)(totalAllocated - freeSpace) / totalAllocated : 0f;
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return dirtyPages.value();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceRate() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return ((float)pageReplaceRate.value() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceAge() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        long rep = pageReplaceRate.value();

        return rep == 0 ? 0 : ((float)pageReplaceAge.value() / rep);
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemoryPages() {
        if (!persistenceEnabled)
            return getTotalAllocatedPages();

        if (!metricsEnabled)
            return 0;

        return pageMem.loadedPages();
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemorySize() {
        return getPhysicalMemoryPages() * pageMem.systemPageSize();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferPages() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return pageMem.checkpointBufferPagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferSize() {
        return getUsedCheckpointBufferPages() * pageMem.systemPageSize();
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointBufferSize() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return checkpointBufSize.value();
    }

    /** {@inheritDoc} */
    @Override public int getPageSize() {
        if (!metricsEnabled)
            return 0;

        return pageMem.pageSize();
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        if (!metricsEnabled)
            return 0;

        return readPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        if (!metricsEnabled)
            return 0;

        return writtenPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        if (!metricsEnabled)
            return 0;

        return replacedPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        return offHeapSize.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffheapUsedSize() {
        if (!metricsEnabled)
            return 0;

        return pageMem.loadedPages() * pageMem.systemPageSize();
    }

    /**
     * @param size Region size.
     */
    public void updateOffHeapSize(long size) {
        this.offHeapSize.add(size);
    }

    /**
     * @param size Checkpoint buffer size.
     */
    public void updateCheckpointBufferSize(long size) {
        this.checkpointBufSize.add(size);
    }

    /**
     * Updates pageReplaceRate metric.
     */
    public void updatePageReplaceRate(long pageAge) {
        if (metricsEnabled) {
            pageReplaceRate.increment();

            pageReplaceAge.add(pageAge);

            replacedPages.increment();
        }
    }

    /**
     * Updates page read.
     */
    public void onPageRead() {
        if (metricsEnabled)
            readPages.increment();
    }

    /**
     * Updates page written.
     */
    public void onPageWritten() {
        if (metricsEnabled)
            writtenPages.increment();
    }

    /**
     * Increments dirtyPages counter.
     */
    public void incrementDirtyPages() {
        if (metricsEnabled)
            dirtyPages.increment();
    }

    /**
     * Decrements dirtyPages counter.
     */
    public void decrementDirtyPages() {
        if (metricsEnabled)
            dirtyPages.decrement();
    }

    /**
     * Resets dirtyPages counter to zero.
     */
    public void resetDirtyPages() {
        if (metricsEnabled)
            dirtyPages.reset();
    }

    /** */
    public LongAdderMetric totalAllocatedPages() {
        return totalAllocatedPages;
    }

    public MemoryPageMetrics groupMemoryPageMetrics(Integer cacheGrpId) {
        return cacheGrpMetrics.computeIfAbsent(
            cacheGrpId,
            grpId -> Optional.of(kernalContext)
                .map(GridKernalContext::cache)
                .map(cache -> cache.cacheGroupDescriptors().get(grpId))
                .map(desc -> {
                    String cacheGrpName = desc.cacheOrGroupName();
                    String registryName = cacheGroupMetricsRegistryName(cacheGrpName);
                    MetricRegistry registry = kernalContext.metric().registry(registryName);

                    MemoryPageMetrics metrics = MemoryPageMetricsImpl.builder(registry)
                        .pageAllocationCallback(new LongAdderMetricDelegate(dataRegionMemoryPageMetrics.totalAllocatedPages()))
                        .dataPagesSizeCallback(new LongAdderMetricDelegate(dataRegionMemoryPageMetrics.dataPagesSize()))
                        .idxPagesSizeCallback(new LongAdderMetricDelegate(dataRegionMemoryPageMetrics.idxPagesSize()))
                        .freeListPagesSizeCallback(new LongAdderMetricDelegate(dataRegionMemoryPageMetrics.freelistPagesSize()))
                        .metaPagesSizeCallback(new LongAdderMetricDelegate(dataRegionMemoryPageMetrics.metaPagesSize()))
                        .freePagesSizeCallback(new LongAdderMetricDelegate(dataRegionMemoryPageMetrics.freePagesSize()))
                        .build();

                    metrics.setMetricsEnabled(metricsEnabled);

                    return metrics;
                })
                .orElse(dataRegionMemoryPageMetrics)
        );
    }

    /**
     * Updates eviction rate metric.
     */
    public void updateEvictionRate() {
        if (metricsEnabled)
            evictRate.increment();
    }

    /**
     *
     */
    public void incrementLargeEntriesPages() {
        if (metricsEnabled)
            largeEntriesPages.increment();
    }

    /**
     *
     */
    public void decrementLargeEntriesPages() {
        if (metricsEnabled)
            largeEntriesPages.decrement();
    }

    /**
     * Enable metrics.
     */
    public void enableMetrics() {
        metricsEnabled = true;
        dataRegionMemoryPageMetrics.setMetricsEnabled(true);
        cacheGrpMetrics.values().forEach(m -> m.setMetricsEnabled(true));
    }

    /**
     * Disable metrics.
     */
    public void disableMetrics() {
        metricsEnabled = false;
        dataRegionMemoryPageMetrics.setMetricsEnabled(false);
        cacheGrpMetrics.values().forEach(m -> m.setMetricsEnabled(false));
    }

    /**
     * @param persistenceEnabled Persistence enabled.
     */
    public void persistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;
    }

    /**
     * @param pageMem Page mem.
     */
    public void pageMemory(PageMemory pageMem) {
        this.pageMem = pageMem;

        MetricRegistry mreg = kernalContext.metric().registry(metricName(DATAREGION_METRICS_PREFIX, memPlcCfg.getName()));

        mreg.register("PagesFillFactor",
            this::getPagesFillFactor,
            "The percentage of the used space.");

        mreg.register("PhysicalMemoryPages",
            this::getPhysicalMemoryPages,
            "Number of pages residing in physical RAM.");

        mreg.register("OffheapUsedSize",
            this::getOffheapUsedSize,
            "Offheap used size in bytes.");

        mreg.register("TotalAllocatedSize",
            this::getTotalAllocatedSize,
            "Gets a total size of memory allocated in the data region, in bytes");

        mreg.register("PhysicalMemorySize",
            this::getPhysicalMemorySize,
            "Gets total size of pages loaded to the RAM, in bytes");

        mreg.register("UsedCheckpointBufferSize",
            this::getUsedCheckpointBufferSize,
            "Gets used checkpoint buffer size in bytes");
    }

    /**
     * @param rateTimeInterval Time interval (in milliseconds) used to calculate allocation/eviction rate.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public void rateTimeInterval(long rateTimeInterval) {
        this.rateTimeInterval = rateTimeInterval;

        allocRate.reset(rateTimeInterval, subInts);
        evictRate.reset(rateTimeInterval, subInts);
        pageReplaceRate.reset(rateTimeInterval, subInts);
        pageReplaceAge.reset(rateTimeInterval, subInts);
    }

    /**
     * Sets number of subintervals the whole rateTimeInterval will be split into to calculate allocation rate.
     *
     * @param subInts Number of subintervals.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public void subIntervals(int subInts) {
        assert subInts > 0;

        if (this.subInts == subInts)
            return;

        if (rateTimeInterval / subInts < 10)
            subInts = (int) rateTimeInterval / 10;

        allocRate.reset(rateTimeInterval, subInts);
        evictRate.reset(rateTimeInterval, subInts);
        pageReplaceRate.reset(rateTimeInterval, subInts);
        pageReplaceAge.reset(rateTimeInterval, subInts);
    }

    public MemoryPageMetrics memoryPageMetrics() {
        return dataRegionMemoryPageMetrics;
    }

    /**
     * Clear metrics.
     */
    public void clear() {
        totalAllocatedPages.reset();
        largeEntriesPages.reset();
        dirtyPages.reset();
        readPages.reset();
        writtenPages.reset();
        replacedPages.reset();
        offHeapSize.reset();
        checkpointBufSize.reset();
        allocRate.reset();
        evictRate.reset();
        pageReplaceRate.reset();
        pageReplaceAge.reset();

        dataRegionMemoryPageMetrics.reset();
        cacheGrpMetrics.values().forEach(MemoryPageMetrics::reset);
    }

    /** @param time Time to add to {@code totalThrottlingTime} metric in milliseconds. */
    public void addThrottlingTime(long time) {
        if (metricsEnabled)
            totalThrottlingTime.add(time);
    }
}
