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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.pagemem.PageCategory;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderWithDelegateMetric;
import org.jetbrains.annotations.Nullable;

public class MemoryPageMetricsImpl implements MemoryPageMetrics {
    /** */
    private volatile boolean metricsEnabled;

    private final LongAdderMetric totalAllocatedPages;

    /** Data pages. */
    private final LongAdderMetric dataPagesSize;

    /** Indexes. */
    private final LongAdderMetric idxPagesSize;

    /** Reuse list. */
    private final LongAdderMetric freelistPagesSize;

    /** Meta, tracking. */
    private final LongAdderMetric metaPagesSize;

    /** Preallocated size. */
    private final LongAdderMetric freePagesSize;

    /** */
    private MemoryPageMetricsImpl(
        MetricRegistry metricRegistry,
        @Nullable LongAdderWithDelegateMetric.Delegate pageAllocationCb,
        @Nullable LongAdderWithDelegateMetric.Delegate dataPagesSizeCb,
        @Nullable LongAdderWithDelegateMetric.Delegate idxPagesSizeCb,
        @Nullable LongAdderWithDelegateMetric.Delegate freeListPagesSizeCb,
        @Nullable LongAdderWithDelegateMetric.Delegate metaPagesSizeCb,
        @Nullable LongAdderWithDelegateMetric.Delegate freePagesSizeCb
    ) {
        totalAllocatedPages = createMetricWithOptionalDelegate(
            metricRegistry, "TotalAllocatedPages", "Cache group total allocated pages.", pageAllocationCb
        );

        dataPagesSize = createMetricWithOptionalDelegate(
            metricRegistry, "PhysicalMemoryDataPagesSize", "Pages amount used for DATA.", dataPagesSizeCb
        );

        idxPagesSize = createMetricWithOptionalDelegate(
            metricRegistry, "PhysicalMemoryIndexPagesSize", "Pages amount used for indexes.", idxPagesSizeCb
        );

        freelistPagesSize = createMetricWithOptionalDelegate(
            metricRegistry, "PhysicalMemoryFreelistPagesSize", "Free pages count.", freeListPagesSizeCb
        );

        metaPagesSize = createMetricWithOptionalDelegate(
            metricRegistry, "PhysicalMemoryMetaPagesSize", "Pages amount used for metadata.", metaPagesSizeCb
        );

        freePagesSize = createMetricWithOptionalDelegate(
            metricRegistry, "PhysicalMemoryFreePagesSize", "Preallocated pages count.", freePagesSizeCb
        );
    }

    public static final class Builder {
        private final MetricRegistry metricRegistry;

        private LongAdderWithDelegateMetric.Delegate pageAllocationCb;
        private LongAdderWithDelegateMetric.Delegate dataPagesSizeCb;
        private LongAdderWithDelegateMetric.Delegate idxPagesSizeCb;
        private LongAdderWithDelegateMetric.Delegate freeListPagesSizeCb;
        private LongAdderWithDelegateMetric.Delegate metaPagesSizeCb;
        private LongAdderWithDelegateMetric.Delegate freePagesSizeCb;

        Builder(MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
        }

        public Builder pageAllocationCallback(LongAdderWithDelegateMetric.Delegate cb) {
            pageAllocationCb = cb;
            return this;
        }

        public Builder dataPagesSizeCallback(LongAdderWithDelegateMetric.Delegate cb) {
            dataPagesSizeCb = cb;
            return this;
        }

        public Builder idxPagesSizeCallback(LongAdderWithDelegateMetric.Delegate cb) {
            idxPagesSizeCb = cb;
            return this;
        }

        public Builder freeListPagesSizeCallback(LongAdderWithDelegateMetric.Delegate cb) {
            freeListPagesSizeCb = cb;
            return this;
        }

        public Builder metaPagesSizeCallback(LongAdderWithDelegateMetric.Delegate cb) {
            metaPagesSizeCb = cb;
            return this;
        }

        public Builder freePagesSizeCallback(LongAdderWithDelegateMetric.Delegate cb) {
            freePagesSizeCb = cb;
            return this;
        }

        public MemoryPageMetricsImpl build() {
            return new MemoryPageMetricsImpl(
                metricRegistry,
                pageAllocationCb,
                dataPagesSizeCb,
                idxPagesSizeCb,
                freeListPagesSizeCb,
                metaPagesSizeCb,
                freePagesSizeCb
            );
        }
    }

    public static Builder builder(MetricRegistry metricRegistry) {
        return new Builder(metricRegistry);
    }

    private static LongAdderMetric createMetricWithOptionalDelegate(
        MetricRegistry metricRegistry, String name, String desc, @Nullable LongAdderWithDelegateMetric.Delegate delegate
    ) {
        return delegate == null ?
            metricRegistry.longAdderMetric(name, desc) :
            metricRegistry.longAdderMetric(name, delegate, desc);
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric totalAllocatedPages() {
        return totalAllocatedPages;
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric dataPagesSize() {
        return dataPagesSize;
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric idxPagesSize() {
        return idxPagesSize;
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric freelistPagesSize() {
        return freelistPagesSize;
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric metaPagesSize() {
        return metaPagesSize;
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric freePagesSize() {
        return freePagesSize;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        totalAllocatedPages.reset();
        dataPagesSize.reset();
        idxPagesSize.reset();
        freelistPagesSize.reset();
        metaPagesSize.reset();
        freePagesSize.reset();
    }

    /** {@inheritDoc} */
    @Override public void pageAllocated(PageCategory category) {
        if (!metricsEnabled)
            return;

        freePagesSize.decrement();
        switch (category) {
            case DATA:
            case REUSE:
                dataPagesSize.increment();
                break;
            case INDEX:
                idxPagesSize.increment();
                break;
            case META:
                metaPagesSize.increment();
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void pageReused(PageCategory category) {
        if (!metricsEnabled)
            return;

        freelistPagesSize.decrement();
        switch (category) {
            case META:
                metaPagesSize.increment();
                break;
            case INDEX:
                idxPagesSize.increment();
                break;
            case DATA:
                dataPagesSize.increment();
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void pageReleased(int cnt, PageCategory category) {
        if (!metricsEnabled)
            return;

        assert category != PageCategory.REUSE;

        switch (category) {
            case DATA:
                dataPagesSize.add(-cnt);
                break;
            case META:
                metaPagesSize.add(-cnt);
                break;
            case INDEX:
                idxPagesSize.add(-cnt);
                break;
        }

        freelistPagesSize.add(cnt);
    }

    /** {@inheritDoc} */
    @Override public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }
}
