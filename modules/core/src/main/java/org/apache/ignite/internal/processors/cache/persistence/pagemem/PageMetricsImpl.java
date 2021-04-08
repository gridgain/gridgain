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

import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderWithDelegateMetric;
import org.jetbrains.annotations.Nullable;

public class PageMetricsImpl implements PageMetrics {
    /** Total pages. */
    private final LongAdderMetric totalPages;

    /** Data pages. */
    private final LongAdderMetric dataPages;

    /** Indexes. */
    private final LongAdderMetric idxPages;

    /** */
    private PageMetricsImpl(
        MetricRegistry metricRegistry,
        @Nullable LongAdderWithDelegateMetric.Delegate pageAllocationCb,
        @Nullable LongAdderWithDelegateMetric.Delegate dataPagesSizeCb,
        @Nullable LongAdderWithDelegateMetric.Delegate idxPagesSizeCb
    ) {
        totalPages = createMetricWithOptionalDelegate(
            metricRegistry, "TotalAllocatedPages", "Cache group total allocated pages.", pageAllocationCb
        );

        dataPages = createMetricWithOptionalDelegate(
            metricRegistry, "PhysicalMemoryDataPagesSize", "Pages amount used for DATA.", dataPagesSizeCb
        );

        idxPages = createMetricWithOptionalDelegate(
            metricRegistry, "PhysicalMemoryIndexPagesSize", "Pages amount used for indexes.", idxPagesSizeCb
        );
    }

    public static final class Builder {
        private final MetricRegistry metricRegistry;

        private LongAdderWithDelegateMetric.Delegate totalPagesCb;

        private LongAdderWithDelegateMetric.Delegate dataPagesCb;

        private LongAdderWithDelegateMetric.Delegate indexPagesCb;

        Builder(MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
        }

        public Builder totalPagesCallback(LongAdderWithDelegateMetric.Delegate cb) {
            totalPagesCb = cb;
            return this;
        }

        public Builder dataPagesCallback(LongAdderWithDelegateMetric.Delegate cb) {
            dataPagesCb = cb;
            return this;
        }

        public Builder indexPagesCallback(LongAdderWithDelegateMetric.Delegate cb) {
            indexPagesCb = cb;
            return this;
        }

        public PageMetricsImpl build() {
            return new PageMetricsImpl(
                metricRegistry,
                totalPagesCb,
                dataPagesCb,
                indexPagesCb
            );
        }
    }

    public static Builder builder(MetricRegistry metricRegistry) {
        return new Builder(metricRegistry);
    }

    private static LongAdderMetric createMetricWithOptionalDelegate(
        MetricRegistry metricRegistry,
        String name,
        String desc,
        @Nullable LongAdderWithDelegateMetric.Delegate delegate
    ) {
        return delegate == null ?
            metricRegistry.longAdderMetric(name, desc) :
            metricRegistry.longAdderMetric(name, delegate, desc);
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric totalPages() {
        return totalPages;
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric dataPages() {
        return dataPages;
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric indexPages() {
        return idxPages;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        totalPages.reset();
        dataPages.reset();
        idxPages.reset();
    }
}
