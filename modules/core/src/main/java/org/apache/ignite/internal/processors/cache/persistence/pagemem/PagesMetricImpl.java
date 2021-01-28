/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.pagemem.PageCategory;

/**
 * Collects metrics per data region.
 */
public class PagesMetricImpl implements PagesMetric {
    /** Data pages. */
    private final LongAdder physicalMemoryDataPagesSize = new LongAdder();
    /** Indexes. */
    private final LongAdder physicalMemoryIndexPagesSize = new LongAdder();
    /** Reuse list. */
    private final LongAdder physicalMemoryFreelistPagesSize = new LongAdder();
    /** Meta, tracking. */
    private final LongAdder physicalMemoryMetaPagesSize = new LongAdder();
    /** Preallocated size. */
    private final LongAdder physicalMemoryFreePagesSize = new LongAdder();

    /** {@inheritDoc} */
    @Override public void pageAllocated(PageCategory category) {
        physicalMemoryFreePagesSize.decrement();
        switch (category) {
            case DATA:
                physicalMemoryDataPagesSize.increment();
                break;
            case REUSE:
                physicalMemoryDataPagesSize.increment();
                break;
            case INDEX:
                physicalMemoryIndexPagesSize.increment();
                break;
            case META:
                physicalMemoryMetaPagesSize.increment();
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void pageFromReuseList(PageCategory category) {
        physicalMemoryFreelistPagesSize.decrement();
        switch (category) {
            case META:
                physicalMemoryMetaPagesSize.increment();
                break;
            case INDEX:
                physicalMemoryIndexPagesSize.increment();
                break;
            case DATA:
                physicalMemoryDataPagesSize.increment();
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void reusePageIncreased(int count, PageCategory category) {
        assert category != PageCategory.REUSE;

        switch (category) {
            case DATA:
                physicalMemoryDataPagesSize.add(-count);
                break;
            case META:
                physicalMemoryMetaPagesSize.add(-count);
                break;

            case INDEX:
                physicalMemoryIndexPagesSize.add(-count);
                break;

        }

        physicalMemoryFreelistPagesSize.add(count);
    }

    /** {@inheritDoc} */
    @Override public void freePageUsed() {
        physicalMemoryFreePagesSize.decrement();
    }

    /** {@inheritDoc} */
    @Override public void freePagesIncreased(int count) {
        physicalMemoryFreePagesSize.add(count);
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryDataPagesSize() {
        return physicalMemoryDataPagesSize.sum();
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryIndexPagesSize() {
        return physicalMemoryIndexPagesSize.sum();
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryFreelistPagesSize() {
        return physicalMemoryFreelistPagesSize.sum();
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryMetaPagesSize() {
        return physicalMemoryMetaPagesSize.sum();
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryFreePagesSize() {
        return physicalMemoryFreePagesSize.sum();
    }
}
