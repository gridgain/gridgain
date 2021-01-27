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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagemem.PageCategory;

public class PagesMetricNoStoreImpl implements PagesMetric {
    private AtomicLong physicalMemoryDataPagesSize = new AtomicLong(0);
    /** SQL indexes */
    private AtomicLong physicalMemoryIndexPagesSize = new AtomicLong(0);
    /** Reuse list. */
    private AtomicLong physicalMemoryFreelistPagesSize = new AtomicLong(0);
    /** meta, tracking */
    private AtomicLong physicalMemoryMetaPagesSize = new AtomicLong(0);
    /** Preallocated size */
    private AtomicLong physicalMemoryFreePagesSize = new AtomicLong(0);

    /** {@inheritDoc} */
    @Override public void pageAllocated(PageCategory category) {
        physicalMemoryFreePagesSize.decrementAndGet();
        switch (category) {
            case DATA:
                physicalMemoryDataPagesSize.getAndIncrement();
                break;
            case REUSE:
                physicalMemoryDataPagesSize.getAndIncrement();
                break;
            case INDEX:
                physicalMemoryIndexPagesSize.incrementAndGet();
                break;
            case META:
                physicalMemoryMetaPagesSize.incrementAndGet();
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void pageFromReuseList(PageCategory category) {
        physicalMemoryFreelistPagesSize.decrementAndGet();
        switch (category) {
            case META:
                physicalMemoryMetaPagesSize.incrementAndGet();
                break;
            case INDEX:
                physicalMemoryIndexPagesSize.incrementAndGet();
                break;
            case DATA:
                physicalMemoryDataPagesSize.incrementAndGet();
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void reusePageIncreased(int count, PageCategory category) {
        assert category != PageCategory.REUSE;

        switch (category) {
            case DATA:
                physicalMemoryDataPagesSize.addAndGet(-count);
                break;
            case META:
                physicalMemoryMetaPagesSize.addAndGet(-count);
                break;

            case INDEX:
                physicalMemoryIndexPagesSize.addAndGet(-count);
                break;

        }

        physicalMemoryFreelistPagesSize.addAndGet(count);
    }

    /** {@inheritDoc} */
    @Override public void freePageUsed() {
        physicalMemoryFreePagesSize.decrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void freePagesIncreased(int count) {
        physicalMemoryFreePagesSize.addAndGet(count);
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryDataPagesSize() {
        return physicalMemoryDataPagesSize.get();
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryIndexPagesSize() {
        return physicalMemoryIndexPagesSize.get();
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryFreelistPagesSize() {
        return physicalMemoryFreelistPagesSize.get();
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryMetaPagesSize() {
        return physicalMemoryMetaPagesSize.get();
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryFreePagesSize() {
        return physicalMemoryFreePagesSize.get();
    }
}
