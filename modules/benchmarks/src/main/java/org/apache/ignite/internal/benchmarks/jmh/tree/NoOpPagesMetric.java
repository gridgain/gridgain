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

package org.apache.ignite.internal.benchmarks.jmh.tree;

import org.apache.ignite.internal.pagemem.PageCategory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesMetric;

/**
 * No op {@link PagesMetric}.
 */
public class NoOpPagesMetric implements PagesMetric {
    /** {@inheritDoc} */
    @Override public long physicalMemoryDataPagesSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryIndexPagesSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryFreelistPagesSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryMetaPagesSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryFreePagesSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void freePageUsed() {

    }

    /** {@inheritDoc} */
    @Override public void pageAllocated(PageCategory category) {

    }

    /** {@inheritDoc} */
    @Override public void pageFromReuseList(PageCategory category) {

    }

    /** {@inheritDoc} */
    @Override public void freePagesIncreased(int count) {

    }

    /** {@inheritDoc} */
    @Override public void reusePageIncreased(int count, PageCategory category) {

    }
}
