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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.pagemem.PageCategory;
import org.apache.ignite.internal.pagemem.PageIdAllocator;

public class PagesMetricNoStoreImpl implements PagesMetric {
    /** groupId -> partId -> counter */
    private Map<Integer, Map<Integer, AtomicInteger>> physicalMemoryDataPagesSize = new ConcurrentHashMap<>();
    /** SQL indexes */
    private long physicalMemoryIndexPagesSize = 0;
    /** Reuse list. */
    private long physicalMemoryFreelistPagesSize = 0;
    /** meta, tracking */
    private long physicalMemoryMetaPagesSize = 0;
    /** Preallocated size */
    private long physicalMemoryFreePagesSize = 0;

    /** {@inheritDoc} */
    @Override public void pageAllocated(int grpId, int part, byte pageFlag, PageCategory category) {
        physicalMemoryFreePagesSize--;
        switch (category) {
            case DATA:
                physicalMemoryDataPagesSize
                    .computeIfAbsent(grpId, id -> new ConcurrentHashMap<>())
                    .computeIfAbsent(part, id -> new AtomicInteger(0)).getAndIncrement();
                break;
            case INDEX:
                physicalMemoryIndexPagesSize++;
                break;
            case META:
                physicalMemoryMetaPagesSize++;
                break;

        }
    }

    /** {@inheritDoc} */
    @Override public void pageFromReuseList(int grpId, int partId, byte pageFlag) {
        physicalMemoryFreelistPagesSize--;
        if (partId == PageIdAllocator.INDEX_PARTITION) {
            //TODO: define index or metadata
            physicalMemoryIndexPagesSize++;
        } else {
            physicalMemoryDataPagesSize
                .computeIfAbsent(grpId, id -> new ConcurrentHashMap<>())
                .computeIfAbsent(partId, id -> new AtomicInteger(0)).getAndIncrement();
        }
    }

    /** {@inheritDoc} */
    @Override public void reusePageIncreased(int count, int grpId, int partId, byte flags) {
        if (partId == PageIdAllocator.INDEX_PARTITION) {
            //TODO: define index or metadata
            physicalMemoryIndexPagesSize -= count;
        } else {
            physicalMemoryDataPagesSize
                .computeIfAbsent(grpId, id -> new ConcurrentHashMap<>())
                .computeIfAbsent(partId, id -> new AtomicInteger(0))
                .addAndGet(-count);
        }
        physicalMemoryFreelistPagesSize += count;
    }

    /** {@inheritDoc} */
    @Override public void freePageUsed() {
        physicalMemoryFreePagesSize--;
    }

    /** {@inheritDoc} */
    @Override public void freePagesIncreased(int count) {
        physicalMemoryFreePagesSize += count;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryDataPagesSize(int grpId) {
        Map<Integer, AtomicInteger> sizes = physicalMemoryDataPagesSize.get(grpId);
        long result = 0;
        for (AtomicInteger partValue : sizes.values()) {
            result += partValue.get();
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryIndexPagesSize() {
        return physicalMemoryIndexPagesSize;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryFreelistPagesSize() {
        return physicalMemoryFreelistPagesSize;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryMetaPagesSize() {
        return physicalMemoryMetaPagesSize;
    }

    /** {@inheritDoc} */
    @Override public long physicalMemoryFreePagesSize() {
        return physicalMemoryFreePagesSize;
    }
}
