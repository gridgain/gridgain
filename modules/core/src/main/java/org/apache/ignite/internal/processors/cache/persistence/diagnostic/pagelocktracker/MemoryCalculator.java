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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MemoryCalculator {
    /** */
    private final AtomicLong heapUsed = new AtomicLong();

    /** */
    private final AtomicLong offHeapUsed = new AtomicLong();

    /** */
    MemoryCalculator() {
        onHeapAllocated(16 + (8 + 16) * 2);
    }

    /** */
    public void onHeapAllocated(long bytes) {
        assert bytes >= 0;

        heapUsed.getAndAdd(bytes);
    }

    /** */
    public void onOffHeapAllocated(long bytes) {
        assert bytes >= 0;

        offHeapUsed.getAndAdd(bytes);
    }

    /** */
    public void onHeapFree(long bytes) {
        heapUsed.getAndAdd(-bytes);
    }

    /** */
    public void onOffHeapFree(long bytes) {
        offHeapUsed.getAndAdd(-bytes);
    }

    /** */
    public long getOffHeapUsed() {
        return offHeapUsed.get();
    }

    /** */
    public long getHeapUsed() {
        return heapUsed.get();
    }
}
