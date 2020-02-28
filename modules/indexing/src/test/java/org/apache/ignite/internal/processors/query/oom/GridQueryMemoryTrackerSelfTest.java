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

package org.apache.ignite.internal.processors.query.oom;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryTracker;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Grid query memory tracker tests.
 */
public class GridQueryMemoryTrackerSelfTest extends GridCommonAbstractTest {
    /**
     * Ensure initial state is all zeros.
     */
    @Test
    public void testInitialState() {
        QueryMemoryTracker tracker = new QueryMemoryTracker(null, 128L, 256L, true);

        assertEquals(0, tracker.reserved());
        assertEquals(0, tracker.maxReserved());
        assertEquals(0, tracker.writtenOnDisk());
        assertEquals(0, tracker.maxWrittenOnDisk());
        assertEquals(0, tracker.totalWrittenOnDisk());
    }

    /**
     * Ensure memory metrics are collected correctly.
     */
    @Test
    public void testMemoryReservation() {
        QueryMemoryTracker tracker = new QueryMemoryTracker(null, 128L, 256L, true);

        assertTrue(tracker.reserve(52L));

        assertEquals(52L, tracker.reserved());
        assertEquals(52L, tracker.maxReserved());

        assertTrue(tracker.reserve(30L));

        assertEquals(82L, tracker.reserved());
        assertEquals(82L, tracker.maxReserved());

        tracker.release(30L);

        assertEquals(52L, tracker.reserved());
        assertEquals(82L, tracker.maxReserved());

        assertTrue(tracker.reserve(10L));

        assertEquals(62L, tracker.reserved());
        assertEquals(82L, tracker.maxReserved());

        assertFalse(tracker.reserve(200L));

        assertEquals(262L, tracker.reserved());
        assertEquals(262L, tracker.maxReserved());

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(
            log,
            () -> new QueryMemoryTracker(null, 128L, 256L, false).reserve(512L),
            IgniteSQLException.class,
            "SQL query run out of memory: Query quota exceeded."
        );
    }

    /**
     * Ensure offloading metrics are collected correctly.
     */
    @Test
    public void testDiskOffloading() {
        QueryMemoryTracker tracker = new QueryMemoryTracker(null, 128L, 256L, true);

        tracker.swap(52L);

        assertEquals(52L, tracker.writtenOnDisk());
        assertEquals(52L, tracker.maxWrittenOnDisk());
        assertEquals(52L, tracker.totalWrittenOnDisk());

        tracker.swap(30L);

        assertEquals(82L, tracker.writtenOnDisk());
        assertEquals(82L, tracker.maxWrittenOnDisk());
        assertEquals(82L, tracker.totalWrittenOnDisk());

        tracker.unswap(30L);

        assertEquals(52L, tracker.writtenOnDisk());
        assertEquals(82L, tracker.maxWrittenOnDisk());
        assertEquals(82L, tracker.totalWrittenOnDisk());

        tracker.swap(10L);

        assertEquals(62L, tracker.writtenOnDisk());
        assertEquals(82L, tracker.maxWrittenOnDisk());
        assertEquals(92L, tracker.totalWrittenOnDisk());
    }

    /**
     * Ensure memory tracker reports to parent tracker correctly and respects the result.
     */
    @Test
    public void testParentTracker() {
        AtomicBoolean shouldFail = new AtomicBoolean();
        AtomicBoolean quotaExceeded = new AtomicBoolean();

        H2MemoryTracker parent = new H2MemoryTracker() {
            private long reserved;

            @Override public boolean reserve(long size) {
                if (shouldFail.get())
                    throw new IgniteException("Test exception");

                if (quotaExceeded.get())
                    return false;

                reserved += size;

                return true;
            }

            @Override public void release(long size) {
                reserved -= size;
            }

            @Override public long reserved() {
                return reserved;
            }

            @Override public void swap(long size) {
                // NO-OP
            }

            @Override public void unswap(long size) {
                // NO-OP
            }

            @Override public void incrementFilesCreated() {
                // NO-OP
            }

            @Override public void close() {
                // NO-OP
            }
        };

        long blockSize = 256L;
        long quota = 3 * blockSize + 16;

        QueryMemoryTracker tracker = new QueryMemoryTracker(parent, quota, blockSize, true);

        assertTrue(tracker.reserve(42L)); // first block from parent

        assertEquals(blockSize, parent.reserved());

        assertTrue(tracker.reserve(42L)); // same block since 42 * 2 < blockSize

        assertEquals(256L, parent.reserved());

        assertTrue(tracker.reserve(500L)); // reservation size is big enoght, so reservation
                                                // from parent should be equal to previos size + required bytes

        assertEquals(584L, parent.reserved());

        assertTrue(tracker.reserve(42L)); // another block but reduced just to fit the quota

        assertEquals(784L, parent.reserved());

        tracker.release(200); // here reservation from parent should shrink
                                   // so resulting size will be equal to actual reservation size

        assertEquals(426, parent.reserved());

        quotaExceeded.set(true);

        assertFalse(tracker.reserve(42L));

        shouldFail.set(true);

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, () -> tracker.reserve(42L), IgniteException.class, "Test exception");
    }
}
