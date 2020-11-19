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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.NavigableMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

/**
 * Class for testing {@link WalArchiveSize}.
 */
public class WalArchiveSizeTest extends GridCommonAbstractTest {
    /**
     * Checking whether {@link WalArchiveSize#unlimited} works correctly.
     */
    @Test
    public void testUnlimited() {
        WalArchiveSize size = new WalArchiveSize(log, U.MB);

        assertEquals(size.maxSize(), U.MB);
        assertFalse(size.unlimited());

        size = new WalArchiveSize(log, UNLIMITED_WAL_ARCHIVE);

        assertEquals(size.maxSize(), UNLIMITED_WAL_ARCHIVE);
        assertTrue(size.unlimited());
    }

    /**
     * Checking whether {@link WalArchiveSize#exceedMax} works correctly.
     */
    @Test
    public void testExceedMax() {
        WalArchiveSize size = new WalArchiveSize(log, 5 * U.MB);

        for (long i = 0; i < 5; i++) {
            size.updateCurrentSize(i, U.MB);
            assertFalse(size.exceedMax());
        }

        size.updateCurrentSize(5, U.MB);
        assertTrue(size.exceedMax());
    }

    /**
     * Checking whether {@link WalArchiveSize#currentSegments} and
     * {@link WalArchiveSize#currentSize} works correctly.
     */
    @Test
    public void testCurrentSegmentsAndSize() {
        WalArchiveSize size = new WalArchiveSize(log, 5 * U.MB);

        for (long i = 0; i < 5; i++) {
            size.updateCurrentSize(i, U.MB);

            assertEquals(U.MB * (i + 1), size.currentSize());
            assertEquals(0, size.reservedSize());
            assertEquals(0, size.availableDelete());

            NavigableMap<Long, Long> segments = size.currentSegments();
            assertEquals(i + 1, segments.size());

            for (long j = 0; j < i + 1; j++)
                assertEquals(segments.get(j).longValue(), U.MB);
        }

        for (int i = 0; i < 2; i++) {
            size.updateCurrentSize(0, -U.MB);
            assertFalse(size.currentSegments().containsKey(0L));
        }
    }

    /**
     * Checking whether {@link WalArchiveSize#availableDelete},
     * {@link WalArchiveSize#updateLastCheckpointSegmentIndex} and
     * {@link WalArchiveSize#updateMinReservedSegmentIndex} works correctly.
     */
    @Test
    public void testAvailableDelete() {
        WalArchiveSize size = new WalArchiveSize(log, 5 * U.MB);

        for (long i = 0; i < 5; i++)
            size.updateCurrentSize(i, U.MB);

        assertEquals(0, size.availableDelete());

        size.updateLastCheckpointSegmentIndex(1);
        assertEquals(1, size.availableDelete());

        size.updateMinReservedSegmentIndex(2L);
        assertEquals(1, size.availableDelete());

        size.updateMinReservedSegmentIndex(null);
        assertEquals(1, size.availableDelete());

        size.updateLastCheckpointSegmentIndex(2);
        assertEquals(2, size.availableDelete());

        size.updateMinReservedSegmentIndex(1L);
        assertEquals(1, size.availableDelete());

        size.updateCurrentSize(0, -U.MB);
        assertEquals(0, size.availableDelete());

        size.updateCurrentSize(0, U.MB);
        assertEquals(1, size.availableDelete());
    }

    /**
     * Checking whether {@link WalArchiveSize#reserve}, {@link WalArchiveSize#release} and
     * {@link WalArchiveSize#reservedSize} works correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReserve() throws Exception {
        WalArchiveSize size = new WalArchiveSize(log, 5 * U.MB);

        for (long i = 0; i < 5; i++) {
            size.reserve(U.MB, null, null);

            assertFalse(size.exceedMax());
            assertEquals(U.MB * (i + 1), size.reservedSize());
        }

        for (long i = 0; i < 5; i++) {
            size.release(U.MB);

            assertFalse(size.exceedMax());
            assertEquals(U.MB * (5 - (i + 1)), size.reservedSize());
        }

        for (long i = 0; i < 5; i++)
            size.updateCurrentSize(i, U.MB);

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
            size.reserve(U.MB, (low, high) -> {
                assertEquals(0, low.longValue());
                assertEquals(1, high.longValue());

                size.updateCurrentSize(low, -U.MB);
            }, latch::countDown);

            return null;
        });

        U.await(latch);
        size.updateLastCheckpointSegmentIndex(1);

        fut.get(1_000);

        assertFalse(size.exceedMax());
        assertEquals(4 * U.MB, size.currentSize());
        assertEquals(U.MB, size.reservedSize());
    }
}
