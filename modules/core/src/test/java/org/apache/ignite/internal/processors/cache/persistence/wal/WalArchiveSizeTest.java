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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

/**
 * Class for testing {@link WalArchiveSize}.
 */
public class WalArchiveSizeTest extends GridCommonAbstractTest {
    /** Max wal archive size. */
    private long maxWalArchiveSize = 5 * U.MB;

    /** Wal segment size. */
    private long walSegmentSize = U.MB;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(TRANSACTIONAL))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
                    .setMaxWalArchiveSize(maxWalArchiveSize)
                    .setWalSegmentSize((int)walSegmentSize)
            );
    }

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
        assertEquals(0, size.availableDeleteSize());

        size.updateLastCheckpointSegmentIndex(1);
        assertEquals(1, size.availableDelete());
        assertEquals(U.MB, size.availableDeleteSize());

        size.updateMinReservedSegmentIndex(2L);
        assertEquals(1, size.availableDelete());
        assertEquals(U.MB, size.availableDeleteSize());

        size.updateMinReservedSegmentIndex(null);
        assertEquals(1, size.availableDelete());
        assertEquals(U.MB, size.availableDeleteSize());

        size.updateLastCheckpointSegmentIndex(2);
        assertEquals(2, size.availableDelete());
        assertEquals(2 * U.MB, size.availableDeleteSize());

        size.updateMinReservedSegmentIndex(1L);
        assertEquals(1, size.availableDelete());
        assertEquals(U.MB, size.availableDeleteSize());

        size.updateCurrentSize(0, -U.MB);
        assertEquals(0, size.availableDelete());
        assertEquals(0, size.availableDeleteSize());

        size.updateCurrentSize(0, U.MB);
        assertEquals(1, size.availableDelete());
        assertEquals(U.MB, size.availableDeleteSize());
    }

    /**
     * Checking whether {@link WalArchiveSize#reserveSize}, {@link WalArchiveSize#releaseSize} and
     * {@link WalArchiveSize#reservedSize} works correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReserve() throws Exception {
        WalArchiveSize size = new WalArchiveSize(log, 5 * U.MB);

        for (long i = 0; i < 5; i++) {
            size.reserveSize(U.MB, null, null, null);

            assertFalse(size.exceedMax());
            assertEquals(U.MB * (i + 1), size.reservedSize());
        }

        for (long i = 0; i < 5; i++) {
            size.releaseSize(U.MB, null);

            assertFalse(size.exceedMax());
            assertEquals(U.MB * (5 - (i + 1)), size.reservedSize());
        }

        for (long i = 0; i < 5; i++)
            size.updateCurrentSize(i, U.MB);

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
            size.reserveSize(U.MB, (low, high) -> {
                assertEquals(0, low.longValue());
                assertEquals(1, high.longValue());

                size.updateCurrentSize(low, -U.MB);

                return 1;
            }, latch::countDown, null);

            return null;
        });

        U.await(latch);
        size.updateLastCheckpointSegmentIndex(1);

        fut.get(1_000);

        assertFalse(size.exceedMax());
        assertEquals(4 * U.MB, size.currentSize());
        assertEquals(U.MB, size.reservedSize());

        CountDownLatch latch0 = new CountDownLatch(1);
        CountDownLatch latch1 = new CountDownLatch(1);

        fut = GridTestUtils.runAsync(() -> {
            size.reserveSize(U.MB, (low, high) -> {
                assertEquals(1, low.longValue());
                assertEquals(2, high.longValue());

                if (latch0.getCount() > 0 || latch1.getCount() > 0)
                    return 0;
                else {
                    size.updateCurrentSize(low, -U.MB);

                    return 1;
                }
            }, () -> (latch0.getCount() > 0 ? latch0 : latch1).countDown(), null);

            return null;
        });

        U.await(latch0);
        size.updateLastCheckpointSegmentIndex(2);

        U.await(latch1);
        size.updateMinReservedSegmentIndex(3L);

        fut.get(1_000);
        assertFalse(size.exceedMax());
        assertEquals(3 * U.MB, size.currentSize());
        assertEquals(2 * U.MB, size.reservedSize());
    }

    /**
     * Checking exceeded WAL archive at start of node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExceedMaxWalArchiveSizeOnStartNode() throws Exception {
        IgniteEx n = startGrid(0);

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        for (int i = 0; i < 10_000; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(10 * U.KB)]);

        stopGrid(0);

        maxWalArchiveSize = 2 * U.MB;
        walSegmentSize = U.MB;

        // Expectation of not exceeding.
        n = startGrid(0);

        WalArchiveSize size = GridTestUtils.getFieldValueHierarchy(
            n.context().cache().context().wal(),
            "walArchiveSize"
        );

        assertFalse(size.exceedMax());

        stopGrid(0);

        // Expectation of exceeding.
        maxWalArchiveSize = 513 * U.KB;
        walSegmentSize = 512 * U.KB;

        GridTestUtils.assertThrows(
            log, () -> startGrid(0),
            IgniteCheckedException.class,
            "Exceeding maximum WAL archive size"
        );
    }
}
