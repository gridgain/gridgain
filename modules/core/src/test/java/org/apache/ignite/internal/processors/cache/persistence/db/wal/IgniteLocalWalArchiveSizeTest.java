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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WalArchiveSize;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.Repeat;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;

/**
 * Class for testing not exceeding {@link DataStorageConfiguration#getMaxWalArchiveSize()}.
 */
@RunWith(Parameterized.class)
public class IgniteLocalWalArchiveSizeTest extends GridCommonAbstractTest {
    /** Watcher of physical exceeding of the archive. */
    @Nullable private volatile WalArchiveWatcher walArchiveWatcher;

    /** WAL archive enabled flag. */
    @Parameterized.Parameter(0)
    public boolean walArchiveEnabled;

    /** WAL compaction enabled flag. */
    @Parameterized.Parameter(1)
    public boolean walCompactionEnabled;

    /**
     * Generate test's parameters.
     *
     * @return Test's parameters.
     */
    @Parameterized.Parameters(name = "walArchiveEnabled={0}, walCompactionEnabled={1}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
            new Object[] {true, false},
            new Object[] {true, true},
            new Object[] {false, false},
            new Object[] {false, true}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopWatcher(null);

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopWatcher(null);

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
                    .setMaxWalArchiveSize(5 * U.MB)
                    .setWalSegmentSize((int)U.MB)
                    .setWalCompactionEnabled(walCompactionEnabled)
                    .setWalArchivePath(walArchiveEnabled ? DFLT_WAL_ARCHIVE_PATH : DFLT_WAL_PATH)
            );
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        IgniteEx n = super.startGrid(idx);

        WalArchiveWatcher watcher;
        walArchiveWatcher = (watcher = new WalArchiveWatcher(n));

        watcher.start();
        U.await(watcher.start);

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        return n;
    }

    /**
     * Checking that maximum WAL archive size is not exceeded.
     *
     * @throws Exception If failed.
     */
    @Test
    @Repeat(10)
    public void test() throws Exception {
        IgniteEx n = startGrid(0);

        for (int i = 0; i < 1_000; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(100 * U.KB)]);

        stopWatcher(watcher -> assertFalse(watcher.exceed));
        assertNull(n.context().failure().failureContext());
    }

    /**
     * Stop watcher if not absent.
     *
     * @param consumer Watcher consumer.
     * @throws Exception If failed.
     */
    protected void stopWatcher(@Nullable Consumer<WalArchiveWatcher> consumer) throws Exception {
        WalArchiveWatcher watcher = walArchiveWatcher;

        if (watcher != null) {
            watcher.stop = true;

            U.join(watcher);

            if (consumer != null)
                consumer.accept(watcher);

            walArchiveWatcher = null;
        }
    }

    /**
     * Class for tracking the physical excess of WAL archive.
     */
    protected static class WalArchiveWatcher extends Thread {
        /** Path to WAL archive dir. */
        final File walArchivePath;

        /** WAL archive size. */
        final WalArchiveSize walArchiveSize;

        /** Exceeding {@link WalArchiveSize#maxSize()} flag. */
        volatile boolean exceed;

        /** Start thread latch. */
        final CountDownLatch start = new CountDownLatch(1);

        /** Stop flag. */
        volatile boolean stop;

        /**
         * Constructor.
         *
         * @param n Node.
         */
        private WalArchiveWatcher(IgniteEx n) {
            IgniteWriteAheadLogManager wal = n.context().cache().context().wal();

            assertNotNull(walArchivePath = GridTestUtils.getFieldValueHierarchy(wal, "walArchiveDir"));
            assertNotNull(walArchiveSize = GridTestUtils.getFieldValueHierarchy(wal, "walArchiveSize"));

            assertEquals(
                n.configuration().getDataStorageConfiguration().getMaxWalArchiveSize(),
                walArchiveSize.maxSize()
            );
        }

        /** {@inheritDoc} */
        @Override public void run() {
            start.countDown();

            while (!stop && !exceed) {
                File[] files = walArchivePath.listFiles();

                long size = Stream.of(files).mapToLong(File::length).sum();

                if (size > walArchiveSize.maxSize()) {
                    synchronized (walArchiveSize) {
                        Map<Long, String> segments = walArchiveSize.currentSegments().entrySet().stream()
                            .collect(toMap(Map.Entry::getKey, e -> U.humanReadableByteCount(e.getValue()),
                                Objects::toString, TreeMap::new));

                        Map<String, String> physicalFiles = Stream.of(files)
                            .collect(toMap(File::getName, f -> U.humanReadableByteCount(f.length()),
                                Objects::toString, TreeMap::new));

                        log.error("There was an excess of WAL archive [physicalSize=" + U.humanReadableByteCount(size)
                            + ", maxSize=" + U.humanReadableByteCount(walArchiveSize.maxSize())
                            + ", currentSize=" + U.humanReadableByteCount(walArchiveSize.currentSize())
                            + ", reservedSize=" + U.humanReadableByteCount(walArchiveSize.reservedSize())
                            + ", physicalFiles=" + physicalFiles + ", segments=" + segments
                            + ']');

                        exceed = true;
                    }
                }
            }
        }
    }
}
