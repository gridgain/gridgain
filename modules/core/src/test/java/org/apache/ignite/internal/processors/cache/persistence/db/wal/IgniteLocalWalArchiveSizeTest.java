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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.WalArchiveSize;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;

/**
 * Class for testing the maximum archive size.
 */
public class IgniteLocalWalArchiveSizeTest extends GridCommonAbstractTest {
    /** Observer of the size of WAL archive. */
    @Nullable private volatile WalArchiveSizeObserver walArchiveObserver;

    /** Wal compaction enabled flag. */
    private boolean walCompactionEnabled;

    /** Wal archive enabled flag. */
    private boolean walArchiveEnabled = true;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopObserver(null);

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopObserver(null);

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
                    .setWalSegmentSize((int)U.MB)
                    .setWalSegments(10)
                    .setMaxWalArchiveSize(5 * U.MB)
                    .setWalCompactionEnabled(walCompactionEnabled)
                    .setWalArchivePath(walArchiveEnabled ? DFLT_WAL_ARCHIVE_PATH : DFLT_WAL_PATH)
            );
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        IgniteEx n = super.startGrid(idx);

        WalArchiveSizeObserver walArchiveSizeObserver0 = new WalArchiveSizeObserver(
            GridTestUtils.getFieldValueHierarchy(n.context().cache().context().wal(), "walArchiveDir"),
            GridTestUtils.getFieldValueHierarchy(n.context().cache().context().wal(), "walArchiveSize"),
            n.configuration().getDataStorageConfiguration().getMaxWalArchiveSize()
        );

        walArchiveSizeObserver0.start();
        walArchiveObserver = walArchiveSizeObserver0;

        U.await(walArchiveSizeObserver0.start);

        n.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        return n;
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will not be a fail node due to the inability to clear WAL archive
     * if only archiving will work.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotFailAndExceedMaxArchiverOnly() throws Exception {
        checkNotFailedAndNotExceedMaxWalArchiveSize();
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will not be a fail node due to the inability to clear WAL archive
     * if archiving and compaction works.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotFailAndExceedMaxArchiverWithCompaction() throws Exception {
        walCompactionEnabled = true;

        checkNotFailedAndNotExceedMaxWalArchiveSize();
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will not be a fail node due to the inability to clear WAL archive
     * if only rollOver will work.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotFailAndExceedMaxRollOverOnly() throws Exception {
        walArchiveEnabled = false;

        checkNotFailedAndNotExceedMaxWalArchiveSize();
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will not be a fail node due to the inability to clear WAL archive
     * if rollOver and compaction works.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotFailAndExceedMaxRollOverWithCompaction() throws Exception {
        walArchiveEnabled = false;
        walCompactionEnabled = true;

        checkNotFailedAndNotExceedMaxWalArchiveSize();
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will be a fail node due to the inability to clear WAL archive
     * if only archiving will work.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailAndExceedMaxArchiverOnly() throws Exception {
        checkFailedAndNotExceedMaxWalArchiveSize();
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will be a fail node due to the inability to clear WAL archive
     * if archiving and compaction works.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailAndExceedMaxArchiverWithCompaction() throws Exception {
        walCompactionEnabled = true;

        checkFailedAndNotExceedMaxWalArchiveSize();
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will be a fail node due to the inability to clear WAL archive
     * if only rollOver will work.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailAndExceedMaxRollOverOnly() throws Exception {
        walArchiveEnabled = false;

        checkFailedAndNotExceedMaxWalArchiveSize();
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will be a fail node due to the inability to clear WAL archive
     * if rollOver and compaction works.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailAndExceedMaxRollOverWithCompaction() throws Exception {
        walArchiveEnabled = false;
        walCompactionEnabled = true;

        checkFailedAndNotExceedMaxWalArchiveSize();
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will not be a fail node due to the inability to clear WAL archive.
     *
     * @throws Exception If failed.
     */
    private void checkNotFailedAndNotExceedMaxWalArchiveSize() throws Exception {
        IgniteEx n = startGrid(0);

        for (int i = 0; i < 1_000; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(100 * U.KB)]);

        stopObserver(observer -> {
            assertFalse(observer.exceed);
            assertFalse(observer.walArchiveSize.nodeFailure());
        });
    }

    /**
     * Check that maximum WAL archive size will not be exceeded and
     * that there will be a fail node due to the inability to clear WAL archive.
     *
     * @throws Exception If failed.
     */
    private void checkFailedAndNotExceedMaxWalArchiveSize() throws Exception {
        IgniteEx n = startGrid(0);

        GridTestUtils.assertThrows(log, () -> {
            try (Transaction tx = n.transactions().txStart()) {
                for (int i = 0; i < 1_000; i++)
                    n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(100 * U.KB)]);

                tx.commit();
            }
        }, Throwable.class, null);

        stopObserver(observer -> {
            assertFalse(observer.exceed);
            assertTrue(observer.walArchiveSize.nodeFailure());
        });
    }

    /**
     * Stop {@link #walArchiveObserver}.
     *
     * @param c Consumer of stopped observer.
     * @throws Exception If failed.
     */
    private void stopObserver(@Nullable Consumer<WalArchiveSizeObserver> c) throws Exception {
        WalArchiveSizeObserver observer = walArchiveObserver;

        if (observer != null && !observer.stop) {
            observer.stop = true;

            observer.join();

            if (c != null)
                c.accept(observer);
        }
    }

    /**
     * Class for tracking not exceeding size of WAL archive.
     */
    private static class WalArchiveSizeObserver extends Thread {
        /** Wal archive directory. */
        final File dir;

        /** Holder of WAL archive size information. */
        final WalArchiveSize walArchiveSize;

        /** Max size(in bytes) of WAL archive directory. */
        final long max;

        /** {@link #max} was exceeded. */
        volatile boolean exceed;

        /** Latch start of thread. */
        final CountDownLatch start = new CountDownLatch(1);

        /** Stop flag. */
        volatile boolean stop;

        /**
         * Constructor.
         *
         * @param dir Wal archive directory.
         * @param walArchiveSize Holder of WAL archive size information.
         * @param max Max size(in bytes) of WAL archive directory.
         */
        public WalArchiveSizeObserver(File dir, WalArchiveSize walArchiveSize, long max) {
            this.dir = dir;
            this.walArchiveSize = walArchiveSize;
            this.max = max;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            start.countDown();

            while (!stop && !exceed) {
                long size = 0;

                File[] files = dir.listFiles();

                for (File file : files)
                    size += file.length();

                if (size > max) {
                    exceed = true;

                    Object sizes;
                    long currSize;

                    synchronized (walArchiveSize) {
                        currSize = walArchiveSize.currentSize();
                        sizes = GridTestUtils.getFieldValueHierarchy(walArchiveSize, "sizes");
                    }

                    log.error("Excess [maxSize=" + U.humanReadableByteCount(max) +
                        ", fileSizes=" + U.humanReadableByteCount(size) +
                        ", currSize=" + U.humanReadableByteCount(currSize) +
                        ", files=" + Stream.of(files).map(File::getName).collect(toList()) +
                        ", sizes=" + sizes + ']');
                }
            }
        }
    }
}
