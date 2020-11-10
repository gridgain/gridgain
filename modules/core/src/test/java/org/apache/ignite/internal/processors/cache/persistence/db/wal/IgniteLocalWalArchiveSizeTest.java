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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Class for testing the maximum archive size.
 */
public class IgniteLocalWalArchiveSizeTest extends GridCommonAbstractTest {
    /** Observer of the size of WAL archive. */
    @Nullable private volatile WalArchiveSizeObserver walArchiveObserver;

    /** Wal compaction enabled flag. */
    private boolean walCompactionEnabled;



    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopObserver();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopObserver();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
                    .setWalSegmentSize((int)U.MB)
                    .setWalSegments(10)
                    .setMaxWalArchiveSize(5 * U.MB)
                    .setWalCompactionEnabled(walCompactionEnabled)
            ).setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        IgniteEx n = super.startGrid(idx);

        walArchiveObserver = new WalArchiveSizeObserver(
            GridTestUtils.getFieldValueHierarchy(n.context().cache().context().wal(), "walArchiveDir"),
            n.configuration().getDataStorageConfiguration().getMaxWalArchiveSize()
        );

        walArchiveObserver.start();
        U.await(walArchiveObserver.start);

        n.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        return n;
    }

    @Test
    public void testArchiverOnly() throws Exception {
        checkNotExceedMaxWalArchiveSize();
    }

    @Test
    public void testArchiverWithCompaction() throws Exception {
        walCompactionEnabled = true;

        checkNotExceedMaxWalArchiveSize();
    }

    /**
     * Checking that max WAL archive size is not exceeded.
     */
    private void checkNotExceedMaxWalArchiveSize() throws Exception {
        IgniteEx n = startGrid(0);

        for (int i = 0; i < 1_000; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(100 * U.KB)]);

        assertFalse(stopObserver().exceed);
    }

    /**
     * Stop {@link #walArchiveObserver}.
     *
     * @return Stopped observer.
     * @throws Exception If failed.
     */
    @Nullable private WalArchiveSizeObserver stopObserver() throws Exception {
        WalArchiveSizeObserver observer = walArchiveObserver;

        if (observer != null && !observer.stop) {
            observer.stop = true;

            observer.join();
        }

        return observer;
    }

    /**
     * Class for tracking not exceeding size of WAL archive.
     */
    private static class WalArchiveSizeObserver extends Thread {
        /** Wal archive directory. */
        final File dir;

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
         * @param max Max size(in bytes) of WAL archive directory.
         */
        public WalArchiveSizeObserver(File dir, long max) {
            this.dir = dir;
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

                    if (log.isInfoEnabled()) {
                        log.info("Excess [max=" + U.humanReadableByteCount(max) +
                            ", curr=" + U.humanReadableByteCount(size) + "files=" + Arrays.toString(files) + ']');
                    }
                }
            }
        }
    }
}
