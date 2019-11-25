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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Class contains various tests related to cache entry expiration feature.
 */
public class IgnitePdsCacheEntriesExpirationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                .setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(60_000);

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(1)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 250)));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Verifies scenario of a deadlock between thread, modifying a cache entry (acquires cp read lock and entry lock),
     * ttl thread, expiring the entry (acquires cp read lock and entry lock)
     * and checkpoint thread (acquires cp write lock).
     *
     * Checkpoint thread in not used but emulated by the test to avoid test hang (interruptible API for acquiring
     * write lock is used).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeadlockBetweenCachePutAndEntryExpiration() throws Exception {
        IgniteEx srv0 = startGrid(0);

        srv0.cluster().active(true);

        awaitPartitionMapExchange();

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)srv0.context().cache().context().database();

        IgniteCache<Object, Object> cache = srv0.getOrCreateCache(DEFAULT_CACHE_NAME);

        AtomicBoolean timeoutReached = new AtomicBoolean(false);

        long timeout = 10_000;

        AtomicInteger keysBatch = new AtomicInteger(0);
        int keysCnt = 10_000;

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            while (!timeoutReached.get()) {
                Map<Integer, byte[]> keys = new TreeMap<>();

                int nextBatch = keysBatch.getAndIncrement();

                for (int i = nextBatch * keysCnt; i < (nextBatch + 1) * keysCnt; i++)
                    keys.put(i, new byte[546]);

                cache.putAll(keys);

                doSleep(250);
            }
        }, "loader-thread");

        IgniteInternalFuture updateFut = GridTestUtils.runAsync(() -> {
            while (!timeoutReached.get()) {
                int maxKey = (keysBatch.get() - 1) * keysCnt;

                if (maxKey > 0) {
                    for (int i = 0; i < maxKey; i++) {
                        if (i % 2 == 0 || i % 3 == 0) {
                            byte[] arr = (byte[])cache.get(i);

                            if (arr != null)
                                cache.put(i, new byte[arr.length + 1]);
                        }
                    }
                }

                doSleep(100);
            }
        }, "updater-thread");

        IgniteInternalFuture lockUnlockCPWLFut = GridTestUtils.runAsync(() -> {
            while (!timeoutReached.get()) {
                doSleep(10);

                try {
                    db.checkpointLock.writeLock().lockInterruptibly();
                }
                catch (InterruptedException e) {
                    break;
                }

                try {
                    doSleep(50);
                }
                finally {
                    db.checkpointLock.writeLock().unlock();
                }

            }
        }, "lock-unlock-cp-write-lock-thread");

        GridTestUtils.runAsync(() -> {
            long start = System.currentTimeMillis();

            while(System.currentTimeMillis() - start < timeout) {
                doSleep(1_000);
            }

            timeoutReached.set(true);
        });

        try {
            loadFut.get(timeout * 2);
            updateFut.get(timeout * 2);
            lockUnlockCPWLFut.get(timeout * 2);
        }
        catch (IgniteFutureTimeoutCheckedException ignored) {
            lockUnlockCPWLFut.cancel();

            fail("Failed to wait for futures for doubled timeout");
        }
    }
}
