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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
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
            .setAffinity(new RendezvousAffinityFunction(false, 2))
            .setBackups(0)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 350)));

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
        IgniteEx srv0 = startGrids(2);

        srv0.cluster().active(true);

        awaitPartitionMapExchange();

        srv0.getOrCreateCache(DEFAULT_CACHE_NAME);

        GridDhtPartitionTopologyImpl top =
            (GridDhtPartitionTopologyImpl)srv0.cachex(DEFAULT_CACHE_NAME).context().topology();

        AtomicReference dbRef = new AtomicReference();

        AtomicBoolean cpWriteLocked = new AtomicBoolean(false);

        AtomicInteger partId = new AtomicInteger();

        CountDownLatch ttlLatch = new CountDownLatch(2);

        top.partitionFactory((ctx, grp, id) -> {
            partId.set(id);
            return new GridDhtLocalPartition(ctx, grp, id, false) {
                @Override public IgniteCacheOffheapManager.CacheDataStore dataStore() {
                    Thread t = Thread.currentThread();
                    String tName = t.getName();

                    if (tName == null || !tName.contains("updater"))
                        return super.dataStore();

                    boolean unswapFoundInST = false;

                    for (StackTraceElement e : t.getStackTrace()) {
                        if (e.getMethodName().contains("unswap")) {
                            unswapFoundInST = true;

                            break;
                        }
                    }

                    if (!unswapFoundInST)
                        return super.dataStore();

                    while (!cpWriteLocked.get()) {
                        try {
                            Thread.sleep(10);
                        }
                        catch (InterruptedException e) {
                            // No-op.
                        }
                    }

                    System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] unswap after cp writeLock");

                    ttlLatch.countDown();

                    return super.dataStore();
                }

                @Override public boolean reserve() {
                    Thread t = Thread.currentThread();
                    String tName = t.getName();

                    if (tName == null || !tName.contains("ttl-cleanup-worker"))
                        return super.reserve();

                    boolean purgeExpiredFoundInST = false;

                    for (StackTraceElement e : t.getStackTrace()) {
                        if (e.getMethodName().contains("purgeExpiredInternal")) {
                            purgeExpiredFoundInST = true;

                            break;
                        }
                    }

                    if (!purgeExpiredFoundInST)
                        return super.reserve();

                    GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)dbRef.get();

                    System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] reserve called " + db.checkpointLockIsHeldByThread());

                    ttlLatch.countDown();

                    try {
                        ttlLatch.await();

                        System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] ttlLatch released");
                    }
                    catch (InterruptedException e) {
                        // No-op.
                    }

                    return super.reserve();
                }
            };
        });

        stopGrid(1);

        srv0.cluster().setBaselineTopology(srv0.cluster().topologyVersion());

        Thread.sleep(500);

        IgniteCache<Object, Object> cache = srv0.getOrCreateCache(DEFAULT_CACHE_NAME);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)srv0.context().cache().context().database();

        dbRef.set(db);

        AtomicBoolean timeoutReached = new AtomicBoolean(false);

        long timeout = 10_000;

        int key = 0;

        while (true) {
            if (srv0.affinity(DEFAULT_CACHE_NAME).partition(key) != partId.get())
                key++;
            else break;
        }

        cache.put(key, 1);

        int finalKey = key;

        IgniteInternalFuture updateFut = GridTestUtils.runAsync(() -> {
            int i = 10;

            while (!timeoutReached.get()) {
                cache.put(finalKey, i++);

                try {
                    Thread.sleep(300);
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }
        }, "updater-thread");

        GridTestUtils.runAsync(() -> {
            while (ttlLatch.getCount() != 1) {
                try {
                    Thread.sleep(20);
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }

            try {
                cpWriteLocked.set(true);

                System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] cp write locked");

                db.checkpointLock.writeLock().lockInterruptibly();

                ttlLatch.await();
            }
            catch (InterruptedException e) {
                // No-op.
            }
            finally {
                db.checkpointLock.writeLock().unlock();
            }

            System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] cp write lock holder exiting...");
        }, "cp-write-lock-holder");

        GridTestUtils.runAsync(() -> {
            long start = System.currentTimeMillis();

            while(System.currentTimeMillis() - start < timeout) {
                doSleep(1_000);
            }

            timeoutReached.set(true);
        });

        try {
//            loadFut.get(timeout * 2);
            updateFut.get(timeout * 2);
//            lockUnlockCPWLFut.get(timeout * 4);
        }
        catch (IgniteFutureTimeoutCheckedException ignored) {
            fail("Failed to wait for futures for doubled timeout");
        }
    }
}
