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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteTxCacheWriteSynchronizationModesMultithreadedTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 4;

    /** */
    private static final int CLIENTS = 2;

    /** */
    private static final int NODES = SRVS + CLIENTS;

    /** */
    private boolean clientMode;

    /** */
    private static final int MULTITHREADED_TEST_KEYS = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(clientMode);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(SRVS);

        clientMode = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        for (int i = 0; i < CLIENTS; i++)
            assertTrue(grid(SRVS + i).configuration().isClientMode());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedPrimarySyncRestartWithoutBackupsAndStores() throws Exception {
        multithreaded(PRIMARY_SYNC, 0, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedPrimarySyncWithoutBackupsAndStores() throws Exception {
        multithreaded(PRIMARY_SYNC, 0, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullSyncWithoutBackupsAndStores() throws Exception {
        multithreaded(FULL_SYNC, 0, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullSyncRestartWithoutBackupsAndStores() throws Exception {
        multithreaded(FULL_SYNC, 0, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullAsyncWithoutBackupsAndStores() throws Exception {
        multithreaded(FULL_ASYNC, 0, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullAsyncRestartWithoutBackupsAndStores() throws Exception {
        multithreaded(FULL_ASYNC, 0, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedPrimarySyncRestartOneBackupsAndWithoutStores() throws Exception {
        multithreaded(PRIMARY_SYNC, 1, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedPrimarySyncOneBackupsAndWithoutStores() throws Exception {
        multithreaded(PRIMARY_SYNC, 1, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullSyncWithoutOneAndWithoutStores() throws Exception {
        multithreaded(FULL_SYNC, 1, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullSyncRestartOneBackupsAndWithoutStores() throws Exception {
        multithreaded(FULL_SYNC, 1, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullAsyncWithoutOneAndWithoutStores() throws Exception {
        multithreaded(FULL_ASYNC, 1, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullAsyncRestartOneBackupsAndWithoutStores() throws Exception {
        multithreaded(FULL_ASYNC, 1, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedPrimarySyncRestartOneBackupsAndStores() throws Exception {
        multithreaded(PRIMARY_SYNC, 1, true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedPrimarySyncOneBackupsAndStores() throws Exception {
        multithreaded(PRIMARY_SYNC, 1, true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullSyncWithoutOneAndStores() throws Exception {
        multithreaded(FULL_SYNC, 1, true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullSyncRestartOneBackupsAndStores() throws Exception {
        multithreaded(FULL_SYNC, 1, true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullAsyncWithoutOneAndStores() throws Exception {
        multithreaded(FULL_ASYNC, 1, true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullAsyncRestartOneBackupsAndStores() throws Exception {
        multithreaded(FULL_ASYNC, 1, true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedPrimarySyncRestartTwoBackupsAndWithoutStores() throws Exception {
        multithreaded(PRIMARY_SYNC, 2, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedPrimarySyncTwoBackupsAndWithoutStores() throws Exception {
        multithreaded(PRIMARY_SYNC, 2, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullSyncWithoutTwoAndWithoutStores() throws Exception {
        multithreaded(FULL_SYNC, 2, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullSyncRestartTwoBackupsAndWithoutStores() throws Exception {
        multithreaded(FULL_SYNC, 2, false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullAsyncWithoutTwoAndWithoutStores() throws Exception {
        multithreaded(FULL_ASYNC, 2, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedFullAsyncRestartTwoBackupsAndWithoutStores() throws Exception {
        multithreaded(FULL_ASYNC, 2, false, false, true);
    }

    /**
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param store If {@code true} sets store in cache configuration.
     * @param nearCache If {@code true} creates near cache on one of client nodes.
     * @param restart If {@code true} restarts one node during test.
     * @throws Exception If failed.
     */
    private void multithreaded(CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean store,
        boolean nearCache,
        boolean restart) throws Exception {

        if (MvccFeatureChecker.forcedMvcc()) {
            if (store && !MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.CACHE_STORE))
                return;

            if (nearCache && !MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE))
                return;
        }

        final Ignite ignite = ignite(0);

        createCache(ignite, cacheConfiguration(DEFAULT_CACHE_NAME, syncMode, backups, store), nearCache);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> restartFut = null;

        try {
            if (restart) {
                restartFut = GridTestUtils.runAsync(() -> {
                    while (!stop.get()) {
                        startGrid(NODES);

                        U.sleep(100);

                        stopGrid(NODES);
                    }
                    return null;
                }, "restart-thread");
            }

            commitMultithreaded((ignite14, cache) -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                Integer key = rnd.nextInt(MULTITHREADED_TEST_KEYS);

                while (true) {
                    try {
                        cache.put(key, rnd.nextInt());

                        break;
                    }
                    catch (CacheException e) {
                        MvccFeatureChecker.assertMvccWriteConflict(e);
                    }
                }
            });

            commitMultithreaded((ignite13, cache) -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                Map<Integer, Integer> map = new TreeMap<>();

                for (int i = 0; i < 100; i++) {
                    Integer key = rnd.nextInt(MULTITHREADED_TEST_KEYS);

                    map.put(key, rnd.nextInt());
                }

                while (true) {
                    try {
                        cache.putAll(map);

                        break;
                    }
                    catch (CacheException e) {
                        if (X.hasCause(e, TransactionRollbackException.class))
                            return;

                        MvccFeatureChecker.assertMvccWriteConflict(e);
                    }
                }
            });

            commitMultithreaded((ignite12, cache) -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                Map<Integer, Integer> map = new TreeMap<>();

                for (int i = 0; i < 100; i++) {
                    Integer key = rnd.nextInt(MULTITHREADED_TEST_KEYS);

                    map.put(key, rnd.nextInt());
                }

                try {
                    try (Transaction tx = ignite12.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        for (Map.Entry<Integer, Integer> e : map.entrySet())
                            cache.put(e.getKey(), e.getValue());

                        tx.commit();
                    }
                }
                catch (CacheException | IgniteException ignored) {
                    // No-op.
                }
            });

            if (!MvccFeatureChecker.forcedMvcc()) {
                commitMultithreaded((ignite1, cache) -> {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Map<Integer, Integer> map = new LinkedHashMap<>();

                    for (int i = 0; i < 10; i++) {
                        Integer key = rnd.nextInt(MULTITHREADED_TEST_KEYS);

                        map.put(key, rnd.nextInt());
                    }

                    while (true) {
                        try (Transaction tx = ignite1.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                            for (Map.Entry<Integer, Integer> e : map.entrySet())
                                cache.put(e.getKey(), e.getValue());

                            tx.commit();

                            break;
                        }
                        catch (TransactionOptimisticException ignored) {
                            // Retry.
                        }
                        catch (CacheException | IgniteException ignored) {
                            break;
                        }
                    }
                });
            }
        }
        finally {
            stop.set(true);

            if (restartFut != null)
                restartFut.get();
        }
    }

    /**
     * @param c Test iteration closure.
     * @throws Exception If failed.
     */
    private void commitMultithreaded(
        final IgniteBiInClosure<Ignite, IgniteCache<Integer, Integer>> c) throws Exception {
        final long stopTime = System.currentTimeMillis() + GridTestUtils.SF.applyLB(10_000, 3_000);

        GridTestUtils.runMultiThreaded(idx -> {
            int nodeIdx = idx % NODES;

            Thread.currentThread().setName("tx-thread-" + nodeIdx);

            Ignite ignite = ignite(nodeIdx);

            IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

            while (System.currentTimeMillis() < stopTime)
                c.apply(ignite, cache);
        }, NODES * 3, "tx-thread");

        final IgniteCache<Integer, Integer> cache = ignite(0).cache(DEFAULT_CACHE_NAME);

        for (int key = 0; key < MULTITHREADED_TEST_KEYS; key++) {
            final Integer key0 = key;

            boolean wait = GridTestUtils.waitForCondition(() -> {
                final Integer val = cache.get(key0);

                for (int i = 1; i < NODES; i++) {
                    IgniteCache<Integer, Integer> cache1 = ignite(i).cache(DEFAULT_CACHE_NAME);

                    if (!Objects.equals(val, cache1.get(key0)))
                        return false;
                }
                return true;
            }, 5000);

            assertTrue(wait);
        }
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     * @param nearCache If {@code true} creates near cache on one of client nodes.
     * @return Created cache.
     */
    private <K, V> IgniteCache<K, V> createCache(Ignite ignite, CacheConfiguration<K, V> ccfg,
        boolean nearCache) {
        IgniteCache<K, V> cache = ignite.createCache(ccfg);

        if (nearCache)
            ignite(NODES - 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

        return cache;
    }

    /**
     * @param name Cache name.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param store If {@code true} configures cache store.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(@NotNull String name,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean store) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setName(name);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setBackups(backups);

        if (store) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
        }

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }
}
