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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteOutOfMemoryPropagationTest extends GridCommonAbstractTest {
    /** */
    public static final int NODES = 3;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private CacheMode mode;

    /** */
    private int backupsCnt;

    /** */
    private CacheWriteSynchronizationMode writeSyncMode;

    /** */
    private long maxDataRegionSize = 10L * 1024 * 1024 + 1;

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }

    /** */
    @Test
    public void testPutOOMPropagation() throws Exception {
        testOOMPropagation(false);
    }

    /** */
    @Test
    public void testStreamerOOMPropagation() throws Exception {
        testOOMPropagation(true);
    }

    /** */
    private void testOOMPropagation(boolean useStreamer) throws Exception {
        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            for (CacheMode cacheMode : CacheMode.values()) {
                for (CacheWriteSynchronizationMode writeSyncMode : CacheWriteSynchronizationMode.values()) {
                    for (int backupsCnt = 0; backupsCnt <= 1; backupsCnt++) {
                        if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC
                            || cacheMode == CacheMode.REPLICATED)
                            continue;

                        if (atomicityMode == CacheAtomicityMode.TRANSACTIONAL && !useStreamer) {
                            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                                    checkOOMPropagation(
                                        false,
                                        CacheAtomicityMode.TRANSACTIONAL,
                                        cacheMode,
                                        writeSyncMode,
                                        backupsCnt,
                                        concurrency,
                                        isolation);
                                }
                            }
                        }

                        checkOOMPropagation(useStreamer, atomicityMode, cacheMode, writeSyncMode, backupsCnt);
                    }
                }
            }
        }
    }

    /** */
    private void checkOOMPropagation(boolean useStreamer, CacheAtomicityMode atomicityMode, CacheMode cacheMode,
        CacheWriteSynchronizationMode writeSyncMode, int backupsCnt) throws Exception {
        checkOOMPropagation(useStreamer, atomicityMode, cacheMode, writeSyncMode, backupsCnt, null, null);
    }

    /** */
    private void checkOOMPropagation(boolean useStreamer, CacheAtomicityMode atomicityMode, CacheMode cacheMode,
        CacheWriteSynchronizationMode writeSyncMode, int backupsCnt,
        TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        Throwable t = null;

        System.out.println("Checking conf: CacheAtomicityMode." + atomicityMode +
            " CacheMode." + cacheMode + " CacheWriteSynchronizationMode." + writeSyncMode + " backupsCount = " + backupsCnt
            + " TransactionConcurrency." + concurrency + " TransactionIsolation." + isolation);

        initGrid(atomicityMode, cacheMode, writeSyncMode, backupsCnt);
        try {
            forceOOM(useStreamer, concurrency, isolation);
        }
        catch (Throwable t0) {
            t = t0;

            t.printStackTrace(System.out);

            assertTrue(X.hasCause(t, IgniteOutOfMemoryException.class, ClusterTopologyException.class));
        }
        finally {
            assertNotNull(t);

            stopAllGrids();
        }
    }

    /**
     * Ignite grid of 3 server nodes with passed parameters.
     *
     * @param atomicityMode atomicity mode
     * @param mode cache mode
     * @param writeSyncMode cache write synchronization mode
     * @param backupsCnt backups count
     * @throws Exception If failed.
     */
    private void initGrid(CacheAtomicityMode atomicityMode, CacheMode mode,
        CacheWriteSynchronizationMode writeSyncMode, int backupsCnt) throws Exception {

        this.atomicityMode = atomicityMode;
        this.mode = mode;
        this.backupsCnt = backupsCnt;
        this.writeSyncMode = writeSyncMode;
        this.maxDataRegionSize = 10L * 1024 * 1024;

        Ignition.setClientMode(false);

        for (int i = 0; i < NODES; i++)
            startGrid(i);

        Ignition.setClientMode(true);

        client = startGrid(NODES + 1);

        // it is required to start first node in test jvm, but we can not start client node,
        // because client will fail to connect and test will fail too.
        // as workaround start first server node in test jvm and then stop it.
        stopGrid(0);
    }

    /**
     * @param useStreamer Use streamer.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     */
    public void forceOOM(boolean useStreamer, TransactionConcurrency concurrency, TransactionIsolation isolation) {
        final IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        IgniteDataStreamer<String, String> streamer = client.dataStreamer(DEFAULT_CACHE_NAME);

        Map<String, String> map = new HashMap<>();

        Transaction tx = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            map.put("k" + i, "v" + i);

            if (map.size() > 1_000) {
                if (concurrency != null && isolation != null)
                    tx = client.transactions().txStart(concurrency, isolation);

                if (useStreamer)
                    streamer.addData(map);
                else
                    cache.putAll(map);

                map.clear();

                if (tx != null) {
                    tx.commit();
                    tx.close();
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return !(Ignition.isClientMode() || igniteInstanceName.endsWith("0"));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        memCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setMaxSize(maxDataRegionSize)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU)
        );

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Object, Object> baseCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        baseCfg.setAtomicityMode(this.atomicityMode);
        baseCfg.setCacheMode(this.mode);
        baseCfg.setBackups(this.backupsCnt);
        baseCfg.setWriteSynchronizationMode(this.writeSyncMode);

        cfg.setMetricsLogFrequency(1_000);
        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler(false, 0));
        cfg.setCacheConfiguration(baseCfg);

        return cfg;
    }

    /**
     * Tests how eviction behaves when {@link DataRegionConfiguration#getEmptyPagesPoolSize()} is not enough for large
     * values.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteOOM() throws Exception {
        atomicityMode = CacheAtomicityMode.ATOMIC;
        mode = CacheMode.PARTITIONED;
        writeSyncMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        backupsCnt = 0;
        maxDataRegionSize = 1024L * 1024 * 1024;

        IgniteEx sn = startGrids(1);

        //IgniteEx cn = startClientGrid(2);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = sn.cache(DEFAULT_CACHE_NAME);

        List<IgniteInternalFuture<Object>> futures = new ArrayList<>();

        AtomicInteger k = new AtomicInteger(102);

        Function<ThreadLocalRandom, byte[]> function = random -> new byte[/*random.nextInt(3) * k.get()*/500 * 1024];

        AtomicInteger exceptionsCnt = new AtomicInteger();

        AtomicInteger successfulOpCountAfterException = new AtomicInteger();

        final int exceptionsLimit = 1;

        for (int i = 0; i < 3; i++) {
            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                while (true) {
                    if (exceptionsCnt.get() >= exceptionsLimit)
                        break;

                    try {
                        cache.put(random.nextInt(), function.apply(random));

                        if (exceptionsCnt.get() > 0)
                            successfulOpCountAfterException.incrementAndGet();
                    }
                    catch (CachePartialUpdateException e) {
                        exceptionsCnt.incrementAndGet();

                        e.printStackTrace();

                        break;
                    }
                }
            });

            futures.add(fut);
        }

        for (int i = 0; i < 2; i++) {
            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                while (true) {
                    cache.get(random.nextInt());

                    if (exceptionsCnt.get() >= exceptionsLimit)
                        break;
                }
            });

            futures.add(fut);
        }

        for (int i = 0; i < 2; i++) {
            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                while (true) {
                    if (exceptionsCnt.get() >= exceptionsLimit)
                        break;

                    try {
                        cache.replace(random.nextInt(), function.apply(random));

                        if (exceptionsCnt.get() > 0)
                            successfulOpCountAfterException.incrementAndGet();
                    }
                    catch (CachePartialUpdateException e) {
                        exceptionsCnt.incrementAndGet();

                        e.printStackTrace();

                        break;
                    }
                }
            });

            futures.add(fut);
        }

        for (IgniteInternalFuture<Object> future : futures) {
            future.get();
        }

        assertTrue(exceptionsCnt.get() >= exceptionsLimit);
        assertTrue(successfulOpCountAfterException.get() > 0);

        log.info("Successful operations after exception: " + successfulOpCountAfterException.get());
    }
}
