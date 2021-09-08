/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.ReconciliationType;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.verify.ReconciliationType.DATA_CONSISTENCY;
import static org.apache.ignite.internal.processors.cache.verify.ReconciliationType.CACHE_SIZE_CONSISTENCY;

/**
 * Tests partition reconciliation of sizes with in-memory caches.
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationFixPartitionSizesStressTest extends PartitionReconciliationAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public static int nodesCnt;

    /** */
    @Parameterized.Parameter(1)
    public static int startKey;

    /** */
    @Parameterized.Parameter(2)
    public static int endKey;

    /** */
    @Parameterized.Parameter(3)
    public static CacheAtomicityMode cacheAtomicityMode;

    /** */
    @Parameterized.Parameter(4)
    public static CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(5)
    public static int backupCnt;

    /** */
    @Parameterized.Parameter(6)
    public static int partCnt;

    /** */
    @Parameterized.Parameter(7)
    public static String cacheGrp;

    /** */
    @Parameterized.Parameter(8)
    public static int reconBatchSize;

    /** */
    @Parameterized.Parameter(9)
    public static int reconParallelism;

    /** */
    @Parameterized.Parameter(10)
    public static int loadThreadsCnt;

    /** */
    @Parameterized.Parameter(11)
    public static boolean cacheClearOp;

    public static CacheWriteSynchronizationMode cacheWriteSynchronizationMode;

    /** */
    static Random rnd = new Random();

    /** */
    static AtomicReference<ReconciliationResult> reconResult = new AtomicReference<>();

    /** Crd server node. */
    protected IgniteEx ig;

    /** Client. */
    protected IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        DataStorageConfiguration storageConfiguration = new DataStorageConfiguration();
        storageConfiguration.setPageSize(1024);

        cfg.setDataStorageConfiguration(storageConfiguration);

        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        cacheWriteSynchronizationMode = /*rnd.nextBoolean() ? *//*CacheWriteSynchronizationMode.FULL_SYNC :*/ CacheWriteSynchronizationMode.PRIMARY_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    private CacheConfiguration getCacheConfig(String name, CacheAtomicityMode cacheAtomicityMode, CacheMode cacheMode, int backupCount, int partCount, String cacheGroup) {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(name);
        if (cacheGroup != null)
            ccfg.setGroupName(cacheGroup);
        ccfg.setWriteSynchronizationMode(cacheWriteSynchronizationMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, partCount));
        ccfg.setBackups(backupCount);
        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setCacheMode(cacheMode);

        return ccfg;
    }

    /**
     *
     */
    @Parameterized.Parameters(name = "nodesCnt = {0}, startKey = {1}, endKey = {2}, cacheAtomicityMode = {3}, cacheMode = {4}, " +
        "backupCount = {5}, partCount = {6}, cacheGroup = {7}, batchSize = {8}, reconParallelism = {9}, loadThreads = {10}, cacheClear = {11}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[] {1, 0, 1000, ATOMIC, PARTITIONED, 0, 1, null, 100, 1, 8, false});

        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 2, 10, "testCacheGroup1", 100, 1, 8, false});

        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 32, null, 100, 1, 8, false});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, null, 100, 1, 8, false});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 1, null, 10, 3, 8, false});

        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 1, 12, null, 10, 3, 8, false});

        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 14, "testCacheGroup1", 10, 3, 8, false});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 17, null, 10, 3, 8, false});

        params.add(new Object[] {1, 0, 3000, ATOMIC,PARTITIONED, 0, 1, null, 1, 10, 8, false});

        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 2, 10, null, 1, 10, 8, false});

        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 12, null, 1, 10, 8, false});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, "testCacheGroup1", 1, 10, 8, false});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 1, null, 1, 10, 8, false});

        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 2, 10, null, 1, 10, 8, false});

        params.add(new Object[] {4, 0, 1000, TRANSACTIONAL, PARTITIONED, 2, 12, null, 1, 10, 8, false});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, "testCacheGroup1", 1, 10, 8, false});

        return params;
    }

    /**
     * <ul>
     * <li>Start nodes.</li>
     * <li>Start one or two caches.</li>
     * <li>Load some data.</li>
     * <li>Break cache sizes.</li>
     * <li>Start load threads.</li>
     * <li>Do size reconciliation.</li>
     * <li>Stop load threads.</li>
     * <li>Check size of primary/backup partitions in cluster.</li>
     * </ul>
     */
    @Test
    public void test() throws Exception {
        Set<ReconciliationType> reconciliationTypes = new HashSet<>();

        reconciliationTypes.add(CACHE_SIZE_CONSISTENCY);

        if (rnd.nextBoolean())
            reconciliationTypes.add(DATA_CONSISTENCY);

        log.info(">>> Reconciliation types: " + reconciliationTypes);
        log.info(">>> CacheWriteSynchronizationMode: " + cacheWriteSynchronizationMode);

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        ig.cluster().active(true);

        List<IgniteCache<Object, Object>> caches = new ArrayList<>();

        caches.add(client.createCache(
            getCacheConfig(DEFAULT_CACHE_NAME + 0, cacheAtomicityMode, cacheMode, backupCnt, partCnt, cacheGrp)
        ));

        if (rnd.nextBoolean()) {
            caches.add(client.createCache(
                getCacheConfig(DEFAULT_CACHE_NAME + 1, cacheAtomicityMode, cacheMode, backupCnt, partCnt, cacheGrp)
            ));
        }

        log.info(">>> Cache count: " + caches.size());

        Set<String> cacheNames = caches.stream().map(IgniteCache::getName).collect(Collectors.toSet());

        for (long i = startKey; i < endKey; i++) {
            i += 1;
            if (i < endKey) {
                for (IgniteCache<Object, Object> cache : caches)
                    cache.put(i, i);
            }
        }

        List<Integer> startSizes = new ArrayList<>();

        for (IgniteCache<Object, Object> cache : caches)
            startSizes.add(cache.size());

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

//        breakCacheSizes(grids, cacheNames);
//
//        for (int i = 0; i < caches.size(); i++)
//            assertFalse(caches.get(i).size() == startSizes.get(i));

//        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
//        builder.repair(true);
//        builder.parallelism(reconParallelism);
//        builder.caches(cacheNames);
//        builder.batchSize(reconBatchSize);
//        builder.reconTypes(new HashSet(reconciliationTypes));
//        builder.repairAlg(RepairAlgorithm.PRIMARY);

        reconResult = new AtomicReference<>();

        List<IgniteInternalFuture> loadFuts = new ArrayList<>();

        for (int i = 0; i < loadThreadsCnt; i++)
            caches.forEach(cache -> loadFuts.add(startAsyncLoad0(reconResult, client, cache, startKey, endKey, cacheClearOp)));

        GridTestUtils.runMultiThreadedAsync(() -> {
//            reconResult.set(partitionReconciliation(client, builder));
            doSleep(20_000);
            reconResult.set(new ReconciliationResult());
        }, 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> reconResult.get() != null, 120_000);

//        List<String> errors = reconResult.get().errors();

//        assertTrue(errors.isEmpty());

        for (IgniteInternalFuture fut : loadFuts)
            fut.get();

        awaitPartitionMapExchange();
        doSleep(10000);
        cacheNames.forEach(cacheName -> assertPartitionsSame(idleVerify(grid(0), cacheName)));

        for (long i = startKey; i < endKey; i++) {
            for (IgniteCache<Object, Object> cache : caches)
                cache.put(i, i);
        }

        awaitPartitionMapExchange();
        doSleep(10000);
        cacheNames.forEach(cacheName -> assertPartitionsSame(idleVerify(grid(0), cacheName)));

        long allKeysCountForCacheGroup;
        long allKeysCountForCache;

        for (String cacheName : cacheNames) {
            allKeysCountForCacheGroup = 0;
            allKeysCountForCache = 0;

            for (int i = 0; i < nodesCnt; i++) {
                long i0 = getFullPartitionsSizeForCacheGroup(grid(i), cacheName);
                allKeysCountForCacheGroup += i0;

                long i1 = getPartitionsSizeForCache(grid(i), cacheName);
                allKeysCountForCache += i1;
            }

            assertEquals(endKey, client.cache(cacheName).size());

            if (cacheMode == REPLICATED) {
                assertEquals((long)endKey * nodesCnt * (cacheGrp == null ? 1 : caches.size()), allKeysCountForCacheGroup);
                assertEquals((long)endKey * nodesCnt, allKeysCountForCache);
            }
            else {
                assertEquals((long)endKey * (1 + backupCnt) * (cacheGrp == null ? 1 : caches.size()), allKeysCountForCacheGroup);
                assertEquals((long)endKey * (1 + backupCnt), allKeysCountForCache);
            }
        }
    }
}
