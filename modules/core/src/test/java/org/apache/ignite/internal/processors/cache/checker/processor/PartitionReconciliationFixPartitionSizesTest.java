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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.verify.ReconType.CONSISTENCY;
import static org.apache.ignite.internal.processors.cache.verify.ReconType.SIZES;

/**
 * Tests count of calls the recheck process with different inputs.
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationFixPartitionSizesTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    @Parameterized.Parameter(0)
    public static int nodesCnt;
    @Parameterized.Parameter(1)
    public static int startKey;
    @Parameterized.Parameter(2)
    public static int endKey;
    @Parameterized.Parameter(3)
    public static CacheAtomicityMode cacheAtomicityMode;
    @Parameterized.Parameter(4)
    public static CacheMode cacheMode;
    @Parameterized.Parameter(5)
    public static int backupCount;
    @Parameterized.Parameter(6)
    public static int partCount;
    @Parameterized.Parameter(7)
    public static String cacheGroup;
    @Parameterized.Parameter(8)
    public static int reconBatchSize;
    @Parameterized.Parameter(9)
    public static int reconParallelism;
    @Parameterized.Parameter(10)
    public static int loadThreads;
    @Parameterized.Parameter(11)
    public static boolean cacheClear;

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
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    private CacheConfiguration getCacheConfig(CacheAtomicityMode cacheAtomicityMode, CacheMode cacheMode, int backupCount, int partCount, String cacheGroup) {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(DEFAULT_CACHE_NAME);
        if (cacheGroup != null)
            ccfg.setGroupName(cacheGroup);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
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

//        params.add(new Object[] {1, 0, 1000,  ATOMIC,        PARTITIONED, 0, 1,  null,              100, 1,  8, false});
//        params.add(new Object[] {3, 0, 3000,  ATOMIC,        PARTITIONED, 0, 10, "testCacheGroup1", 100, 1,  8, false});
//        params.add(new Object[] {4, 0, 3000,  TRANSACTIONAL, PARTITIONED, 2, 12, null,              100, 1,  8, false});
//        params.add(new Object[] {4, 0, 10000, ATOMIC,        REPLICATED,  0, 12, null,              100, 1,  8, false});
//
//        params.add(new Object[] {1, 0, 3000,  ATOMIC,        PARTITIONED, 0, 1,  null,              10,  3,  8, false});
//        params.add(new Object[] {3, 0, 3000,  ATOMIC,        PARTITIONED, 1, 10, null,              10,  3,  8, false});
//        params.add(new Object[] {4, 0, 3000,  TRANSACTIONAL, PARTITIONED, 2, 12, "testCacheGroup1", 10,  3,  8, false});
//        params.add(new Object[] {4, 0, 10000, ATOMIC,        REPLICATED,  0, 12, null,              10,  3,  8, false});
//
//        params.add(new Object[] {1, 0, 3000,  ATOMIC,        PARTITIONED, 0, 1,  null,              1,   10, 8, false});
//        params.add(new Object[] {3, 0, 3000,  ATOMIC,        PARTITIONED, 0, 10, null,              1,   10, 8, false});
//        params.add(new Object[] {4, 0, 3000,  TRANSACTIONAL, PARTITIONED, 2, 12, null,              1,   10, 8, false});
//        params.add(new Object[] {4, 0, 10000, ATOMIC,        REPLICATED,  0, 12, "testCacheGroup1", 1,   10, 8, false});
//
//        params.add(new Object[] {1, 0, 3000,  ATOMIC,        PARTITIONED, 0, 1,  null,              1,   10, 8, true});
//        params.add(new Object[] {3, 0, 3000,  ATOMIC,        PARTITIONED, 0, 10, null,              1,   10, 8, true});
        params.add(new Object[] {4, 0, 1000,  TRANSACTIONAL, PARTITIONED, 2, 12, null,              1,   10, 8, true});
        params.add(new Object[] {4, 0, 10000, ATOMIC,        REPLICATED,  0, 12, "testCacheGroup1", 1,   10, 8, true});
//
//        params.add(new Object[] {2, 0, 100, ATOMIC,        REPLICATED,  0, 1, null, 1,   1, 1, true});//падал на assertEquals(endKey, grid(0).cache(DEFAULT_CACHE_NAME).size()); Важно наличие бекап копий
//        params.add(new Object[] {1, 0, 1000, ATOMIC,        PARTITIONED,  0, 1, null, 1,   1, 8, true});//падал на assertEquals(endKey, grid(0).cache(DEFAULT_CACHE_NAME).size()); Важно наличие бекап копий
//        params.add(new Object[] {2, 0, 100, ATOMIC,        PARTITIONED,  1, 1, null, 1,   1, 1, false});//зависает в org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor.execute Важно наличие бекап копий

        return params;
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void test() throws Exception {
        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        ig.cluster().active(true);

        client.createCache(
            getCacheConfig(cacheAtomicityMode, cacheMode, backupCount, partCount, cacheGroup)
        );

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        for (long i = startKey; i < endKey; i++) {
            i += 1;
            if (i < endKey) {
                cache.put(i, i);
                System.out.print(i + " ");
            }
        }

//        System.out.println();

        int startSize = cache.size();

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++) {
            grids.add(grid(i));
        }

        breakCacheSizes(grids, Arrays.asList(DEFAULT_CACHE_NAME));

        assertFalse(cache.size() == startSize);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(reconParallelism);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> objects = new HashSet<>();
        objects.add(DEFAULT_CACHE_NAME);
//        objects.add("qqq");
        builder.caches(objects);
        builder.batchSize(reconBatchSize);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));

        reconResult = new AtomicReference<>();

        List<IgniteInternalFuture> loadFuts = new ArrayList<>();

        for (int i = 0; i < loadThreads; i++)
            loadFuts.add(startAsyncLoad0(reconResult, cache, startKey, endKey, cacheClear));

        GridTestUtils.runMultiThreadedAsync(() -> {
            reconResult.set(partitionReconciliation(client, builder));
        }, 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> reconResult.get() != null, 120_000);

//        System.out.println("qvsdhntsd partitionReconciliation stop");

        for (IgniteInternalFuture fut : loadFuts)
            fut.get();

//        for (long i = startKey; i < endKey; i++)
//            if (grid(0).cache(DEFAULT_CACHE_NAME).containsKey(i))
//                System.out.println("cache contains key: " + i);

        for (long i = startKey; i < endKey; i++)
                grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        long allKeysCountForCacheGroup = 0;
        long allKeysCountForCache = 0;

        for (int i = 0; i < nodesCnt; i++) {
//            System.out.println("jhsdaidfgh " + i);

            long i0 = getFullPartitionsSizeForCacheGroup(grid(i), DEFAULT_CACHE_NAME);
//            System.out.println("asdhubd " + i0);
            allKeysCountForCacheGroup += i0;

            long i1 = getPartitionsSizeForCache(grid(i), DEFAULT_CACHE_NAME);
//            System.out.println("kjkhdfdf " + i1);
            allKeysCountForCache += i1;
        }

        assertEquals(endKey, grid(0).cache(DEFAULT_CACHE_NAME).size());

        if (cacheMode == REPLICATED) {
            assertEquals((long)endKey * nodesCnt, allKeysCountForCacheGroup);
            assertEquals((long)endKey * nodesCnt, allKeysCountForCache);
        }
        else {
            assertEquals((long)endKey * (1 + backupCount), allKeysCountForCacheGroup);
            assertEquals((long)endKey * (1 + backupCount), allKeysCountForCache);
        }
    }

}