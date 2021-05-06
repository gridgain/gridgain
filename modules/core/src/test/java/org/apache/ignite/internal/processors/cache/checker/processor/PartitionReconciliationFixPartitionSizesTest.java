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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
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
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.verify.ReconType.CONSISTENCY;
import static org.apache.ignite.internal.processors.cache.verify.ReconType.SIZES;

/**
 * Tests count of calls the recheck process with different inputs.
 */
public class PartitionReconciliationFixPartitionSizesTest extends PartitionReconciliationAbstractTest {
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

    private CacheConfiguration getCacheConfig(String name, CacheAtomicityMode cacheAtomicityMode, CacheMode cacheMode, int backupCount, int partCount, String cacheGroup) {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(name);
        if (cacheGroup != null)
            ccfg.setGroupName(cacheGroup);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, partCount));
        ccfg.setBackups(backupCount);
        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setCacheMode(cacheMode);

        return ccfg;
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testTwoReconciliationInRow() throws Exception {
        int nodesCnt = 3;

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        ig.cluster().active(true);

        int startKey = 0;
        int endKey = 2000;

        IgniteCache<Object, Object> cache0 = client.createCache(DEFAULT_CACHE_NAME);

        //first

        for (long i = startKey; i < endKey; i++) {
            i += 1;
            if (i < endKey) {
                cache0.put(i, i);
                System.out.print(i + " ");
            }
        }

//        System.out.println();

        int startSize0 = cache0.size();

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++) {
            grids.add(grid(i));
        }

        breakCacheSizes(grids, Arrays.asList(DEFAULT_CACHE_NAME));

        assertFalse(cache0.size() == startSize0);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(DEFAULT_CACHE_NAME);
//        objects.add("qqq");
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));

        reconResult = new AtomicReference<>();

        List<IgniteInternalFuture> loadFuts = new ArrayList<>();

        for (int i = 0; i < 4; i++)
            loadFuts.add(startAsyncLoad0(reconResult, cache0, startKey, endKey, false));

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
            cache0.put(i, i);

        long allKeysCountForCacheGroup;
        long allKeysCountForCache;

        for (String cacheName : cacheNames) {
            allKeysCountForCacheGroup = 0;
            allKeysCountForCache = 0;

            for (int i = 0; i < nodesCnt; i++) {
//            System.out.println("jhsdaidfgh " + i);

                long i0 = getFullPartitionsSizeForCacheGroup(grid(i), cacheName);
//            System.out.println("asdhubd " + i0);
                allKeysCountForCacheGroup += i0;

                long i1 = getPartitionsSizeForCache(grid(i), cacheName);
//            System.out.println("kjkhdfdf " + i1);
                allKeysCountForCache += i1;
            }

            assertEquals(endKey, client.cache(cacheName).size());

            assertEquals((long)endKey, allKeysCountForCacheGroup);
            assertEquals((long)endKey, allKeysCountForCache);

        }

        cache0.clear();

        //second

        for (long i = startKey; i < endKey; i++) {
            i += 1;
            if (i < endKey) {
                cache0.put(i, i);
                System.out.print(i + " ");
            }
        }

//        System.out.println();

        startSize0 = cache0.size();

        grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++) {
            grids.add(grid(i));
        }

        breakCacheSizes(grids, Arrays.asList(DEFAULT_CACHE_NAME));

        assertFalse(cache0.size() == startSize0);

        reconResult = new AtomicReference<>();

        loadFuts = new ArrayList<>();

        for (int i = 0; i < 4; i++)
            loadFuts.add(startAsyncLoad0(reconResult, cache0, startKey, endKey, false));

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
            cache0.put(i, i);

        for (String cacheName : cacheNames) {
            allKeysCountForCacheGroup = 0;
            allKeysCountForCache = 0;

            for (int i = 0; i < nodesCnt; i++) {
//            System.out.println("jhsdaidfgh " + i);

                long i0 = getFullPartitionsSizeForCacheGroup(grid(i), cacheName);
//            System.out.println("asdhubd " + i0);
                allKeysCountForCacheGroup += i0;

                long i1 = getPartitionsSizeForCache(grid(i), cacheName);
//            System.out.println("kjkhdfdf " + i1);
                allKeysCountForCache += i1;
            }

            assertEquals(endKey, client.cache(cacheName).size());

            assertEquals((long)endKey, allKeysCountForCacheGroup);
            assertEquals((long)endKey, allKeysCountForCache);

        }

    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testRepairPartOfCachesReconciliationInRow() throws Exception {
        CacheConfiguration ccfg0 = new CacheConfiguration("cache0");
        CacheConfiguration ccfg1 = new CacheConfiguration("cache1");
        CacheConfiguration ccfg2 = new CacheConfiguration("cache2_group0").setGroupName("group0");
        CacheConfiguration ccfg3 = new CacheConfiguration("cache3_group0").setGroupName("group0");
        CacheConfiguration ccfg4 = new CacheConfiguration("cache4_group1").setGroupName("group1");
        CacheConfiguration ccfg5 = new CacheConfiguration("cache5_group1").setGroupName("group1");

        int nodesCnt = 3;

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        IgniteCache cache0 = client.createCache(ccfg0);
        IgniteCache cache1 = client.createCache(ccfg1);
        IgniteCache cache2_group0 = client.createCache(ccfg2);
        IgniteCache cache3_group0 = client.createCache(ccfg3);
        IgniteCache cache4_group1 = client.createCache(ccfg4);
        IgniteCache cache5_group1 = client.createCache(ccfg5);

        List<IgniteCache> caches = new ArrayList<>();

        caches.add(cache0);
        caches.add(cache1);
        caches.add(cache2_group0);
        caches.add(cache3_group0);
        caches.add(cache4_group1);
        caches.add(cache5_group1);

        caches.stream().forEach(cache -> {
            for (int i = 0; i < 100; i++)
                cache.put(i, i);
        });

        cache0.put(1, 1);
        cache1.put(1, 1);
        cache2_group0.put(1, 1);
        cache3_group0.put(1, 1);
        cache4_group1.put(1, 1);
        cache5_group1.put(1, 1);

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        breakCacheSizes(grids, Arrays.asList(
            cache0.getName(),
            cache1.getName(),
            cache2_group0.getName(),
            cache3_group0.getName(),
            cache4_group1.getName(),
            cache5_group1.getName())
        );

        assertFalse(cache1.size() == 100);
        assertFalse(cache3_group0.size() == 100);
        assertFalse(cache0.size() == 100);
        assertFalse(cache2_group0.size() == 100);
        assertFalse(cache4_group1.size() == 100);
        assertFalse(cache5_group1.size() == 100);


        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache0.getName());
        cacheNames.add(cache2_group0.getName());
        cacheNames.add(cache4_group1.getName());
        cacheNames.add(cache5_group1.getName());
//        objects.add("qqq");
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));

        partitionReconciliation(client, builder);

        assertFalse(cache1.size() == 100);
        assertFalse(cache3_group0.size() == 100);
        assertTrue(cache0.size() == 100);
        assertTrue(cache2_group0.size() == 100);
        assertTrue(cache4_group1.size() == 100);
        assertTrue(cache5_group1.size() == 100);

    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testRepairEmptyCacheSize() throws Exception {
        testRepairCacheSize(0);
    }

    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testRepairCacheSizeWithOneEntry() throws Exception {
        testRepairCacheSize(1);
    }

    private void testRepairCacheSize(int entryCount) throws Exception {
        int nodesCnt = 3;

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        IgniteCache cache = client.createCache("cache0");

        for (int i = 0; i < 2000; i++)
            cache.put(i, i);

        cache.clear();

        for (int i = 0; i < entryCount; i++)
            cache.put(i, i);

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        breakCacheSizes(grids, Arrays.asList(cache.getName()));

        assertFalse(cache.size() == entryCount);


        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache.getName());
//        objects.add("qqq");
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));

        partitionReconciliation(client, builder);

        assertEquals(entryCount, cache.size());
    }

    @Test
    public void testRepairEmptyCacheSizeWithoutPreloading() throws Exception {
        int nodesCnt = 4;

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        IgniteCache cache = client.createCache("cache0");

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));


        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache.getName());
//        objects.add("qqq");
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));

        partitionReconciliation(client, builder);

        assertEquals(0, cache.size());
    }

    @Test
    public void testCacheSizeNotRepaired() throws Exception {//щее не сделан
        int nodesCnt = 3;

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        IgniteCache cache = client.createCache("cache0");

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        breakCacheSizes(grids, Arrays.asList(cache.getName()));

        long brokenCacheSize = cache.size();

        assertFalse(cache.size() == 1000);


        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache.getName());
//        objects.add("qqq");
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY)));

        partitionReconciliation(client, builder);

        assertEquals(brokenCacheSize, cache.size());
    }

}