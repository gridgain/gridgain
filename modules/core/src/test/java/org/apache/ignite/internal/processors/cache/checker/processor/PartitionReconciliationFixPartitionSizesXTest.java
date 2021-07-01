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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.ReconType;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.internal.processors.cache.verify.ReconType.CONSISTENCY;
import static org.apache.ignite.internal.processors.cache.verify.ReconType.SIZES;

/**
 * Tests partition reconciliation of sizes.
 */
public class PartitionReconciliationFixPartitionSizesXTest extends PartitionReconciliationFixPartitionSizesTest {
    static AtomicReference<ReconciliationResult> reconResult = new AtomicReference<>();

    /** Crd server node. */
    protected IgniteEx ig;

    /** Client. */
    protected IgniteEx client;
    protected IgniteEx client1;
    protected IgniteEx client2;
    protected IgniteEx client3;

    private Random rnd = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        DataStorageConfiguration storageConfiguration = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setMaxSize(1000 * 1024 * 1024));

        storageConfiguration.setPageSize(/*16384*/1024);

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

    /** Tests that two size reconciliationы in a row work sucessfully. */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testTwoReconciliationInRow() throws Exception {
        int nodesCnt = 1;

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);
        client1 = startClientGrid(nodesCnt + 1);
        client2 = startClientGrid(nodesCnt + 2);
        client3 = startClientGrid(nodesCnt + 3);

        ig.cluster().active(true);

        int startKey = 0;
        int endKey = 4_000;

        IgniteCache<TestKey, Object> cache0 = client.createCache(new CacheConfiguration<TestKey, Object>(DEFAULT_CACHE_NAME).setAffinity(new RendezvousAffinityFunction(false, 1)));
        IgniteCache<TestKey, Object> cache1 = client1.cache(DEFAULT_CACHE_NAME);
        IgniteCache<TestKey, Object> cache2 = client2.cache(DEFAULT_CACHE_NAME);
        IgniteCache<TestKey, Object> cache3 = client3.cache(DEFAULT_CACHE_NAME);

        //first reconciliation

        for (long i = startKey; i < endKey; i++) {
            cache0.put(new TestKey(i), i);
        }

        int startSize0 = cache0.size();

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

//        breakCacheSizes(grids, new HashSet<>(Arrays.asList(DEFAULT_CACHE_NAME)));

//        assertFalse(cache0.size() == startSize0);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(1);
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(DEFAULT_CACHE_NAME);
        builder.caches(cacheNames);
        builder.batchSize(10000);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));
        builder.repairAlg(RepairAlgorithm.PRIMARY);

        reconResult = new AtomicReference<>();

        List<IgniteInternalFuture> loadFuts = new ArrayList<>();

//        for (int i = 0; i < 4; i++)
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 100_000_000;
                        while (reconResult.get() == null) {
                            cache0.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut0 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 200_000_000;
                        while (reconResult.get() == null) {
                            cache0.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut1 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 300_000_000;
                        while (reconResult.get() == null) {
                            cache0.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut2 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 400_000_000;
                        while (reconResult.get() == null) {
                            cache0.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut3 " + i);
            }));


            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 500_000_000;
                        while (reconResult.get() == null) {
                            cache1.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut4 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 600_000_000;
                        while (reconResult.get() == null) {
                            cache1.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut5 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 700_000_000;
                        while (reconResult.get() == null) {
                            cache1.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut6 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 800_000_000;
                        while (reconResult.get() == null) {
                            cache1.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut7 " + i);
            }));


            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 900_000_000;
                        while (reconResult.get() == null) {
                            cache2.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut8 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 1000_000_000;
                        while (reconResult.get() == null) {
                            cache2.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut9 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 1100_000_000;
                        while (reconResult.get() == null) {
                            cache2.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut10 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 1200_000_000;
                        while (reconResult.get() == null) {
                            cache2.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut11 " + i);
            }));


            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 1300_000_000;
                        while (reconResult.get() == null) {
                            cache3.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut12 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 1400_000_000;
                        while (reconResult.get() == null) {
                            cache3.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut13 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 1500_000_000;
                        while (reconResult.get() == null) {
                            cache3.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut14 " + i);
            }));
            loadFuts.add(GridTestUtils.runAsync(() -> {
                int i = 1600_000_000;
                        while (reconResult.get() == null) {
                            cache3.put(new TestKey(++i), i);
                        }
                System.out.println("loadFut15 " + i);
            }));

            //----------------------

        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 100_000_000;
            while (reconResult.get() == null) {
                cache0.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut0 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 200_000_000;
            while (reconResult.get() == null) {
                cache0.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut1 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 300_000_000;
            while (reconResult.get() == null) {
                cache0.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut2 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 400_000_000;
            while (reconResult.get() == null) {
                cache0.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut3 " + i);
        }));


        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 500_000_000;
            while (reconResult.get() == null) {
                cache1.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut4 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 600_000_000;
            while (reconResult.get() == null) {
                cache1.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut5 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 700_000_000;
            while (reconResult.get() == null) {
                cache1.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut6 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 800_000_000;
            while (reconResult.get() == null) {
                cache1.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut7 " + i);
        }));


        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 900_000_000;
            while (reconResult.get() == null) {
                cache2.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut8 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 1000_000_000;
            while (reconResult.get() == null) {
                cache2.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut9 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 1100_000_000;
            while (reconResult.get() == null) {
                cache2.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut10 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 1200_000_000;
            while (reconResult.get() == null) {
                cache2.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut11 " + i);
        }));


        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 1300_000_000;
            while (reconResult.get() == null) {
                cache3.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut12 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 1400_000_000;
            while (reconResult.get() == null) {
                cache3.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut13 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 1500_000_000;
            while (reconResult.get() == null) {
                cache3.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut14 " + i);
        }));
        loadFuts.add(GridTestUtils.runAsync(() -> {
            int i = 1600_000_000;
            while (reconResult.get() == null) {
                cache3.put(new TestKey(i=i+2), i);
            }
            System.out.println("loadFut15 " + i);
        }));

//            loadFuts.add(startAsyncLoad0(reconResult, cache0, startKey, endKey, false));
//            loadFuts.add(startAsyncLoad0(reconResult, cache0, startKey, endKey, false));
//            loadFuts.add(startAsyncLoad0(reconResult, cache0, startKey, endKey, false));
//            loadFuts.add(startAsyncLoad0(reconResult, cache0, startKey, endKey, false));

        GridTestUtils.runMultiThreadedAsync(() -> {
            reconResult.set(partitionReconciliation(client, builder));
        }, 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> reconResult.get() != null, 120_000);

        for (IgniteInternalFuture fut : loadFuts)
            fut.get();

//        for (long i = startKey; i < endKey; i++)
//            cache0.put(i, i);
//
//        long allKeysCountForCacheGroup;
//        long allKeysCountForCache;
//
//        for (String cacheName : cacheNames) {
//            allKeysCountForCacheGroup = 0;
//            allKeysCountForCache = 0;
//
//            for (int i = 0; i < nodesCnt; i++) {
//                long i0 = getFullPartitionsSizeForCacheGroup(grid(i), cacheName);
//                allKeysCountForCacheGroup += i0;
//
//                long i1 = getPartitionsSizeForCache(grid(i), cacheName);
//                allKeysCountForCache += i1;
//            }
//
//            assertEquals(endKey, client.cache(cacheName).size());
//
//            assertEquals(endKey, allKeysCountForCacheGroup);
//            assertEquals(endKey, allKeysCountForCache);
//
//        }

        System.out.println("cache.size() = " + cache0.size());

//        cache0.clear();

        //second reconciliation

    }

    static class TestKey {
        long value;

        private long[] array = new long[] {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
                40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
                60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
                70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
                80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
                90, 91, 92, 93, 94, 95, 96, 97, 98, 99
        };

        TestKey(long value) {
            this.value = value;
        }
    }

    /** Tests that only sizes of repaired caches fixed. */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testRepairPartOfCachesReconciliation() throws Exception {
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

        breakCacheSizes(grids, new HashSet<>(Arrays.asList(
                cache0.getName(),
                cache1.getName(),
                cache2_group0.getName(),
                cache3_group0.getName(),
                cache4_group1.getName(),
                cache5_group1.getName()))
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
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache0.getName());
        cacheNames.add(cache2_group0.getName());
        cacheNames.add(cache4_group1.getName());
        cacheNames.add(cache5_group1.getName());
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));
        builder.repairAlg(RepairAlgorithm.PRIMARY);

        List<String> errors = partitionReconciliation(client, builder).errors();

        assertTrue(errors.isEmpty());

        assertFalse(cache1.size() == 100);
        assertFalse(cache3_group0.size() == 100);
        assertTrue(cache0.size() == 100);
        assertTrue(cache2_group0.size() == 100);
        assertTrue(cache4_group1.size() == 100);
        assertTrue(cache5_group1.size() == 100);

    }

    /** Test size reconciliation for empty cache. */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testRepairEmptyCacheSize() throws Exception {
        testRepairCacheSize(0);
    }

    /** Test size reconciliation for cache with one entry. */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testRepairCacheSizeWithOneEntry() throws Exception {
        testRepairCacheSize(1);
    }

    /** */
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

        breakCacheSizes(grids, new HashSet<>(Arrays.asList(cache.getName())));

        assertFalse(cache.size() == entryCount);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache.getName());
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));
        builder.repairAlg(RepairAlgorithm.PRIMARY);

        List<String> errors = partitionReconciliation(client, builder).errors();

        assertTrue(errors.isEmpty());

        assertEquals(entryCount, cache.size());
    }

    /** */
    @Test
    public void testRepairSizeOfEmptyCacheWithoutPreloading() throws Exception {
        testRepairSizeOfEmptyCacheWithoutPreloading(false);
    }

    /** */
    @Test
    public void testRepairSizeOfEmptyCacheWithoutPreloadingWithCacheGroup() throws Exception {
        testRepairSizeOfEmptyCacheWithoutPreloading(true);
    }

    /** */
    public void testRepairSizeOfEmptyCacheWithoutPreloading(boolean cacheGrp) throws Exception {
        int nodesCnt = 4;

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("cache0");

        if (cacheGrp) {
            ccfg.setGroupName("group0");

            log().info(">>> Cache in cache group");
        }

        IgniteCache<Object, Object> cache = client.createCache(ccfg);

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache.getName());
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY, SIZES)));
        builder.repairAlg(RepairAlgorithm.PRIMARY);

        List<String> errors = partitionReconciliation(client, builder).errors();

        assertTrue(errors.isEmpty());

        assertEquals(0, cache.size());
    }

    /** Test that size not repaired if reconciliation was started without {@link ReconType#SIZES} */
    @Test
    public void testCacheSizeNotRepaired() throws Exception {
        int nodesCnt = 3;

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        IgniteCache cache = client.createCache("cache0");

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        breakCacheSizes(grids, new HashSet<>(Arrays.asList(cache.getName())));

        long brokenCacheSize = cache.size();

        assertFalse(cache.size() == 1000);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache.getName());
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(CONSISTENCY)));
        builder.repairAlg(RepairAlgorithm.PRIMARY);

        List<String> errors = partitionReconciliation(client, builder).errors();

        assertTrue(errors.isEmpty());

        assertEquals(brokenCacheSize, cache.size());
    }

    /** */
    @Test
    public void testRestartPersistenceCluaterAfterSizeReconciliation() throws Exception {
        int nodesCnt = 2;

        persistence = true;

        ig = startGrids(nodesCnt);

        ig.cluster().state(ClusterState.ACTIVE);

        client = startClientGrid(nodesCnt);

        int backupCnt = 1;

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("cache0")
            .setBackups(backupCnt);

        if (rnd.nextBoolean()) {
            ccfg.setGroupName("group0");

            log().info(">>> Cache in cache group");
        }

        int entryCount = 2000;

        IgniteCache cache = client.createCache(ccfg);

        for (int i = 0; i < entryCount; i++)
            cache.put(i, i);

        List<IgniteEx> grids = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++)
            grids.add(grid(i));

        breakCacheSizes(grids, new HashSet<>(Arrays.asList(cache.getName())));

        assertFalse(cache.size() == entryCount);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(10);
        Set<String> cacheNames = new HashSet<>();
        cacheNames.add(cache.getName());
        builder.caches(cacheNames);
        builder.batchSize(10);
        builder.reconTypes(new HashSet(Arrays.asList(SIZES)));
        builder.repairAlg(RepairAlgorithm.PRIMARY);

        List<String> errors = partitionReconciliation(client, builder).errors();

        assertTrue(errors.isEmpty());

        assertEquals(entryCount, cache.size());

        stopAllGrids();

        ig = startGrids(nodesCnt);

        ig.cluster().state(ClusterState.ACTIVE);

        cache = grid(1).cache("cache0");

        long allKeysCountForCacheGroup = 0;
        long allKeysCountForCache = 0;

        for (int i = 0; i < nodesCnt; i++) {
            long i0 = getFullPartitionsSizeForCacheGroup(grid(i), cache.getName());
            allKeysCountForCacheGroup += i0;

            long i1 = getPartitionsSizeForCache(grid(i), cache.getName());
            allKeysCountForCache += i1;
        }

        assertEquals(entryCount * (1 + backupCnt), allKeysCountForCacheGroup);
        assertEquals(entryCount * (1 + backupCnt), allKeysCountForCache);
    }

}