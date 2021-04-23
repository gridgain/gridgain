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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.Thread.sleep;
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

    static AtomicReference<ReconciliationResult> reconResult = new AtomicReference<>();

    static final long BROKEN_PART_SIZE = 666;

    /** Crd server node. */
    protected IgniteEx ig;

    /** Client. */
    protected IgniteEx client;

    private Random rnd = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        DataStorageConfiguration storageConfiguration = new DataStorageConfiguration();
        storageConfiguration.setPageSize(1024);

        cfg.setDataStorageConfiguration(storageConfiguration);

//        DataStorageConfiguration memCfg = new DataStorageConfiguration()
//            .setDefaultDataRegionConfiguration(
//                new DataRegionConfiguration().setMaxSize(400 * 1024 * 1024).setPersistenceEnabled(true));
//
//        cfg.setDataStorageConfiguration(memCfg);

//        CacheConfiguration ccfg = new CacheConfiguration();
//        ccfg.setName(DEFAULT_CACHE_NAME);
////        ccfg.setGroupName("zzz");
//        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
//        ccfg.setAffinity(new RendezvousAffinityFunction(false, 4));
//        ccfg.setBackups(1);
//        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//
//        cfg.setCacheConfiguration(ccfg);

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

    volatile int clear0;
    volatile int clear1;

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
        "backupCount = {5}, partCount = {6}, cacheGroup = {7}, batchSize = {8}, reconParallelism = {9}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[] {1, 0, 500, ATOMIC, PARTITIONED, 0, 1, null, 100, 1});
        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 0, 12, "testCacheGroup1", 100, 1});
        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 12, null, 100, 1});
        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, null, 100, 1});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 1, null, 10, 3});
        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 1, 12, null, 10, 3});
        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 12, "testCacheGroup1", 10, 3});
        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, null, 10, 3});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 1, null, 1, 10});
        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 0, 12, null, 1, 10});
        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 12, null, 1, 10});
        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, "testCacheGroup1", 1, 10});

        return params;
    }

    @Test
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
            if (i < endKey)
                cache.put(i, i);
        }

//        cache.removeAll();

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

        for (int i = 0; i < 8; i++)
            loadFuts.add(startAsyncLoad0(cache, false));

        GridTestUtils.runMultiThreadedAsync(() -> {
            reconResult.set(partitionReconciliation(client, builder));
        }, 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> reconResult.get() != null, 120_000);

        System.out.println("qvsdhntsd partitionReconciliation stop");

        for (IgniteInternalFuture fut : loadFuts)
            fut.get();

        for (long i = startKey; i < endKey; i++)
                grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        System.out.println("qdfgkjpoet " + getFullPartitionsSizeForCacheGroup(Collections.singletonList(grid(0)), DEFAULT_CACHE_NAME));
        System.out.println("qdfgkjpoet " + getFullPartitionsSizeForCacheGroup(Collections.singletonList(grid(1)), DEFAULT_CACHE_NAME));

        assertEquals(endKey, grid(0).cache(DEFAULT_CACHE_NAME).size());

        long allKeysCount = getFullPartitionsSizeForCacheGroup(grids, DEFAULT_CACHE_NAME);

        if (cacheMode == REPLICATED)
            assertEquals((long)endKey * nodesCnt, allKeysCount);
        else
            assertEquals((long)endKey * (backupCount + 1), allKeysCount);
        System.out.println(getPartitionsSizeForCache(grid(1), DEFAULT_CACHE_NAME));
    }

    private void setPartitionSize(IgniteEx grid, String cacheName, int partId, int delta) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        int cacheId = cctx.cacheId();

        cctx.group().topology().localPartition(partId).dataStore().updateSize(cacheId, delta);
    }

    private void updatePartitionsSize(IgniteEx grid, String cacheName) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        int cacheId = cctx.cacheId();

        cctx.group().topology().localPartitions().forEach(part -> part.dataStore().updateSize(cacheId, BROKEN_PART_SIZE/*Math.abs(rnd.nextInt())*/));
    }

    private long getFullPartitionsSizeForCacheGroup(List<IgniteEx> nodes, String cacheName) {
        return nodes.stream().mapToLong(node -> getFullPartitionsSizeForCacheGroup(node, cacheName)).sum();
    }

    private long getFullPartitionsSizeForCacheGroup(IgniteEx grid, String cacheName) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        int cacheId = cctx.cacheId();

        return cctx.group().topology().localPartitions().stream().mapToLong(part -> {
            System.out.println("qedsffd localPartition " + grid.name() + " " + cacheName + " " + part.dataStore().fullSize());
            return part.dataStore().fullSize();
        }).sum();
    }

    private long getPartitionsSizeForCache(IgniteEx grid, String cacheName) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        int cacheId = cctx.cacheId();

        System.out.println("qqqqqqqqqqqq");

        return cctx.group().topology().localPartitions()
            .stream()
//            .mapToLong(part -> {
//                System.out.println("zdfdf " + part);
//                System.out.println("dfvfd " + part.dataStore());
//                System.out.println("xfgnf " + part.dataStore().cacheSizes());
//                return part
//                    .dataStore()
//                    .cacheSizes()
//                    .get(cacheId);
//                }
//            )
            .mapToLong(part -> {
                System.out.println("zdfdf " + part);
                System.out.println("dfvfd " + part.dataStore());
                System.out.println("xfgnf " + part.dataStore().cacheSizes());
                if (cctx.group().sharedGroup())
                    return part
                    .dataStore()
                    .cacheSizes()
                    .get(cacheId);
                else
                    return part.dataStore().fullSize();
                }
            )
//            .mapToLong(part -> Optional.of(part.dataStore().cacheSizes().get(cacheId)).orElseGet(() -> 0L))
            .sum();
    }

    private void breakCacheSizes(List<IgniteEx> nodes, List<String> cacheNames) {
        nodes.forEach(node -> {
            cacheNames.forEach(cacheName -> {
                updatePartitionsSize(node, cacheName);
            });
        });
    }

    private IgniteInternalFuture startAsyncLoad0(IgniteCache cache, boolean clear) {
        return GridTestUtils.runAsync(() -> {
                while(reconResult.get() == null) {
                    int op;

                    if (clear)
                        op = rnd.nextInt(8);
                    else
                        op = rnd.nextInt(7);

                    long n;

                    switch (op) {
                        case 0:
                            Map map = new TreeMap();

                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            map.put(n, n);

                            cache.putAll(map);

                            break;

                        case 1:
                            Set set = new TreeSet();

                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);
                            n = startKey + rnd.nextInt(endKey - startKey);
                            set.add(n);

                            cache.removeAll(set);

                            break;
                        case 2:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            cache.put(n, n);

                            break;
                        case 3:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            cache.remove(n);

                            break;
                        case 4:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            cache.getAndPut(n, n+1);

                            break;
                        case 5:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            cache.getAndRemove(n);

                            break;
                        case 6:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            long n0 = n;

                            cache.invoke(n, (k, v) -> {
                                if (v == null)
                                    return n0;
                                else
                                    return null;
                            });

                            break;
                        case 7:
                            n = startKey + rnd.nextInt(endKey - startKey);

                            if (n % 10 == 0)
                                cache.clear();

                            break;
                    }



                }
            },
            "LoadThread");
    }

    private IgniteInternalFuture startAsyncLoad1(IgniteCache cache, boolean clear) {
        return GridTestUtils.runAsync(() -> {
            while(reconResult.get() == null) {
                    int i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    cache.put(i1, i1);

                if (clear && i1 % 20 == 0)
                    cache.clear();
            }
        });
    }

    public static void main1(String[] args) {
        Map<String, String> map = new ConcurrentHashMap<>();

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");

        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();

        Map.Entry<String, String> next = iterator.next();

        System.out.println(next);

        map.put("1", "1qwer");
        map.put("4", "4");
        map.put("3", "3qwer");

        while(iterator.hasNext()) {
            next = iterator.next();
            System.out.println(next);
        }

    }

    static class TestKey {
        int i;

        int[] array0 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array1 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array2 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array3 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array4 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array5 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array6 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array7 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array8 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        int[] array9 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };

        TestKey(int i) {
            this.i = i;
        }
    }

    static class TestValue {
        long[] array0 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array1 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array2 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array3 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array4 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array5 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array6 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array7 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array8 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array9 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array10 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array11 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array12 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array13 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array14 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array15 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array16 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array17 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array18 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };
        long[] array19 = {
            1, 2, 3, 4, 5, 6, 7, 8, 9,
            11, 12, 13, 14, 15, 16, 17, 18, 19,
            21, 22, 23, 24, 25, 26, 27, 28, 29,
            31, 32, 33, 34, 35, 36, 37, 38, 39,
            41, 42, 43, 44, 45, 46, 47, 48, 49,
            51, 52, 53, 54, 55, 56, 57, 58, 59,
            66, 62, 63, 64, 66, 66, 67, 68, 69,
            77, 72, 73, 74, 77, 76, 77, 78, 79,
            81, 82, 83, 84, 88, 86, 87, 88, 89,
            91, 92, 93, 94, 95, 96, 97, 98, 99
        };

        TestValue() {

        }

//        @Override public String toString() {
//            return "TestValue{" +
//                "array0=" + Arrays.toString(array0) +
//                '}';
//        }
    }

}