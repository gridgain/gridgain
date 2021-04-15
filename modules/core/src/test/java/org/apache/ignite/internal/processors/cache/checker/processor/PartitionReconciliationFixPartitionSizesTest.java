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
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.lang.Thread.sleep;

/**
 * Tests count of calls the recheck process with different inputs.
 */
public class PartitionReconciliationFixPartitionSizesTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static int nodesCnt = 2;

    protected static int backups = 1;

    static int startKey = 0;

    static int endKey = 1000;

    static AtomicReference<ReconciliationResult> reconResult = new AtomicReference<>();

    static final long BROKEN_PART_SIZE = 9;

//    private List<String> cacheGroup0 = new ArrayList<>();
//    private List<String> cacheGroup1 = new ArrayList<>();

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

        ig = startGrids(nodesCnt);

        client = startClientGrid(nodesCnt);

        ig.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testRepair() throws Exception {
        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            cache.put(i, i);
        }

        setPartitionSize(grid(0), DEFAULT_CACHE_NAME, 0, 58);
        setPartitionSize(grid(0), DEFAULT_CACHE_NAME, 1, -129);
        setPartitionSize(grid(1), DEFAULT_CACHE_NAME, 0, 536);
        setPartitionSize(grid(1), DEFAULT_CACHE_NAME, 1, 139);

        assertFalse(cache.size() == 100);

//        doSleep(500);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(1);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> objects = new HashSet<>();
        objects.add(DEFAULT_CACHE_NAME);
//        objects.add("qqq");
        builder.caches(objects);

        AtomicReference<ReconciliationResult> res = new AtomicReference<>();

        GridTestUtils.runMultiThreadedAsync(() -> res.set(partitionReconciliation(client, builder)), 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> res.get() != null, 40_000);

        ReconciliationResult reconciliationRes = res.get();


        assertEquals(100, cache.size());
//        assertEquals(0, res.get().partitionReconciliationResult().inconsistentKeysCount());
//        org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationResultCollector.Simple.partSizesMap
//        internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0)
//        PartitionReconciliationProcessor#execute
//        CollectPartitionKeysByBatchTask.CollectPartitionKeysByBatchJob.execute0
    }

    @Test
    public void test1() throws Exception {
//        CollectPartitionKeysByBatchTask.msg.clear();
//        CollectPartitionKeysByBatchTask.msg1.clear();
        BPlusTree.i0 = 0;
        clear0 = 0;
        clear1 = 0;

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        int startKey = 0;
//        int endKey = 337;
        int endKey = 2;

        cache.put(1, 1);
        cache.remove(1);
//        cache.put(1, 1);
        cache.remove(1);
    }

    volatile int clear0;
    volatile int clear1;

    @Test
//    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testRepairUnderLoad() throws Exception {
//        CollectPartitionKeysByBatchTask.msg.clear();
//        CollectPartitionKeysByBatchTask.msg1.clear();
//        CollectPartitionKeysByBatchTask.i = 0;
//        BPlusTree.i0 = 0;

//        CacheConfiguration ccfg0 = new CacheConfiguration();
//        ccfg0.setName(DEFAULT_CACHE_NAME);
////        ccfg0.setGroupName("zzz");
//        ccfg0.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
//        ccfg0.setAffinity(new RendezvousAffinityFunction(false, 4));
////        ccfg0.setBackups(1);
//        ccfg0.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//
//        client.createCache(ccfg0);

        IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

        int startKey = 0;
//        int endKey = 337;
//        int endKey = 667;//qssefvsdae cacheSize after recon 170
        int endKey = 1000;

        AtomicInteger putCount = new AtomicInteger();
        AtomicInteger removeCount = new AtomicInteger();

        for (int i = startKey; i < endKey; i++) {
            i += 1;
            if (i < endKey) {
                cache.put(i, i);
                putCount.incrementAndGet();
            }
        }

//        cache.removeAll();

        int startSize = cache.size();

//        setPartitionSize(grid(0), DEFAULT_CACHE_NAME, 0, 58);
//        setPartitionSize(grid(0), DEFAULT_CACHE_NAME, 1, -129);
//        setPartitionSize(grid(1), DEFAULT_CACHE_NAME, 0, 536);
//        setPartitionSize(grid(1), DEFAULT_CACHE_NAME, 1, 139);

        breakCacheSizes(Arrays.asList(grid(0)/*, grid(1), grid(2), grid(3)*/), Arrays.asList(DEFAULT_CACHE_NAME));

        assertFalse(cache.size() == startSize);

        doSleep(300);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(1);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> objects = new HashSet<>();
        objects.add(DEFAULT_CACHE_NAME);
//        objects.add("qqq");
        builder.caches(objects);
        builder.batchSize(100);


        AtomicReference<ReconciliationResult> res = new AtomicReference<>();

//        IgniteInternalFuture loadFut0 = GridTestUtils.runAsync(() -> {
//            System.out.println("qvsdhntsd loadFut start");
//
////            try {
////                sleep(1);
////            }
////            catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//
//            int i = 0;
//            int i1 = 0;
//
//            int max = 0;
//
//            while(res.get() == null && i1 < endKey/* || i < endKey*/) {
//
////                int i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
////                if (!cache.containsKey(i1)) {
//                System.out.println("qdervdvds before put in test key: " + i1);
//                cache.put(i1, new TestValue());
////                    putCount.incrementAndGet();
//                System.out.println("qdervdvds after put in test key: " + i1);
////                }
//
//                i1++/* + ((endKey - startKey) / 10)*/;
//
//                try {
//                    sleep(8);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//
////                i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
////                System.out.println("qdflpltis before remove in test key: " + i1);
////                if (cache.containsKey(i1)) {
////                    cache.remove(i1);
////                    removeCount.incrementAndGet();
////                System.out.println("qdflpltis after remove in test key: " + i1);
////                }
//
////                try {
////                    sleep(3);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                System.out.println("qfegsdg put random: " + i1);
////                doSleep(3);
//
////                if (i1 > max)
////                    max = i1;
//
////                if (i < endKey) {
////                    cache.put(i, i);
////                    i++;
////                }
//            }
//
//            System.out.println("qvraslpf loadFut stop" + i);
//            System.out.println("qmfgtssf loadFut max" + max);
//        });

        IgniteInternalFuture loadFut0 = GridTestUtils.runAsync(() -> {
//            doSleep(150);
//                try {
//                    CollectPartitionKeysByBatchTask.latch.await();
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                System.out.println("qvsdhntsd loadFut0 start");

            int i = 0;

            int max = 0;

            while(res.get() == null/* && i < 10000*/) {
                i++;
//                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    int i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    int i2 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    int i3 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    int i4 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    int i5 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
//                if (!cache.containsKey(i1)) {

                Map map = new TreeMap();

                map.put(i1, i1);
                map.put(i2, i2);
                map.put(i3, i3);
                map.put(i4, i4);
                map.put(i5, i5);

                    System.out.println("qdervdvds before put in test key: " + cache.containsKey(i1) + " " + i1);
                    cache.putAll(map);
                    System.out.println("qdervdvds after put in test key: " + i1);

//                putCount.incrementAndGet();
//                }

//                    try {
//                        sleep(1);
//                    }
//                    catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//
                    i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    i2 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    i3 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    i4 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    i5 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
//                if (cache.containsKey(i1)) {

                Set set = new TreeSet();

                set.add(i1);
                set.add(i2);
                set.add(i3);
                set.add(i4);
                set.add(i5);

                    System.out.println("qdflpltis before remove in test key: " + cache.containsKey(i1) + " " + i1);
                    cache.removeAll(set);
                    System.out.println("qdflpltis after remove in test key: " + i1);

//                    try {
//                        sleep(1);
//                    }
//                    catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }

//                    tx.commit();
//                }

//                if (i1 % 3 == 0 /*&& *//*i == 10 && *//*!clear*/) {
//                    System.out.println("qfgthsfs start clear, cache.size() " + cache.size());
//                    cache.clear();
//                    System.out.println("qfgthsfs finish clear " + cache.size());
//                    clear0++;
//                }
//                    removeCount.incrementAndGet();
//                }

//                try {
//                    sleep(10);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

//                try {
//                    sleep(3);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

//                System.out.println("qfegsdg put random: " + i1);
//                doSleep(3);

//                if (i1 > max)
//                    max = i1;

//                if (i < endKey) {
//                    cache.put(i, i);
//                    i++;
//                }
            }

            System.out.println("qvraslpf loadFut1 stop" + i);
            System.out.println("qmfgtssf loadFut1 max" + max);
        },
            "LoadThread");

//        IgniteInternalFuture loadFut1 = GridTestUtils.runAsync(() -> {
//            System.out.println("qvsdhntsd loadFut1 start");
//
//            int i = 0;
//
//            int max = 0;
//
//            while(res.get() == null/* || i < endKey*/) {
//
////                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
//                    int i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
////                if (!cache.containsKey(i1)) {
//                    System.out.println("qdervdvds before put in test key: " + cache.containsKey(i1) + " " + i1);
//                    cache.put(i1, i1);
//                    System.out.println("qdervdvds after put in test key: " + i1);
////                putCount.incrementAndGet();
////                }
//
////                    try {
////                        sleep(1);
////                    }
////                    catch (InterruptedException e) {
////                        e.printStackTrace();
////                    }
//
//                    i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
////                if (cache.containsKey(i1)) {
//                    System.out.println("qdflpltis before remove in test key: " + cache.containsKey(i1) + " " + i1);
//                    cache.remove(i1);
//                    System.out.println("qdflpltis after remove in test key: " + i1);
////                    removeCount.incrementAndGet();
////                }
//
////                    try {
////                        sleep(1);
////                    }
////                    catch (InterruptedException e) {
////                        e.printStackTrace();
////                    }
//
////                    tx.commit();
////                }
//
////                if (i1 % 3 == 0 /*&& *//*i == 10 && *//*!clear*/) {
////                    System.out.println("qfgthsfs start clear, cache.size() " + cache.size());
////                    cache.clear();
////                    System.out.println("qfgthsfs finish clear " + cache.size());
////                    clear1++;
////                }
////
////                try {
////                    sleep(1);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                try {
////                    sleep(3);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                System.out.println("qfegsdg put random: " + i1);
////                doSleep(3);
//
////                if (i1 > max)
////                    max = i1;
//
////                if (i < endKey) {
////                    cache.put(i, i);
////                    i++;
////                }
//            }
//
//            System.out.println("qvraslpf loadFut1 stop " + i);
//            System.out.println("qmfgtssf loadFut1 max " + max);
//        });

//        IgniteInternalFuture loadFut2 = GridTestUtils.runAsync(() -> {
//            System.out.println("qvsdhntsd loadFut1 start");
//
//            int i = 0;
//
//            int max = 0;
//
//            while(res.get() == null/* || i < endKey*/) {
//
//                int i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
////                if (!cache.containsKey(i1)) {
//                System.out.println("qdervdvds before put in test key: " + i1);
//                cache.put(i1, i1);
//                System.out.println("qdervdvds after put in test key: " + i1);
////                putCount.incrementAndGet();
////                }
//
////                try {
////                    sleep(40);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
//                i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
////                if (cache.containsKey(i1)) {
//                System.out.println("qdflpltis before remove in test key: " + i1);
//                cache.remove(i1);
//                System.out.println("qdflpltis after remove in test key: " + i1);
////                    removeCount.incrementAndGet();
////                }
//
////                try {
////                    sleep(40);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                try {
////                    sleep(10);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                try {
////                    sleep(3);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                System.out.println("qfegsdg put random: " + i1);
////                doSleep(3);
//
////                if (i1 > max)
////                    max = i1;
//
////                if (i < endKey) {
////                    cache.put(i, i);
////                    i++;
////                }
//            }
//
//            System.out.println("qvraslpf loadFut1 stop " + i);
//            System.out.println("qmfgtssf loadFut1 max " + max);
//        });
//
//        IgniteInternalFuture loadFut3 = GridTestUtils.runAsync(() -> {
//            System.out.println("qvsdhntsd loadFut1 start");
//
//            int i = 0;
//
//            int max = 0;
//
//            while(res.get() == null/* || i < endKey*/) {
//
//                int i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
////                if (!cache.containsKey(i1)) {
//                System.out.println("qdervdvds before put in test key: " + i1);
//                cache.put(i1, new TestValue());
//                System.out.println("qdervdvds after put in test key: " + i1);
////                putCount.incrementAndGet();
////                }
//
////                try {
////                    sleep(40);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
//                i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
////                if (cache.containsKey(i1)) {
//                System.out.println("qdflpltis before remove in test key: " + i1);
//                cache.remove(i1);
//                System.out.println("qdflpltis after remove in test key: " + i1);
////                    removeCount.incrementAndGet();
////                }
//
////                try {
////                    sleep(40);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                try {
////                    sleep(10);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                try {
////                    sleep(3);
////                }
////                catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//
////                System.out.println("qfegsdg put random: " + i1);
////                doSleep(3);
//
////                if (i1 > max)
////                    max = i1;
//
////                if (i < endKey) {
////                    cache.put(i, i);
////                    i++;
////                }
//            }
//
//            System.out.println("qvraslpf loadFut1 stop " + i);
//            System.out.println("qmfgtssf loadFut1 max " + max);
//        });

        System.out.println("qvsdhntsd partitionReconciliation start");

        GridTestUtils.runMultiThreadedAsync(() -> {
            doSleep(30);

            res.set(partitionReconciliation(client, builder));
        }, 1, "reconciliation");

//        CollectPartitionKeysByBatchTask.latch.await();

//        cache.removeAll();

//        System.out.println("qfdrbad removeAll");

        GridTestUtils.waitForCondition(() -> res.get() != null, 60_000);

        System.out.println("qvsdhntsd partitionReconciliation stop");

        ReconciliationResult reconciliationRes = res.get();

        loadFut0.get();
//        loadFut1.get();
//        loadFut2.get();
//        loadFut3.get();
//        loadFut2.get();

//        doSleep(500);

//        endKey = 1000;

//        try {
//            FinalizeCountersDiscoveryMessage msg = new FinalizeCountersDiscoveryMessage();
//
//            msg.partSizesMap = res.get().partSizesMap();
//
//
//            System.out.println("qsdzgsdfg msg.partSizesMap.size(): " + msg.partSizesMap.size());
//
//            grid(0).context().discovery().sendCustomEvent(msg);
//        }
//        catch (IgniteCheckedException e) {
//            e.printStackTrace();
//        }

//        doSleep(200);

//        assertTrue(cache.size() == 300);

//        cache.put(101, 102);

        System.out.println("qssefvsdae cacheSize after recon " + cache.size());
//        System.out.println("qssefvsdae key 0 after recon " + cache.get(0));
//        System.out.println("qssefvsdae key 1 after recon " + cache.get(1));

//        for (int i = startKey; i < endKey; i++) {
//            Object o = grid(0).cache(DEFAULT_CACHE_NAME).get(i);
//
//            System.out.println("keys after async load: " + o);
//        }

        int putsAftreAll = 0;

        for (int i = startKey; i < endKey; i++) {
            if (!grid(0).cache(DEFAULT_CACHE_NAME).containsKey(i)) {
                grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);
                putCount.incrementAndGet();
//            cache.remove(i + endKey);
                removeCount.incrementAndGet();

//                System.out.println("qfegsdg put after all: " + i);

                ++putsAftreAll;
            }
        }

        System.out.println("qwghptikd putsAftreAll " + putsAftreAll);//991+173(172)=1164
//        System.out.println("qdsvdrd " + CollectPartitionKeysByBatchTask.msg);
//        System.out.println("qcsdfrs " + CollectPartitionKeysByBatchTask.msg1);
//        System.out.println("qgdhmkff iCleanup " + CollectPartitionKeysByBatchTask.iCleanup);
//        System.out.println("qnjkpfidj iUpdate " + CollectPartitionKeysByBatchTask.iUpdate);
//        System.out.println("qcgpolhuj batches " + CollectPartitionKeysByBatchTask.i);
        System.out.println("qetfstrghbe i0 " + BPlusTree.i0);
        System.out.println("qngjojetghd cach.clear0() " + clear0);
        System.out.println("qngjojetghd cach.clear1() " + clear1);

        System.out.println("qfsvrsdsdsd putCount " + putCount + ", removeCount " + removeCount);

//        for (int i = startKey; i < endKey; i++) {
//            grid(0).cache(DEFAULT_CACHE_NAME).get(i);
//
//            System.out.println("qfegsdg get after all: " + i);
//        }

        assertEquals(endKey, grid(0).cache(DEFAULT_CACHE_NAME).size());

//        assertEquals(0, res.get().partitionReconciliationResult().inconsistentKeysCount());
//        org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationResultCollector.Simple.partSizesMap
//        internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0)
//        PartitionReconciliationProcessor#execute
//        CollectPartitionKeysByBatchTask.CollectPartitionKeysByBatchJob.execute0

//        int cacheId = client.context().cache().cache(DEFAULT_CACHE_NAME).context().cacheId();
//        long delta00 = ((internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0).dataStore())).reconciliationCtx().storageSizeDelta(cacheId);
    }

    @Test
//    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testRepairUnderLoad0() throws Exception {

        CacheConfiguration ccfg0 = new CacheConfiguration();
        ccfg0.setName(DEFAULT_CACHE_NAME);
//        ccfg0.setGroupName("zzz");
        ccfg0.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg0.setAffinity(new RendezvousAffinityFunction(false, 4));
        ccfg0.setBackups(1);
        ccfg0.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg0.setCacheMode(CacheMode.PARTITIONED);

        client.createCache(ccfg0);

        IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

        startKey = 0;
        endKey = 3000;

        for (int i = startKey; i < endKey; i++) {
            i += 1;
            if (i < endKey)
                cache.put(i, i);
        }

//        cache.removeAll();

        int startSize = cache.size();

        breakCacheSizes(Arrays.asList(grid(0)/*, grid(1), grid(2), grid(3)*/), Arrays.asList(DEFAULT_CACHE_NAME));

        assertFalse(cache.size() == startSize);

        doSleep(300);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(1);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> objects = new HashSet<>();
        objects.add(DEFAULT_CACHE_NAME);
//        objects.add("qqq");
        builder.caches(objects);
        builder.batchSize(100);


        reconResult = new AtomicReference<>();

        List<IgniteInternalFuture> loadFuts = new ArrayList<>();

        loadFuts.add(startAsyncLoad0(cache));
        loadFuts.add(startAsyncLoad1(cache));

        GridTestUtils.runMultiThreadedAsync(() -> {
            doSleep(30);

            reconResult.set(partitionReconciliation(client, builder));
        }, 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> reconResult.get() != null, 60_000);

        System.out.println("qvsdhntsd partitionReconciliation stop");

        for (IgniteInternalFuture fut : loadFuts) {
            fut.get();
        }

        for (int i = startKey; i < endKey; i++) {
//            if (!grid(0).cache(DEFAULT_CACHE_NAME).containsKey(i))
                grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);
        }

        System.out.println("qdfgkjpoet " + getFullPartitionsSizeForCacheGroup(Collections.singletonList(grid(0)), DEFAULT_CACHE_NAME));
        System.out.println("qdfgkjpoet " + getFullPartitionsSizeForCacheGroup(Collections.singletonList(grid(1)), DEFAULT_CACHE_NAME));

        assertEquals(endKey, grid(0).cache(DEFAULT_CACHE_NAME).size());

        long allKeysCount = getFullPartitionsSizeForCacheGroup(Arrays.asList(grid(0), grid(1)), DEFAULT_CACHE_NAME);

        doSleep(100);
//        assertEquals(endKey * (backups + 1), allKeysCount);
//        System.out.println(getPartitionsSizeForCache(grid(1), DEFAULT_CACHE_NAME));
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

//        cctx.group().topology().localPartition(partId).dataStore().updateSize(cacheId, delta);
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

        return cctx.group().topology().localPartitions()
            .stream()
            .mapToLong(part -> part.dataStore().cacheSizes().get(cacheId)).sum();

//        cctx.group().topology().localPartition(partId).dataStore().updateSize(cacheId, delta);
    }

    private void breakCacheSizes(List<IgniteEx> nodes, List<String> cacheNames) {
        nodes.forEach(node -> {
            cacheNames.forEach(cacheName -> {
                updatePartitionsSize(node, cacheName);
            });
        });
    }

    private IgniteInternalFuture startAsyncLoad0(IgniteCache cache) {
        return GridTestUtils.runAsync(() -> {
                while(reconResult.get() == null) {
                    int i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    int i2 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    int i3 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    int i4 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    int i5 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;

                    Map map = new TreeMap();

                    map.put(i1, i1);
                    map.put(i2, i2);
                    map.put(i3, i3);
                    map.put(i4, i4);
                    map.put(i5, i5);

                    cache.putAll(map);

                    i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    i2 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    i3 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    i4 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    i5 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;

                    Set set = new TreeSet();

                    set.add(i1);
                    set.add(i2);
                    set.add(i3);
                    set.add(i4);
                    set.add(i5);

                    cache.removeAll(set);
                }
            },
            "LoadThread");
    }

    private IgniteInternalFuture startAsyncLoad1(IgniteCache cache) {
        return GridTestUtils.runAsync(() -> {
            while(reconResult.get() == null) {
                    int i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    cache.put(i1, i1);

                    i1 = startKey + rnd.nextInt(endKey - startKey)/* + ((endKey - startKey) / 10)*/;
                    cache.remove(i1);

                if (i1 % 5 == 0)
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