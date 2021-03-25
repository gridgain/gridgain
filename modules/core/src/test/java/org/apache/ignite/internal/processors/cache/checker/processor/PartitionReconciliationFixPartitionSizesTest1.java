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

import com.sun.tools.javac.util.List;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests count of calls the recheck process with different inputs.
 */
public class PartitionReconciliationFixPartitionSizesTest1 extends GridCommonAbstractTest {
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

    @Test
    public void main(/*String[] args*/) throws Exception {
        int pagesCount = 2;
//        int maxKey = partCount * partCount;
        int maxKey = 2;
        int keysPerPage = maxKey / pagesCount;

        Part part = new Part(pagesCount, keysPerPage);

        part.size.set(100_000);

//        Random rnd = new Random();

        for (int i = 0; i < maxKey; i += 2) {
//            if (!(i > 3500 && i < 4500) && !(i > 6500 && i < 8500))
//            if ((i > 1000) && (i < 8500))
                part.put(i);
            System.out.println("preload put " + i);
        }

        AtomicBoolean doLoad = new AtomicBoolean(true);

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            System.out.println("qvsdhntsd loadFut start");

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (doLoad.get()) {
                int i = rnd.nextInt(maxKey);
                part.put(i);

                int sleep = 1;

//                doSleep(sleep);

                i = rnd.nextInt(maxKey);
//                if (i % 2 == 0)
                    part.remove(i);

//                doSleep(sleep);
            }
        },
             "qwerthread0");

        IgniteInternalFuture loadFut1 = GridTestUtils.runAsync(() -> {
            System.out.println("qvsdhntsd loadFut start");

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (doLoad.get()) {
                int i = rnd.nextInt(maxKey);
                part.put(i);

                int sleep = 2;

//                doSleep(sleep);

                i = rnd.nextInt(maxKey);
//                if (i % 2 == 0)
                    part.remove(i);

//                doSleep(sleep);
            }
        },
            "qwerthread1");

//        doSleep(30);

        IgniteInternalFuture reconFut = GridTestUtils.runAsync(() -> {
            doRecon(part);

            doLoad.set(false);
            }
        );

        reconFut.get();
        loadFut.get();
        loadFut1.get();

        System.out.println("part.realSize() " + part.realSize());
        System.out.println("part.size " + part.size);
        System.out.println("part.reconSize " + part.reconSize);

        assertTrue(part.size.get() == part.realSize());

    }
    static void doRecon(Part part) {
//        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            part.partLock.writeLock().lock();

            try {
                part.reconInProgress = true;
            }
            finally {
                part.partLock.writeLock().unlock();
            }

//            boolean firstPage = true;

            for (Page page : part.pages) {
                System.out.println("start iter page ++++++++++++");

                page.pageLock.readLock().lock();

//                if (part.borderKey == null) {
//                    part.reconSize.set(0);
//                    part.tempMap.clear();
//                }

                try {
                    AtomicInteger firstKey;

                    //set border key
                    if (!page.keys.isEmpty()) {
                        firstKey = new AtomicInteger(page.keys.first());
                        part.borderKey = page.keys.last();
                        System.out.println("set new part.borderKey " + part.borderKey);
                    }
                    else {
                        System.out.println("continue");
                        continue;
                    }

                    //iterate over tempMap
                    Iterator<Map.Entry<Integer, Integer>> tempMapIter = part.tempMap.entrySet().iterator();

                    System.out.println("tempMap after get iterator " + part.tempMap.size() + part.tempMap);

//                    System.out.println("tempMap " + part.tempMap);

                    while (tempMapIter.hasNext()) {
                        Map.Entry<Integer, Integer> entry = tempMapIter.next();
                        doSleep(1);
                        part.tempMap.computeIfPresent(entry.getKey(), (k, v) -> {
                            if (entry.getKey() < firstKey.get()) {
                                part.reconSize.addAndGet(entry.getValue());

                                tempMapIter.remove();
                                return null;
                            }
                            else {
                                tempMapIter.remove();

                                return null;
                            }

                        });

//                        if (entry.getKey() < part.borderKey) {
//                            part.reconSize.addAndGet(entry.getValue());
//
//                                tempMapIter.remove();
//                        }
//                        else
//                            tempMapIter.remove();

                        System.out.println("entry from iterator " + entry);
                    }

//                    System.out.println("in recon --------------- reconSize after iter tempMap " + part.reconSize);

                    //iterate over page

//                    System.out.println("key in recon ---------------");
                    for (Integer key : page.keys) {
                        System.out.println("in recon added key to tempMap: key " + key + " delta " + 1);
                        part.tempMap.put(key, +1);

                        doSleep(1);
                    }

//                    System.out.println("key in recon --------------- reconSize " + part.reconSize);
                    System.out.println("stop iter page ++++++++++++");

                }
                finally {
                    page.pageLock.readLock().unlock();
                }
            }

            part.partLock.writeLock().lock();

            try {
                Iterator<Map.Entry<Integer, Integer>> tempMapIter = part.tempMap.entrySet().iterator();

                System.out.println("tempMap " + part.tempMap.size() + part.tempMap);

                while (tempMapIter.hasNext()) {
                    Map.Entry<Integer, Integer> entry = tempMapIter.next();

//                    if (entry.getKey() < part.borderKey) {
                        part.reconSize.addAndGet(entry.getValue());

//                        tempMapIter.remove();
//                    }
//                    else
//                        tempMapIter.remove();
                }

                part.reconInProgress = false;

                System.out.println("old part.size " + part.size.get());
                part.size.set(part.reconSize.get());
                System.out.println("new part.size " + part.size.get());
            }
            finally {
                part.partLock.writeLock().unlock();
            }
//        });

//        return loadFut;
    }

    static class Part {
        volatile ReentrantReadWriteLock partLock = new ReentrantReadWriteLock();

        volatile CopyOnWriteArrayList<Page> pages;

        volatile int pagesCount;

        volatile int keysPerPage;

        volatile AtomicInteger size = new AtomicInteger();

        // recon
        volatile boolean reconInProgress;

        volatile AtomicInteger reconSize = new AtomicInteger();

        volatile Integer borderKey;

        volatile Map<Integer, Integer> tempMap = new ConcurrentSkipListMap<>();


        Part(int count, int keysPerPage) {
            this.pagesCount = count;
            this.keysPerPage = keysPerPage;
            this.pages = new CopyOnWriteArrayList<>();

            for (int i = 0; i < count; i++) {
                pages.add(new Page());

            }
        }

        void put(int key) {
            partLock.readLock().lock();

            try {
                int pageNumber = key / keysPerPage;
//                System.out.println("asdf key " + key + " pagesCount " + pagesCount + " pageNumber " + pageNumber);

                System.out.println("pageNumber/pages " + pageNumber + "/" + pages.size());

                Page page = pages.get(pageNumber);
                
                page.pageLock.writeLock().lock();
                
                try {
                    if (!page.keys.contains(key)) {
                        page.keys.add(key);

                        System.out.println("put key " + key);

//                    if (reconInProgress) {
//                        if (key < borderKey)
//                            reconSize.incrementAndGet();
//                        else if (tempMap.get(key) != null && tempMap.get(key) != 1)
//                            tempMap.remove(key);
//                        else
//                            tempMap.put(key, +1);
//                    }

                        if (reconInProgress) {
                            if (borderKey != null && key <= borderKey) {
                                doSleep(1);
                                reconSize.incrementAndGet();
                                System.out.println("in PUT after increment reconSize: key " + key + " reconSize " + reconSize.get());
                            }
                            else {
                                doSleep(1);

//                                tempMap.compute(key, (k, v) -> {
//                                    if (v != null && !v.equals(1)) {
//                                        System.out.println("in PUT remove key from tempMap: key " + key);
//                                        return null;
//                                    }
//                                    else if (new Integer(1).equals(v)) {
//                                        System.out.println("in PUT added key to tempMap: key " + key + " delta " + 1);
//                                        return 1;
//                                    }
//                                    else
//                                        return v;
//                                });
                                tempMap.put(key, 1);
                                System.out.println("in PUT added key to tempMap: key " + key);
                            }
                        }

                        size.incrementAndGet();
                    }
                }
                finally {
                    page.pageLock.writeLock().unlock();
                }
            }
            finally {
                partLock.readLock().unlock();
            }
        }

        void remove(int key) {
            partLock.readLock().lock();

            try {
                int pageNumber = key / keysPerPage;

                System.out.println("pageNumber/pages " + pageNumber + "/" + pages.size());

                Page page = pages.get(pageNumber);

                page.pageLock.writeLock().lock();

                try {
                    if (page.keys.contains(key)) {
                        page.keys.remove(key);

                        System.out.println("remove key " + key);

//                    if (reconInProgress) {
//                        if (key < borderKey)
//                            reconSize.incrementAndGet();
//                        else if (tempMap.get(key) != null && tempMap.get(key) != -1)
//                            tempMap.remove(key);
//                        else
//                            tempMap.put(key, -1);
//                    }

                        if (reconInProgress) {
                            if (borderKey != null && key <= borderKey) {

                                doSleep(1);
                                reconSize.decrementAndGet();
                                System.out.println("in REMOVE after decrement reconSize: key " + key + " reconSize " + reconSize.get());
                            }
                            else {
                                String strThread = Thread.currentThread().getName();

                                doSleep(1);

                                System.out.println("in REMOVE before compute " + strThread);

                                tempMap.computeIfPresent(key, (k, v) -> {
                                    System.out.println("in REMOVE start compute " + strThread);

                                    return null;
                                });

//                                tempMap.compute(key, (k, v) -> {
//                                    System.out.println("in REMOVE start compute " + strThread);
//
//                                    if (v != null && v.equals(1)) {
//                                        System.out.println("in REMOVE remove key from tempMap: key " + key + " " + strThread);
//                                        return null;
//                                    }
////                                    else if (new Integer(1).equals(v)) {
////                                        System.out.println("in PUT added key to tempMap: key " + key + " delta " + 1);
////                                        return -1;
////                                    }
//                                    else {
//                                        System.out.println("in REMOVE not changed key in tempMap: key " + key + " " + strThread);
//                                        return v;
//                                    }
//                                });
                            }
                        }

                        size.decrementAndGet();
                    }
                }
                finally {
                    page.pageLock.writeLock().unlock();
                }
            }
            finally {
                partLock.readLock().unlock();
            }
        }

        int realSize() {
            int realSize = 0;

            for (Page page : pages) {
                realSize += page.keys.size();
            }

            return realSize;
        }
    }

    static class Page {
        volatile ReentrantReadWriteLock pageLock = new ReentrantReadWriteLock();

        volatile ConcurrentSkipListSet<Integer> keys = new ConcurrentSkipListSet<>();
    }

//    В тикете было описано как по логам определять наличие расхождения LWM и HWM. Если до того как стрельнет "AssertionError: LWM after HWM" пройдет мало времени, то вероятно мы не успеем в полуручном режиме починить LWM и HWM. Возможно мы могли бы вместо того чтобы сразу ассертить попробовать автоматически починить эту проблему.

}