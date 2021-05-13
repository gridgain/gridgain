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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests count of calls the recheck process with different inputs.
 */
public class PartitionSimulator extends GridCommonAbstractTest {
//    @Test
//    public void main1(/*String[] args*/) {
//        Map<String, String> map = new ConcurrentHashMap<>();
//
//        map.put("1", "1");
//        map.put("2", "2");
//        map.put("3", "3");
//
//        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
//
//        Map.Entry<String, String> next = iterator.next();
//
//        System.out.println(next);
//
//        map.put("1", "1qwer");
//        map.put("4", "4");
//        map.put("3", "3qwer");
//
//        while(iterator.hasNext()) {
//            next = iterator.next();
//            System.out.println(next);
//        }
//
//    }
//
//    @Test
//    public void main2(/*String[] args*/) {
//        Map<String, String> map = new ConcurrentHashMap<>();
//
//        map.put("1", "1");
//        map.put("2", "2");
//        map.put("3", "3");
//
//        map.compute("4", (k, v) -> {
//            System.out.println("qwer" + k + v);
//            return null;
//        });
//
//        System.out.println(map);
//
//    }
//
//    @Test
//    public void main3(/*String[] args*/) throws Exception {
//        Map<String, String> map = new ConcurrentHashMap<>();
////        Map<String, String> map = new ConcurrentSkipListMap<>();
//
//        map.put("1", "1");
////        map.put("2", "1");
//
//        IgniteInternalFuture fut0 = GridTestUtils.runAsync(() -> {
//            map.compute("1", (k, v) -> {
//                if (v.equals("1")) {
//                    System.out.println("in compute before sleep");
//                    doSleep(5000);
//                    System.out.println("in compute after sleep");
//                    return "2";
//                }
//                else {
//                    return "3";
//                }
//            });
//
//            }
//        );
//
//        IgniteInternalFuture fut1 = GridTestUtils.runAsync(() -> {
//            doSleep(1000);
//            System.out.println("before compute");
//            map.compute("1", (k, v) -> {
//                System.out.println("in compute");
//                if (v.equals("1")) {
//                    System.out.println("in compute in if");
//                    return "4";
//                }
//                else {
//                    return "5";
//                }
//            });
//
//            }
//        );
//
//        fut0.get();
//        fut1.get();
//
//        System.out.println(map);
//
//    }

    static Map params;

//    @Test
//    public void test() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 20);
//        params.put("keysCount", 2000);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 1);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", true);
//        params.put("lastPagesIsEmpty", true);
//        params.put("middlePagesIsEmpty", true);
//
//        main();
//    }

//    @Test
//    public void test0() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 1);
//        params.put("keysCount", 0);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 0);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", false);
//        params.put("lastPagesIsEmpty", false);
//        params.put("middlePagesIsEmpty", false);
//
//        main();
//    }

//    @Test
//    public void test1() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 1);
//        params.put("keysCount", 1);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 0);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", false);
//        params.put("lastPagesIsEmpty", false);
//        params.put("middlePagesIsEmpty", false);
//
//        main();
//    }

    @Test
    public void test2() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 1);
        params.put("keysCount", 2);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

    @Test
    public void test3() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 1);
        params.put("keysCount", 3);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

//    @Test
//    public void test4() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 2);
//        params.put("keysCount", 0);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 0);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", false);
//        params.put("lastPagesIsEmpty", false);
//        params.put("middlePagesIsEmpty", false);
//
//        main();
//    }

//    @Test
//    public void test5() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 2);
//        params.put("keysCount", 1);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 0);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", false);
//        params.put("lastPagesIsEmpty", false);
//        params.put("middlePagesIsEmpty", false);
//
//        main();
//    }

    @Test
    public void test6() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 2);
        params.put("keysCount", 2);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

//    @Test
//    public void test7() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 2);
//        params.put("keysCount", 3);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 0);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", false);
//        params.put("lastPagesIsEmpty", false);
//        params.put("middlePagesIsEmpty", false);
//
//        main();
//    }

//    @Test
//    public void test8() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 3);
//        params.put("keysCount", 0);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 0);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", false);
//        params.put("lastPagesIsEmpty", false);
//        params.put("middlePagesIsEmpty", false);
//
//        main();
//    }

//    @Test
//    public void test9() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 3);
//        params.put("keysCount", 1);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 0);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", false);
//        params.put("lastPagesIsEmpty", false);
//        params.put("middlePagesIsEmpty", false);
//
//        main();
//    }

//    @Test
//    public void test10() throws Exception {
//        params = new HashMap();
//
//        params.put("pagesCount", 3);
//        params.put("keysCount", 2);
//        params.put("loadTreads", 2);
////        params.put("puts/removes", 2);
////        params.put("sleepInLoadFutures", 0);
//        params.put("sleepInLoadFuture1", 0);
//        params.put("sleepInLoadFuture2", 0);
//        params.put("sleepInReconTempMapPut", 0);
//        params.put("sleepInPutOpTempMapPut", 0);
//        params.put("sleepInRemoveOpTempMapRemove", 0);
//        params.put("sleepInReconCompute", 0);
////        params.put("sleepInPutOpCompute", 0);
//        params.put("sleepInRemoveOpCompute", 0);
//        params.put("firstPagesIsEmpty", false);
//        params.put("lastPagesIsEmpty", false);
//        params.put("middlePagesIsEmpty", false);
//
//        main();
//    }

    @Test
    public void test11() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 3);
        params.put("keysCount", 3);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

    @Test
    public void test12() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 1000);
        params.put("keysCount", 1000);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

    @Test
    public void test13() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 1000);
        params.put("keysCount", 2000);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

    @Test
    public void test14() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 1000);
        params.put("keysCount", 5000);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

    @Test
    public void test15() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 1000);
        params.put("keysCount", 10000);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

//    WITH EMPTY PAGES
    @Test
    public void test16() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 20);
        params.put("keysCount", 60);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", true);
        params.put("lastPagesIsEmpty", true);
        params.put("middlePagesIsEmpty", true);

        main();
    }

    //    WITH SLEEP
    //    (all sleeps)
    @Test
    public void test17() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 2);
        params.put("keysCount", 2);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 1);
        params.put("sleepInLoadFuture2", 1);
        params.put("sleepInReconTempMapPut", 1);
        params.put("sleepInPutOpTempMapPut", 1);
        params.put("sleepInRemoveOpTempMapRemove", 1);
        params.put("sleepInReconCompute", 1);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 1);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

    //    (not sleep in load)
    @Test
    public void test18() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 2);
        params.put("keysCount", 2);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 1);
        params.put("sleepInPutOpTempMapPut", 1);
        params.put("sleepInRemoveOpTempMapRemove", 1);
        params.put("sleepInReconCompute", 1);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 1);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

    @Test
    public void test19() throws Exception {
        params = new HashMap();

        params.put("pagesCount", 200);
        params.put("keysCount", 20000);
        params.put("loadTreads", 2);
//        params.put("puts/removes", 2);
//        params.put("sleepInLoadFutures", 0);
        params.put("sleepInLoadFuture1", 0);
        params.put("sleepInLoadFuture2", 0);
        params.put("sleepInReconTempMapPut", 0);
        params.put("sleepInPutOpTempMapPut", 0);
        params.put("sleepInRemoveOpTempMapRemove", 0);
        params.put("sleepInReconCompute", 0);
//        params.put("sleepInPutOpCompute", 0);
        params.put("sleepInRemoveOpCompute", 0);
        params.put("firstPagesIsEmpty", false);
        params.put("lastPagesIsEmpty", false);
        params.put("middlePagesIsEmpty", false);

        main();
    }

//    @Test
    public void main() throws Exception {
        int pagesCount = (Integer) params.get("pagesCount");
//        int maxKey = partCount * partCount;
        int maxKey = (Integer) params.get("keysCount");
        int keysPerPage = maxKey / pagesCount;

        Part part = new Part(pagesCount, keysPerPage);

        part.size.set(1_000_000);

//        Random rnd = new Random();

        for (int i = 0; i < maxKey; i += 2) {
//            if (!(i > 3500 && i < 4500) && !(i > 6500 && i < 8500))
//            if ((i > 1000) && (i < 8500))

            Boolean firstEmpty = (Boolean)params.get("firstPagesIsEmpty");
            Boolean middleEmpty = (Boolean)params.get("lastPagesIsEmpty");
            Boolean lastEmpty = (Boolean)params.get("middlePagesIsEmpty");
            if ((firstEmpty && i < keysPerPage + keysPerPage) ||
//                (firstEmpty && i > keysPerPage + keysPerPage && pagesCount >= 8) ||
                (lastEmpty && i > maxKey - keysPerPage + keysPerPage))
                continue;
            part.put(i);
            System.out.println("preload put " + i);
        }

        AtomicBoolean doLoad = new AtomicBoolean(true);

        List<IgniteInternalFuture> loadFuts = new ArrayList<>();

        loadFuts.add(GridTestUtils.runAsync(() -> {
                System.out.println("qvsdhntsd loadFut start");

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int sleep = (Integer)params.get("sleepInLoadFuture1");

                while (doLoad.get()) {
                    int i = rnd.nextInt(maxKey);
//                    if (!(i % 2 == 0))
                        part.put(i);

                    if (sleep > 0)
                        doSleep(sleep);

                    i = rnd.nextInt(maxKey);

//                    if (!(i % 2 == 0))
                        part.remove(i);

                    if (sleep > 0)
                        doSleep(sleep);
                }
            },
            "qwerthread0")
        );

        if ((Integer)params.get("loadTreads") > 1) {
            loadFuts.add(GridTestUtils.runAsync(() -> {
                    System.out.println("qvsdhntsd loadFut start");

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int sleep = (Integer) params.get("sleepInLoadFuture2");

                    while (doLoad.get()) {
                        int i = rnd.nextInt(maxKey);
//                        if (!(i % 2 == 0))
                            part.put(i);

                        if (sleep > 0)
                            doSleep(sleep);

                        i = rnd.nextInt(maxKey);
//                        if (!(i % 2 == 0))
                            part.remove(i);

                        if (sleep > 0)
                            doSleep(sleep);
                    }
                },
                "qwerthread1")
            );
        }

        if ((Integer)params.get("loadTreads") > 2) {
            loadFuts.add(GridTestUtils.runAsync(() -> {
                    System.out.println("qvsdhntsd loadFut start");

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int sleep = (Integer) params.get("sleepInLoadFuture2");

                    while (doLoad.get()) {
                        int i = rnd.nextInt(maxKey);
                        part.put(i);

                        if (sleep > 0)
                            doSleep(sleep);

                        i = rnd.nextInt(maxKey);
//                if (i % 2 == 0)
                        part.remove(i);

                        if (sleep > 0)
                            doSleep(sleep);
                    }
                },
                "qwerthread2")
            );
        }

        if ((Integer)params.get("loadTreads") > 3) {
            loadFuts.add(GridTestUtils.runAsync(() -> {
                    System.out.println("qvsdhntsd loadFut start");

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int sleep = (Integer) params.get("sleepInLoadFuture2");

                    while (doLoad.get()) {
                        int i = rnd.nextInt(maxKey);
                        part.put(i);

                        if (sleep > 0)
                            doSleep(sleep);

                        i = rnd.nextInt(maxKey);
//                if (i % 2 == 0)
                        part.remove(i);

                        if (sleep > 0)
                            doSleep(sleep);
                    }
                },
                "qwerthread3")
            );
        }

//        doSleep(30);

        IgniteInternalFuture reconFut = GridTestUtils.runAsync(() -> {
            doRecon(part);

            doLoad.set(false);
            }
        );

        reconFut.get();

        for (IgniteInternalFuture fut : loadFuts) {
            fut.get();
        }

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

                        int sleep0 = (Integer)params.get("sleepInReconCompute");

                        part.tempMap.computeIfPresent(entry.getKey(), (k, v) -> {
                            if (k != null && v!= null && entry.getKey() < firstKey.get()) {
//                                if (!part.removesInProgress.contains(entry.getKey()))
                                if (sleep0 > 0)
                                    doSleep(sleep0);

                                part.reconSize.addAndGet(entry.getValue());

//                                tempMapIter.remove();

                                System.out.println("RECON tempMap iter add delta and remove key: "
                                    + "key " + k
                                    + ", part.reconSize() " + part.reconSize.get());

                                return null;
                            }
                            else {
//                                tempMapIter.remove();
                                if (sleep0 > 0)
                                    doSleep(sleep0);

                                System.out.println("RECON tempMap iter remove key: "
                                    + "key " + k
                                    + ", part.reconSize() " + part.reconSize.get());

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

                    int sleep = (Integer)params.get("sleepInReconTempMapPut");

                    for (Integer key : page.keys) {
                        System.out.println("in recon added key to tempMap: key " + key + " delta " + 1);
                        part.tempMap.put(key, +1);

                        if (sleep > 0)
                            doSleep(sleep);
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

        volatile Map<Integer, Integer> tempMap = new ConcurrentHashMap<>()/*ConcurrentSkipListMap<>()*/;
        volatile Set<Integer> removesInProgress = new ConcurrentSkipListSet<>();


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
                            Integer sleep = (Integer)params.get("sleepInPutOpTempMapPut");

                            if (sleep > 0)
                                doSleep(sleep);

                            if (borderKey != null && key <= borderKey) {
                                if (sleep > 0)
                                    doSleep(sleep);

                                reconSize.incrementAndGet();
                                System.out.println("in PUT after increment reconSize: key " + key + " reconSize " + reconSize.get());
                            }
                            else {
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
                                if (sleep > 0)
                                    doSleep(sleep);

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

                String strThread = Thread.currentThread().getName();

                System.out.println("pageNumber/pages " + pageNumber + "/" + pages.size() + " " + strThread);

                Page page = pages.get(pageNumber);

                page.pageLock.writeLock().lock();

                try {
                    if (page.keys.contains(key)) {
                        page.keys.remove(key);

                        System.out.println("remove key " + key + " " + strThread);

//                    if (reconInProgress) {
//                        if (key < borderKey)
//                            reconSize.incrementAndGet();
//                        else if (tempMap.get(key) != null && tempMap.get(key) != -1)
//                            tempMap.remove(key);
//                        else
//                            tempMap.put(key, -1);
//                    }

                        if (reconInProgress) {
                            Integer sleep = (Integer)params.get("sleepInRemoveOpTempMapRemove");
//                            tempMap.remove(key);

                            if (sleep > 0)
                                doSleep(sleep);

//                            removesInProgress.add(key);

                            if (borderKey != null && key <= borderKey) {
                                if (sleep > 0)
                                    doSleep(sleep);

                                reconSize.decrementAndGet();
                                System.out.println("in REMOVE after decrement reconSize: key " + key + " reconSize " + reconSize.get() + " " + strThread);
                            }
                            else {
                                if (sleep > 0)
                                    doSleep(sleep);

                                Integer sleep0 = (Integer)params.get("sleepInRemoveOpCompute");

                                System.out.println("in REMOVE before compute " + strThread);

                                tempMap.compute(key, (k, v) -> {
//                                    System.out.println("k: " + k + ", v: " + v);
                                    if (k != null && v != null /*&& v.equals(1)*/) {
                                        if (sleep0 > 0)
                                            doSleep(sleep0);

                                        System.out.println("in REMOVE compute: remove key " + key + " " + strThread);

                                        return null;
                                    }
                                    else if (v == null && borderKey != null && key <= borderKey) {
//                                        doSleep(2);
                                        if (sleep0 > 0)
                                            doSleep(sleep0);
                                        reconSize.decrementAndGet();
                                        System.out.println("in REMOVE compute: decrement and remove key " + key + " " + strThread);

                                        return null;
                                    }
                                    else {
                                        if (sleep0 > 0)
                                            doSleep(sleep0);
                                        System.out.println("in REMOVE compute: do nothing key " + key + " " + strThread);
                                        return v;
                                    }
                                });

//                                tempMap.computeIfPresent(key, (k, v) -> {
//                                    System.out.println("in REMOVE start compute " + strThread);
//
//                                    return null;
//                                });



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

//                            removesInProgress.remove(key);
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
