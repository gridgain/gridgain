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
import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests partition reconciliation of sizes with various cache configurations.
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationFixPartitionSizesStressAbstractParameterizedTest extends PartitionReconciliationFixPartitionSizesStressAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public int nodesCnt;

    /** */
    @Parameterized.Parameter(1)
    public int startKey;

    /** */
    @Parameterized.Parameter(2)
    public int endKey;

    /** */
    @Parameterized.Parameter(3)
    public CacheAtomicityMode cacheAtomicityMode;

    /** */
    @Parameterized.Parameter(4)
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(5)
    public int backupCnt;

    /** */
    @Parameterized.Parameter(6)
    public int partCnt;

    /** */
    @Parameterized.Parameter(7)
    public String cacheGrp;

    /** */
    @Parameterized.Parameter(8)
    public int reconBatchSize;

    /** */
    @Parameterized.Parameter(9)
    public int reconParallelism;

    /** */
    @Parameterized.Parameter(10)
    public int loadThreadsCnt;

    /** */
    @Parameterized.Parameter(11)
    public boolean cacheClearOp;

    /** */
    @Parameterized.Parameter(12)
    public int cacheCount;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     *
     */
    @Parameterized.Parameters(name = "nodesCnt = {0}, startKey = {1}, endKey = {2}, cacheAtomicityMode = {3}, cacheMode = {4}, " +
        "backupCount = {5}, partCount = {6}, cacheGroup = {7}, batchSize = {8}, reconParallelism = {9}, loadThreads = {10}, cacheClear = {11}, cacheCount = {12}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[] {1, 0, 1000, ATOMIC, PARTITIONED, 0, 5, null, 100, 1, 8, false, 1});

        params.add(new Object[] {5, 0, 4000, ATOMIC, PARTITIONED, 0, 10, "testCacheGroup1", 100, 1, 8, false, 2});

        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 32, null, 100, 1, 8, false, 3});

        params.add(new Object[] {4, 0, 15000, ATOMIC, REPLICATED, 0, 12, null, 100, 1, 8, false, 2});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 1, "testCacheGroup1", 10, 3, 8, false, 3});

        params.add(new Object[] {5, 0, 5000, ATOMIC, PARTITIONED, 2, 12, null, 10, 4, 8, false, 2});

        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 14, "testCacheGroup1", 10, 5, 8, false, 2});

        params.add(new Object[] {4, 0, 12000, ATOMIC, REPLICATED, 0, 17, "testCacheGroup1", 10, 6, 8, false, 2});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 18, null, 1, 1, 8, false, 2});

        params.add(new Object[] {4, 0, 3000, ATOMIC, PARTITIONED, 2, 10, null, 2, 20, 8, false, 2});

        params.add(new Object[] {4, 0, 4000, TRANSACTIONAL, PARTITIONED, 2, 12, null, 1, 30, 8, false, 3});

        params.add(new Object[] {4, 0, 8000, ATOMIC, REPLICATED, 0, 12, "testCacheGroup1", 1, 40, 8, false, 1});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 9, "testCacheGroup1", 1, 10, 8, false, 1});

        params.add(new Object[] {5, 0, 7000, ATOMIC, PARTITIONED, 3, 10, null, 9, 10, 8, false, 2});

        params.add(new Object[] {4, 0, 1000, TRANSACTIONAL, PARTITIONED, 2, 12, null, 1, 10, 8, false, 2});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, "testCacheGroup1", 100, 10, 8, false, 2});

        params.add(new Object[] {1, 0, 10000, ATOMIC, PARTITIONED, 0, 1, null, 100, 1, 1, true, 1});

        return params;
    }
}
