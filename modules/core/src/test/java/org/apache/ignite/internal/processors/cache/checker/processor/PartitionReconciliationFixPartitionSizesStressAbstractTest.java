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
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Tests partition reconciliation of sizes with various cache configurations.
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationFixPartitionSizesStressAbstractTest extends PartitionReconciliationAbstractTest {
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
    public CacheWriteSynchronizationMode rndMode;

    /** */
    public Random rnd = new Random();

    /** */
    public AtomicReference<ReconciliationResult> reconResult = new AtomicReference<>();

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
        super.beforeTest();

        rndMode = rnd.nextBoolean() ? FULL_SYNC : PRIMARY_SYNC;

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    protected CacheConfiguration getCacheConfig(String name, CacheAtomicityMode cacheAtomicityMode, CacheMode cacheMode, int backupCount, int partCount, String cacheGroup) {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(name);
        if (cacheGroup != null)
            ccfg.setGroupName(cacheGroup);

        log.info(">>> CacheWriteSynchronizationMode: " + rndMode);

        ccfg.setWriteSynchronizationMode(rndMode);

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

        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 0, 10, "testCacheGroup1", 100, 1, 8, false});

        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 32, null, 100, 1, 8, false});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, null, 100, 1, 8, false});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 1, null, 10, 3, 8, false});

        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 1, 12, null, 10, 3, 8, false});

        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 14, "testCacheGroup1", 10, 3, 8, false});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 17, null, 10, 3, 8, false});

        params.add(new Object[] {1, 0, 3000, ATOMIC,PARTITIONED, 0, 1, null, 1, 10, 8, false});

        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 0, 10, null, 1, 10, 8, false});

        params.add(new Object[] {4, 0, 3000, TRANSACTIONAL, PARTITIONED, 2, 12, null, 1, 10, 8, false});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, "testCacheGroup1", 1, 10, 8, false});

        params.add(new Object[] {1, 0, 3000, ATOMIC, PARTITIONED, 0, 1, null, 1, 10, 8, true});

        params.add(new Object[] {3, 0, 3000, ATOMIC, PARTITIONED, 0, 10, null, 1, 10, 8, true});

        params.add(new Object[] {4, 0, 1000, TRANSACTIONAL, PARTITIONED, 2, 12, null, 1, 10, 8, true});

        params.add(new Object[] {4, 0, 10000, ATOMIC, REPLICATED, 0, 12, "testCacheGroup1", 1, 10, 8, true});

        return params;
    }
}
