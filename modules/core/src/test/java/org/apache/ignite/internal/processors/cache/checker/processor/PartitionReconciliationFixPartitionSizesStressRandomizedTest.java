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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.cache.verify.ReconciliationType;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.verify.ReconciliationType.CACHE_SIZE_CONSISTENCY;
import static org.apache.ignite.internal.processors.cache.verify.ReconciliationType.DATA_CONSISTENCY;

/**
 * Tests partition reconciliation of sizes with various cache configurations.
 */
public class PartitionReconciliationFixPartitionSizesStressRandomizedTest extends PartitionReconciliationFixPartitionSizesStressAbstractTest {
    /** */
    public int nodesCnt;

    /** */
    public int startKey;

    /** */
    public int endKey;

    /** */
    public CacheAtomicityMode cacheAtomicityMode;

    /** */
    public CacheMode cacheMode;

    /** */
    public int backupCnt;

    /** */
    public int partCnt;

    /** */
    public String cacheGrp;

    /** */
    public int reconBatchSize;

    /** */
    public int reconParallelism;

    /** */
    public int loadThreadsCnt;

    /** */
    public boolean cacheClearOp;

    /** */
    public int cacheCount;

    /** */
    CacheWriteSynchronizationMode syncMode;

    /** */
    Set<ReconciliationType> reconciliationTypes;

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

    /** */
    @Test
    public void test() throws Exception {
        setParameters();

        test(nodesCnt, startKey, endKey, cacheAtomicityMode, cacheMode, syncMode, backupCnt, partCnt, cacheGrp,
            reconBatchSize, reconParallelism, loadThreadsCnt, reconciliationTypes, cacheClearOp, cacheCount);
    }

    /** */
    private void setParameters() {
        switch (rnd.nextInt(3)) {
            case 0:
                pageSize = 1024;
                break;
            case 1:
                pageSize = 2048;
                break;
            case 2:
                pageSize = 4096;
                break;
        }

        nodesCnt = 1 + rnd.nextInt(5);

        startKey = 0;

        endKey = rnd.nextInt(10000);

        cacheAtomicityMode = rnd.nextBoolean() ? ATOMIC : TRANSACTIONAL;

        cacheMode = rnd.nextBoolean() ? REPLICATED : PARTITIONED;

        syncMode = /*rnd.nextBoolean() ? */FULL_SYNC/* : PRIMARY_SYNC*/;

        backupCnt = nodesCnt - 1;

        partCnt = 4 + rnd.nextInt(10);

        cacheGrp = rnd.nextBoolean() ? null : "cacheGroup1";

        reconBatchSize = 1 + rnd.nextInt(200);

        reconParallelism = 2 + rnd.nextInt(200);

        loadThreadsCnt = 8;

        reconciliationTypes = new HashSet<>();
        reconciliationTypes.add(CACHE_SIZE_CONSISTENCY);
        if (rnd.nextBoolean())
            reconciliationTypes.add(DATA_CONSISTENCY);

        cacheClearOp = false;//rnd.nextBoolean();

        cacheCount = 1 + rnd.nextInt(3);
    }
}
