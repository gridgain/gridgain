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
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.cache.verify.ReconciliationType;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.processors.cache.verify.ReconciliationType.CACHE_SIZE_CONSISTENCY;
import static org.apache.ignite.internal.processors.cache.verify.ReconciliationType.DATA_CONSISTENCY;

/**
 * Tests partition reconciliation of sizes with various cache configurations.
 */
public class PartitionReconciliationFixPartitionSizesStressParameterizedTest extends PartitionReconciliationFixPartitionSizesStressAbstractParameterizedTest {
    /** */
    @Test
    public void test() throws Exception {
        CacheWriteSynchronizationMode syncMode = rnd.nextBoolean() ? FULL_SYNC : PRIMARY_SYNC;

        Set<ReconciliationType> reconciliationTypes = new HashSet<>();

        reconciliationTypes.add(CACHE_SIZE_CONSISTENCY);

//        if (rnd.nextBoolean())
//            reconciliationTypes.add(DATA_CONSISTENCY);

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

        test(nodesCnt, startKey, endKey, cacheAtomicityMode, cacheMode, syncMode, backupCnt, partCnt, cacheGrp,
            reconBatchSize, reconParallelism, loadThreadsCnt, reconciliationTypes, cacheClearOp, cacheCount);
    }
}
