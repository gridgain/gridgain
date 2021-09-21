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

public class PartitionReconciliationFixPartitionSizesStressParameterizedTest extends PartitionReconciliationFixPartitionSizesStressAbstractParameterizedTest{
    @Test
    public void test() throws Exception {
        CacheWriteSynchronizationMode syncMode = rnd.nextBoolean() ? FULL_SYNC : PRIMARY_SYNC;

        Set<ReconciliationType> reconciliationTypes = new HashSet<>();

        reconciliationTypes.add(CACHE_SIZE_CONSISTENCY);

        if (rnd.nextBoolean())
            reconciliationTypes.add(DATA_CONSISTENCY);

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
