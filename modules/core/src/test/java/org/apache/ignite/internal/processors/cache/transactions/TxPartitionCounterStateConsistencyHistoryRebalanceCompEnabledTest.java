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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.wal.record.RecordUtils;
import org.junit.Test;

/**
 * Test partitions consistency in various scenarios when all rebalance is historical and compaction is enabled.
 */
public class TxPartitionCounterStateConsistencyHistoryRebalanceCompEnabledTest extends TxPartitionCounterStateConsistencyHistoryRebalanceTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

//        cfg.getDataStorageConfiguration().setWalCompactionEnabled(false);
        cfg.getDataStorageConfiguration().setWalCompactionEnabled(true);

        cfg.getDataStorageConfiguration().setWalSegments(100);

//        cfg.setStripedPoolSize(64);

        return cfg;
    }

    @Test
    //@WithSystemProperty(key = "IGNITE_RECOVERY_SEMAPHORE_PERMITS", value = "512")
    @Override public void testPartitionConsistencyWithBackupRestart_ChangeBLT() throws Exception {
        super.testPartitionConsistencyWithBackupRestart_ChangeBLT();
    }

    @Test
    public void testConcurrentLogWal() throws Exception {
        IgniteEx n = startGrid(0);

        n.cluster().state(ClusterState.ACTIVE);

        FileWriteAheadLogManager wal = walMgr(n);

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Long> future0 = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                int recordTypeLength = WALRecord.RecordType.values().length;

                for (int i = 0; i < 1_000 && !stop.get(); i++) {
                    int recordTypeIndex = i % recordTypeLength;

                    WALRecord.RecordType recordType = WALRecord.RecordType.fromIndex(recordTypeIndex);

                    switch (recordType) {
                        case HEADER_RECORD:
                        case BTREE_PAGE_INNER_REPLACE:
                        case BTREE_FORWARD_PAGE_SPLIT:
                        case BTREE_PAGE_MERGE:
                        case SWITCH_SEGMENT_RECORD:
                        case RESERVED:
                        case ENCRYPTED_RECORD:
                        case ENCRYPTED_DATA_RECORD:
                        case MASTER_KEY_CHANGE_RECORD:
                        case OUT_OF_ORDER_UPDATE:
                        case ENCRYPTED_RECORD_V2:
                        case ENCRYPTED_DATA_RECORD_V2:
                        case ENCRYPTED_DATA_RECORD_V3:
                        case ENCRYPTED_OUT_OF_ORDER_UPDATE:
                            recordType = WALRecord.RecordType.TX_RECORD;
                            break;
                    }

                    wal.log(RecordUtils.buildWalRecord(recordType));
                }

                return null;
            } catch (Throwable t) {
                stop.compareAndSet(false, true);

                throw t;
            }
        }, Runtime.getRuntime().availableProcessors() - 1, "put-wal");

        IgniteInternalFuture future1 = GridTestUtils.runAsync(() -> {
            try {
                for (int i = 0; i < 1_000 && !stop.get(); i++)
                    wal.log(RecordUtils.buildTxRecord());
            } catch (Throwable t) {
                stop.compareAndSet(false, true);

                throw t;
            }
        });

        future0.get(getTestTimeout());
        future1.get(getTestTimeout());
    }
}
