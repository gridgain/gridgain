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

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * Test partitions consistency in various scenarios when all rebalance is historical and compaction is enabled.
 */
public class TxPartitionCounterStateConsistencyHistoryRebalanceCompEnabledTest extends TxPartitionCounterStateConsistencyHistoryRebalanceTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setWalCompactionEnabled(true);

        cfg.getDataStorageConfiguration().setWalSegments(200);

        cfg.getDataStorageConfiguration().setMaxWalArchiveSize(DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE);

        cfg.getDataStorageConfiguration().setWalArchivePath(cfg.getDataStorageConfiguration().getWalPath());

        return cfg;
    }

    @Test
    @Override public void testPartitionConsistencyWithBackupRestart_ChangeBLT() throws Exception {
        super.testPartitionConsistencyWithBackupRestart_ChangeBLT();
    }
}
