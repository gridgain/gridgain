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

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;

/**
 * Test partitions consistency in various scenarios when all rebalance is historical and compaction is enabled.
 */
public class TxPartitionCounterStateConsistencyHistoryRebalanceCompactionEnabledTest extends TxPartitionCounterStateConsistencyHistoryRebalanceTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(2 * 1024 * 1024)
            .setCheckpointFrequency(Integer.MAX_VALUE)
            .setWalCompactionEnabled(true)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dbCfg);

        return cfg;
    }
}
