/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PENDING_TX_TRACKER_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@SystemPropertiesList({
    @WithSystemProperty(key = IGNITE_WAL_LOG_TX_RECORDS, value = "false"),
    @WithSystemProperty(key = IGNITE_PENDING_TX_TRACKER_ENABLED, value = "true")
})
public class TxLocalTrackerWithoutTxRecordLoggingTest extends GridCommonAbstractTest {
    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            )
            .setCacheConfiguration(
                new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).setAtomicityMode(TRANSACTIONAL)
            );
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrid(2);

        ignite.cluster().active(true);

        LocalPendingTransactionsTracker tracker = ignite.context().cache().context().tm().pendingTxsTracker();

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        tracker.writeLockState();

        try {
            tracker.startTrackingCommitted();
        }
        finally {
            tracker.writeUnlockState();
        }

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(1, 1);

            tx.commit();
        }

        tracker.writeLockState();

        try {
            TrackCommittedResult res = tracker.stopTrackingCommitted();

            assertEquals(1, res.committedTxs().size());
        }
        finally {
            tracker.writeUnlockState();
        }
    }
}