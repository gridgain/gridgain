/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class IgniteWalRebalanceLoggingTest extends GridCommonAbstractTest {
    /** */
    public static final int CHECKPOINT_FREQUENCY = 100;

    /** Test logger. */
    private final ListeningTestLogger srvLog = new ListeningTestLogger(false, log);

    /** */
    public static final int KEYS_LOW_BORDER = 100;

    /** */
    public static final int KEYS_UPPER_BORDER = 200;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(srvLog);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        storageCfg.setWalMode(WALMode.LOG_ONLY).setWalCompactionEnabled(true).setWalCompactionLevel(1);

        storageCfg.setCheckpointFrequency(CHECKPOINT_FREQUENCY);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /** {@inheritDoc}*/
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc}*/
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void testFollowingPartitionsWereReservedForPotentialHistoryRebalanceMsgTest() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "1");

        checkFollowingPartitionsWereReservedForPotentialHistoryRebalanceMsg(
            "Following partitions were reserved for potential history " +
            "rebalance [groupId=1813188848 parts=[0-7], groupId=1813188847 parts=[0-7]]");
    }

    @Test
    public void testFollowingPpartitionsWereReservedForPotentialHistoryRebalanceNoPartitionsMsgTest() throws Exception {
        checkFollowingPartitionsWereReservedForPotentialHistoryRebalanceMsg(
            "Following partitions were reserved for potential history rebalance []");
    }

    private void checkFollowingPartitionsWereReservedForPotentialHistoryRebalanceMsg(String expectedLogMsg)
        throws Exception {
        startGridsMultiThreaded(2).active(true);

        IgniteCache<Integer, String> cache1 = createCache("cache1", "cache_group1");
        IgniteCache<Integer, String> cache2 = createCache("cache2", "cache_group2");

        for (int i = 0; i < KEYS_LOW_BORDER; i++) {
            cache1.put(i, "abc" + i);
            cache2.put(i, "abc" + i);
        }

        stopGrid(1);

        awaitPartitionMapExchange();

        for (int i = KEYS_LOW_BORDER; i < KEYS_UPPER_BORDER; i++) {
            cache1.put(i, "abc" + i);
            cache2.put(i, "abc" + i);
        }

        Thread.sleep(CHECKPOINT_FREQUENCY * 2);

        startGrid(1);

        LogListener lsnr = LogListener.matches(expectedLogMsg).times(2).build();

        srvLog.registerListener(lsnr);

        awaitPartitionMapExchange();

        assertTrue(lsnr.check());
    }

    private IgniteCache<Integer, String> createCache(String cacheName, String cacheGrpName) {
        return ignite(0).createCache(
            new CacheConfiguration<Integer, String>(cacheName).
                setAffinity(new RendezvousAffinityFunction().setPartitions(8)).
                setGroupName("Group1").setGroupName(cacheGrpName).setBackups(1));
    }
}
