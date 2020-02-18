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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class CachePartitionLostWhileClearingTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 64;

    /** */
    private PartitionLossPolicy plc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setWalSegmentSize(4 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(100L * 1024 * 1024))
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setCacheMode(PARTITIONED).
            setPartitionLossPolicy(plc).
            setBackups(1).
            setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)));

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnCrd() throws Exception {
        doTestPartitionLostWhileClearing(2, PartitionLossPolicy.IGNORE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnFullMessage() throws Exception {
        doTestPartitionLostWhileClearing(3, PartitionLossPolicy.IGNORE);
    }

    /**
     *
     */
    @Test
    public void testPartitionConsistencyOnSupplierRestart_PolicyUnsafe() throws Exception {
        doTestPartitionConsistencyOnSupplierRestart(PartitionLossPolicy.IGNORE);
    }

    /**
     *
     */
    @Test
    public void testPartitionConsistencyOnSupplierRestart_PolicySafe() throws Exception {
        doTestPartitionConsistencyOnSupplierRestart(PartitionLossPolicy.READ_WRITE_SAFE);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestPartitionConsistencyOnSupplierRestart(PartitionLossPolicy plc) throws Exception {
        this.plc = plc;

        try {
            int entryCnt = PARTS_CNT * 200;

            IgniteEx crd = (IgniteEx)startGridsMultiThreaded(2);

            crd.cluster().active(true);

            IgniteCache<Integer, String> cache0 = crd.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < entryCnt / 2; i++)
                cache0.put(i, String.valueOf(i));

            forceCheckpoint();

            stopGrid(1);

            for (int i = entryCnt / 2; i < entryCnt; i++)
                cache0.put(i, String.valueOf(i));

            final IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1));

            final TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

            spi1.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtPartitionDemandMessage) {
                        GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                        return msg0.groupId() == CU.cacheId(DEFAULT_CACHE_NAME);
                    }

                    return false;
                }
            });

            startGrid(cfg);

            spi1.waitForBlocked(1);

            // Cancellation of rebalancing by left supplier.
            stopGrid(0);

            awaitPartitionMapExchange();

            startGrid(0);

            spi1.stopBlock();

            awaitPartitionMapExchange();

            assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test scenario: supplier left during rebalancing leaving partition in LOST state because only
     * one owner remains in partially rebalanced state.
     * <p>
     * Expected result: no assertions are triggered.
     *
     * @param cnt Nodes count.
     * @param plc Policy.
     *
     * @throws Exception If failed.
     */
    private void doTestPartitionLostWhileClearing(int cnt, PartitionLossPolicy plc) throws Exception {
        this.plc = plc;

        try {
            IgniteEx crd = startGrids(cnt);

            crd.cluster().active(true);

            int partId = -1;
            int idx0 = 0;
            int idx1 = 1;

            for (int p = 0; p < PARTS_CNT; p++) {
                List<ClusterNode> nodes = new ArrayList<>(crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(p));

                if (grid(nodes.get(0)) == grid(idx0) && grid(nodes.get(1)) == grid(idx1)) {
                    partId = p;

                    break;
                }
            }

            assertTrue(partId >= 0);

            load(grid(idx0), DEFAULT_CACHE_NAME, partId, 10_000, 0);

            stopGrid(idx1);

            load(grid(idx0), DEFAULT_CACHE_NAME, partId, 10, 10_000);

            IgniteEx g1 = startGrid(idx1);

            stopGrid(idx0); // Restart supplier in the middle of clearing.

            awaitPartitionMapExchange();

            // Expecting partition in OWNING state.
            assertNotNull(counter(partId, DEFAULT_CACHE_NAME, g1.name()));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     * @param partId Partition id.
     * @param cnt Count.
     * @param skip Skip.
     */
    private void load(IgniteEx ignite, String cache, int partId, int cnt, int skip) {
        List<Integer> keys = partitionKeys(ignite.cache(cache), partId, cnt, skip);

        try (IgniteDataStreamer<Object, Object> s = ignite.dataStreamer(cache)) {
            for (Integer key : keys)
                s.addData(key, key);
        }
    }
}
