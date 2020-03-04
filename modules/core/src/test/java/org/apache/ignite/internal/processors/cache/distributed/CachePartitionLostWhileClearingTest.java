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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test scenario: last supplier has left while a partition on demander is cleared before sending first demand request.
 * <p>
 * Expected result: no assertions are triggered.
 */
public class CachePartitionLostWhileClearingTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 64;

    /** */
    private PartitionLossPolicy lossPlc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
            setBackups(1).
            setPartitionLossPolicy(lossPlc).
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

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnCrd() throws Exception {
        lossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        doTestPartitionLostWhileClearing(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnFullMessage() throws Exception {
        lossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        doTestPartitionLostWhileClearing(3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnCrd_Unsafe() throws Exception {
        lossPlc = PartitionLossPolicy.IGNORE; // In persistent mode READ_WRITE_SAFE is used instead of IGNORE.

        doTestPartitionLostWhileClearing(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnFullMessage_Unsafe() throws Exception {
        lossPlc = PartitionLossPolicy.IGNORE; // In persistent mode READ_WRITE_SAFE is used instead of IGNORE.

        doTestPartitionLostWhileClearing(3);
    }

    /**
     * @param cnt Nodes count.
     * @throws Exception If failed.
     */
    private void doTestPartitionLostWhileClearing(int cnt) throws Exception {
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

        final int keysCnt = 10_010;

        List<Integer> keys = partitionKeys(grid(idx0).cache(DEFAULT_CACHE_NAME), partId, keysCnt, 0);

        load(grid(idx0), DEFAULT_CACHE_NAME, keys.subList(0, keysCnt - 10));

        stopGrid(idx1);

        load(grid(idx0), DEFAULT_CACHE_NAME, keys.subList(keysCnt - 10, keysCnt));

        IgniteEx g1 = startGrid(idx1);

        stopGrid(idx0); // Stop supplier in the middle of clearing.

        final GridDhtLocalPartition part = g1.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(partId);

        assertEquals(GridDhtPartitionState.LOST, part.state());

        // TODO fixme own a clearing partition !!! ???
        g1.resetLostPartitions(Collections.singletonList(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();

        // Expecting partition in OWNING state.
        final PartitionUpdateCounter cntr = counter(partId, DEFAULT_CACHE_NAME, g1.name());
        assertNotNull(cntr);

        // Counter must be reset.
        assertEquals(0, cntr.get());

        // Test puts concurrently with clearing after reset are not lost.
        g1.cache(DEFAULT_CACHE_NAME).putAll(keys.stream().collect(Collectors.toMap(k -> k, v -> -1)));

        g1.context().cache().context().evict().awaitFinishAll();

        for (Integer key : keys)
            assertEquals("key=" + key.toString(), -1, g1.cache(DEFAULT_CACHE_NAME).get(key));
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     * @param partId Partition id.
     * @param cnt Count.
     * @param skip Skip.
     */
    private void load(IgniteEx ignite, String cache, List<Integer> keys) {
        try (IgniteDataStreamer<Object, Object> s = ignite.dataStreamer(cache)) {
            for (Integer key : keys)
                s.addData(key, key);
        }
    }
}
