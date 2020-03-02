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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;

/**
 * Test scenario: supplier is left during rebalancing leaving partition in OWNING state (but actually LOST) because only
 * one owner left. <p> Expected result: no assertions are triggered.
 */
public class CachePartitionLossWithPersistenceTest extends GridCommonAbstractTest {
    public static final int WAIT = 2_000;

    /** */
    private static final int PARTS_CNT = 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureDetectionTimeout(100000000L);
        cfg.setClientFailureDetectionTimeout(100000000L);

        cfg.setConsistentId(igniteInstanceName);
        cfg.setClientMode(igniteInstanceName.startsWith("client"));

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
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setPartitionLossPolicy(READ_WRITE_SAFE).
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

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void testPartitionConsistencyOnSupplierRestart() throws Exception {
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

        // Cancellation of rebalancing because a supplier has left.
        stopGrid(0);

        awaitPartitionMapExchange();

        final Collection<Integer> lostParts = grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(PARTS_CNT, lostParts.size());

        final IgniteEx g0 = startGrid(0);

        final Collection<Integer> lostParts2 = g0.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(PARTS_CNT, lostParts2.size());

        spi1.stopBlock();

        g0.resetLostPartitions(Collections.singletonList(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    @Test
    public void testDataLost() throws Exception {
        IgniteEx crd = startGrids(3);
        crd.cluster().active(true);

        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        grid(2).close();

        awaitPartitionMapExchange();

        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        //logCacheSize("DBG: server_3 left");

        final int keys = 1_000;

        load(crd, DEFAULT_CACHE_NAME, IntStream.range(0, keys).boxed());

        //logCacheSize("DBG: 100k loaded");

        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        grid(1).close();
        doSleep(WAIT);

        //logCacheSize("DBG: server_2 left");
        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        startGrid(2);

        //logCacheSize("DBG: server_2 ret");
        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        startGrid(1);

        //logCacheSize("DBG: server_1 ret");
        //printPartitionState(DEFAULT_CACHE_NAME, 0);

        //doSleep(WAIT + 5_000);
        //logCacheSize("DBG: all servers are up");

        crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        //doSleep(WAIT + 5_000);

        awaitPartitionMapExchange();

        //logCacheSize("DBG: after reset loss parts");

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     */
    @Test
    public void testDataLost2() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().active(true);

        startGrid(2);
        resetBaselineTopology();
        awaitPartitionMapExchange();

        // Find a lost partition which is primary for g1.
        int part = IntStream.range(0, PARTS_CNT).boxed().filter(new Predicate<Integer>() {
            @Override public boolean test(Integer p) {
                final List<ClusterNode> nodes = new ArrayList<>(crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(p));

                return nodes.get(0).equals(grid(1).localNode()) && nodes.get(1).equals(grid(2).localNode());
            }
        }).findFirst().orElseThrow(AssertionError::new);

        stopGrid(1);

        final IgniteInternalCache<Object, Object> cachex = crd.cachex(DEFAULT_CACHE_NAME);

        cachex.put(part, 0);

        stopGrid(2);

        final Collection<Integer> lostParts = crd.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(PARTS_CNT, cachex.context().topology().localPartitions().size() + lostParts.size());

        assertTrue(lostParts.contains(part));

        final IgniteEx g1 = startGrid(1);

        final Collection<Integer> g1LostParts = g1.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(lostParts, g1LostParts);

        // Block rebalancing from g2 to g1 to ensure a primary partition is in moving state.
        TestRecordingCommunicationSpi.spi(g1).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return msg instanceof GridDhtPartitionDemandMessage;
            }
        });

        final IgniteEx g2 = startGrid(2);

        final Collection<Integer> g2LostParts = g2.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(lostParts, g2LostParts);

        crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        TestRecordingCommunicationSpi.spi(g1).waitForBlocked();

        // Try put to moving partition. Due to forced reassignment g2 should be a primary for {@code part}.
        cachex.put(part, 1);

        TestRecordingCommunicationSpi.spi(g1).stopBlock();

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    private void logCacheSize(String prefix) {
        final IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        String msg = prefix + " size=" + cache.size();
        msg += ", partsLost=" + cache.lostPartitions();

        log.info(msg);
    }
}
