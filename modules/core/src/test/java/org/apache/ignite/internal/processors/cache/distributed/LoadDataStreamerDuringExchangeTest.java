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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks correctness loading data during exchange through Data streamer which cannot override entries.
 */
public class LoadDataStreamerDuringExchangeTest extends GridCommonAbstractTest {
    /** Count of rows which will be load to cache. */
    public static final int LOAD_LOOP = 100;

    /** True when persistence enabled false mean use only in memory storage. */
    public boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(persistenceEnabled)))
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 16))
                .setBackups(1));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * This test loads data during clearing partition before rebalance started in persistent cache. After the loading it
     * checks that all entries presenting in cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWithPersistence() throws Exception {
        persistenceEnabled = true;

        IgniteEx ignite0 = startGrids(2);

        ignite0.cluster().active(true);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(ignite0);

        List<Integer> keys = movingKeysAfterJoin(ignite0, DEFAULT_CACHE_NAME, LOAD_LOOP, null, getTestIgniteInstanceName(2));

        IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME);

        for (int i = 0; i < LOAD_LOOP / 2; i++)
            streamer.addData(keys.get(i), String.valueOf(keys.get(i)));

        streamer.flush();

        IgniteEx ignite2 = startGrid(2);

        spi0.blockMessages(DataStreamerRequest.class, getTestIgniteInstanceName(1));
        spi0.blockMessages(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(2));

        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(ignite2);

        spi2.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage sm = (GridDhtPartitionsSingleMessage)msg;

                if (sm.exchangeId() == null) {
                    GridDhtPartitionMap map = sm.partitions().get(CU.cacheId(DEFAULT_CACHE_NAME));

                    // Partition states message for a group is send as soon as group rebalancing has finished.
                    return map != null && !map.hasMovingPartitions();
                }
            }

            return false;
        });

        IgniteInternalFuture startFut = GridTestUtils.runAsync(() -> {
            try {
                resetBaselineTopology();
            }
            catch (Exception e) {
                log.info(e.getMessage());
            }
        });

        spi0.waitForBlocked(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(2));

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            for (int i = LOAD_LOOP / 2; i < LOAD_LOOP; i++)
                streamer.addData(keys.get(i), String.valueOf(keys.get(i)));

            streamer.close();
        });

        spi0.waitForBlocked(DataStreamerRequest.class, getTestIgniteInstanceName(1));

        spi0.stopBlock(true, (msg) -> {
            if (msg.ioMessage().message() instanceof GridDhtPartitionsFullMessage)
                return true;

            return false;
        });

        spi2.waitForBlocked();

        spi0.stopBlock();

        loadFut.get();

        spi2.stopBlock();

        startFut.get();

        awaitPartitionMapExchange();

        checkSize(ignite0, keys, 2);
    }

    /**
     * This test loads data during clearing partition before rebalance started in not persistent cache. After the
     * loading it checks that all entries presenting in cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInMemory() throws Exception {
        persistenceEnabled = false;

        IgniteEx ignite0 = startGrids(2);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(ignite0);

        List<Integer> keys = movingKeysAfterJoin(ignite0, DEFAULT_CACHE_NAME, LOAD_LOOP, null, getTestIgniteInstanceName(2));

        IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME);

        for (int i = 0; i < LOAD_LOOP / 2; i++)
            streamer.addData(keys.get(i), String.valueOf(keys.get(i)));

        streamer.flush();

        spi0.blockMessages(DataStreamerRequest.class, getTestIgniteInstanceName(1));
        spi0.blockMessages(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(2));

        TestRecordingCommunicationSpi spi2 = new TestRecordingCommunicationSpi();

        spi2.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage sm = (GridDhtPartitionsSingleMessage)msg;

                if (sm.exchangeId() == null) {
                    GridDhtPartitionMap map = sm.partitions().get(CU.cacheId(DEFAULT_CACHE_NAME));

                    // Partition states message for a group is send as soon as group rebalancing has finished.
                    return map != null && !map.hasMovingPartitions();
                }
            }

            return false;
        });

        IgniteInternalFuture startFut = GridTestUtils.runAsync(() -> {
            try {
                IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(2))
                    .setCommunicationSpi(spi2));

                startGrid(cfg);
            }
            catch (Exception e) {
                log.info(e.getMessage());
            }
        });

        spi0.waitForBlocked(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(2));

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            for (int i = LOAD_LOOP / 2; i < LOAD_LOOP; i++)
                streamer.addData(keys.get(i), String.valueOf(keys.get(i)));

            streamer.close();
        });

        spi0.waitForBlocked(DataStreamerRequest.class, getTestIgniteInstanceName(1));

        spi0.stopBlock(true, (msg) -> {
            if (msg.ioMessage().message() instanceof GridDhtPartitionsFullMessage)
                return true;

            return false;
        });

        spi2.waitForBlocked();

        spi0.stopBlock();

        loadFut.get();

        spi2.stopBlock();

        startFut.get();

        awaitPartitionMapExchange();

        checkSize(ignite0, keys, 2);
    }

    /**
     * Checks that all entries contain in cache.
     *
     * @param ignite0 Ignite.
     * @param keys List of keys.
     * @param joinNode Joined node number.
     */
    private void checkSize(IgniteEx ignite0, List<Integer> keys, int joinNode) {
        Affinity aff = ignite0.affinity(DEFAULT_CACHE_NAME);

        IgniteCache cache = grid(joinNode).cache(DEFAULT_CACHE_NAME);

        for (Integer key : keys) {
            assertTrue(aff.isPrimary(grid(joinNode).localNode(), key));

            if (cache.get(key) == null)
                log.warning("Key " + key + " was not found.");
        }

        int size = 0;

        for (Ignite ign : G.allGrids()) {
            size += ign.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY);

            log.info("Cache of " + ign.name() + " " + ign.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY));
        }

        assertEquals(LOAD_LOOP, size);
    }
}
