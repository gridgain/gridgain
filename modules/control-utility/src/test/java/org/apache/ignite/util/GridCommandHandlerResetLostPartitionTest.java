/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/** */
public class GridCommandHandlerResetLostPartitionTest extends GridCommandHandlerClusterByClassAbstractTest {

    /** Cache name. */
    private static final String[] CACHE_NAMES = {"cacheOne", "cacheTwo", "cacheThree"};

    /** Cache size */
    public static final int CACHE_SIZE = 10000 / CACHE_NAMES.length;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setPageSize(1024)
            .setWalMode(LOG_ONLY)
            .setWalSegmentSize(4 * 1024 * 1024);

        storageCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(100L * 1024 * 1024));

        cfg.setDataStorageConfiguration(storageCfg);

        CacheConfiguration<?, ?>[] ccfg = new CacheConfiguration[] {
            cacheConfiguration(CACHE_NAMES[0], CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_NAMES[1], CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_NAMES[2], CacheAtomicityMode.TRANSACTIONAL)
        };

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @param mode Cache atomicity mode.
     * @return Configured cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String cacheName, CacheAtomicityMode mode) {
        return new CacheConfiguration<>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(mode)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
            .setAffinity(new RendezvousAffinityFunction(false, 64))
            .setIndexedTypes(String.class, String.class);
    }

    /** */
    @Test
    public void testCacheResetLostPartitionsAll() throws Exception {
        injectTestSystemOut();

        doRebalanceAfterPartitionsWereLost();

        AffinityTopologyVersion initTopVer = grid(0).context().discovery().topologyVersionEx();

        assertEquals(EXIT_CODE_OK, execute("--cache", "reset_lost_partitions", "--all"));

        AffinityTopologyVersion resTopVer = grid(0).context().discovery().topologyVersionEx();

        assertEquals(
        "Resetting lost partitions should trigger PME only one time [initialTopologyVersion="
                + initTopVer + ", finalTopologyVersion=" + resTopVer + ']',
            initTopVer.nextMinorVersion(),
            resTopVer);

        final String out = testOut.toString();

        assertContains(log, out, "The following caches have LOST partitions: [cacheTwo, cacheThree, cacheOne].");

        assertContains(log, out, "Reset LOST-partitions performed successfully. Cache group (name = 'cacheOne', id = -433504380), caches ([cacheOne]).");
        assertContains(log, out, "Reset LOST-partitions performed successfully. Cache group (name = 'cacheTwo', id = -433499286), caches ([cacheTwo]).");
        assertContains(log, out, "Reset LOST-partitions performed successfully. Cache group (name = 'cacheThree', id = 18573116), caches ([cacheThree]).");

        // Check all nodes report same lost partitions.
        for (String cacheName : CACHE_NAMES) {
            for (Ignite grid : G.allGrids())
                assertTrue(grid.cache(cacheName).lostPartitions().isEmpty());
        }

        stopAllGrids();
        startGrids(3);

        crd = grid(0);

        // All data was back.
        assertEquals(CACHE_NAMES.length * CACHE_SIZE, averageSizeAroundAllNodes());
    }

    /**
     * Tests that resetting lost partitions for the system cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResetLostPartitionsSystemCache() throws Exception {
        IgniteEx g0 = startGrids(2);

        g0.cluster().state(ACTIVE);

        stopGrid(1);

        // Fill the system cache with some data.
        // Number of key value pairs should be enough to fill all partitions.
        IgniteInternalCache<Integer, Integer> sysCache0 = g0.context().cache().utilityCache();
        int keyNums = sysCache0.configuration().getAffinity().partitions() * 10;
        for (int i = 0; i < keyNums; i++)
            sysCache0.put(i, i);

        // Block supply messages for the system cache and start previously stopped node to make it lost partitions.
        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(g0);
        spi0.blockMessages(TestRecordingCommunicationSpi.blockSupplyMessageForGroup(CU.cacheId(CU.UTILITY_CACHE_NAME)));

        IgniteInternalFuture<?> startFut = GridTestUtils.runAsync(() -> startGrid(1));

        spi0.waitForBlocked();

        stopGrid(0);

        spi0.stopBlock(false);

        startFut.get();

        crd = grid(1);

        IgniteInternalCache<Integer, Integer> sysCache1 = grid(1).context().cache().utilityCache();

        // All partitions must be in lost state.
        assertEquals(sysCache1.configuration().getAffinity().partitions(), sysCache1.lostPartitions().size());

        // Resetting lost partitions for the syste cache.
        assertEquals(EXIT_CODE_OK, execute(
            "--port",
            grid(1).localNode().attribute(ATTR_REST_TCP_PORT).toString(),
            "--cache",
            "reset_lost_partitions",
            CU.UTILITY_CACHE_NAME));

        // Check that all partitions were reset.
        assertEquals(0, sysCache1.lostPartitions().size());
    }

    /**
     * @throws Exception if fail.
     */
    private void doRebalanceAfterPartitionsWereLost() throws Exception {
        startGrids(3);

        grid(0).cluster().state(ACTIVE);

        for (String cacheName : CACHE_NAMES) {
            Map<String, String> putMap = new TreeMap<>();
            for (int i = 0; i < CACHE_SIZE; i++)
                putMap.put(Integer.toString(i), "Value" + i);

            grid(0).cache(cacheName).putAll(putMap);
        }

        String g1Name = grid(1).name();

        stopGrid(1);

        cleanPersistenceDir(g1Name);

        // Here we have two from three data nodes and cache with 1 backup. So there is no data loss expected.
        assertEquals(CACHE_NAMES.length * CACHE_SIZE, averageSizeAroundAllNodes());

        // Start node 1 with empty PDS. Rebalance will be started, only sys cache will be rebalanced.
        IgniteConfiguration cfg1 = getConfiguration(getTestIgniteInstanceName(1));
        TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi) cfg1.getCommunicationSpi();
        spi1.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionDemandMessage) {
                    GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage) msg;

                    return msg0.groupId() != CU.cacheId("ignite-sys-cache");
                }

                return false;
            }
        });

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(cfg1);

                return null;
            }
        });

        spi1.waitForBlocked();

        // During rebalance stop node 3. Rebalance will be stopped.
        stopGrid(2);

        spi1.stopBlock();

        // Start node 3.
        startGrid(2);

        // Data loss is expected because rebalance to node 1 have not finished and node 2 was stopped.
        assertTrue(CACHE_NAMES.length * CACHE_SIZE > averageSizeAroundAllNodes());

        // Check all nodes report same lost partitions.
        for (String cacheName : CACHE_NAMES) {
            Collection<Integer> lost = null;

            for (Ignite grid : G.allGrids()) {
                if (lost == null)
                    lost = grid.cache(cacheName).lostPartitions();
                else
                    assertEquals(lost, grid.cache(cacheName).lostPartitions());
            }

            assertTrue(lost != null && !lost.isEmpty());
        }
    }

    /**
     * Checks that all nodes see the correct size.
     */
    private int averageSizeAroundAllNodes() {
        int totalSize = 0;

        for (Ignite ignite : IgnitionEx.allGrids()) {
            for (String cacheName : CACHE_NAMES)
                totalSize += ignite.cache(cacheName).size();
        }

        return totalSize / IgnitionEx.allGrids().size();
    }
}
