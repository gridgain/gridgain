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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_PME_FREE_SWITCH_DISABLED;

/**
 *
 */
public class GridExchangeFreeSwitchTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 6;

    /** Persistence flag. */
    private boolean startPersistentRegion;

    /** Start additional cache in volatile region. */
    private boolean startVolatileRegion;

    /** Persistent region name. */
    private static final String PERSISTENT = "persistent";

    /** Volatile region name. */
    private static final String VOLATILE = "volatile";

    /** */
    private static final int PARTS_CNT = 64;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setActiveOnStart(false);
        cfg.setAutoActivationEnabled(false);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        List<DataRegionConfiguration> cfgs = new ArrayList<>();

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);

        final int size = 50 * 1024 * 1024;

        assertTrue(startPersistentRegion || startVolatileRegion);

        if (startPersistentRegion) {
            DataRegionConfiguration drCfg = new DataRegionConfiguration();
            drCfg.setName(PERSISTENT).setInitialSize(size).setMaxSize(size).setPersistenceEnabled(true);
            cfgs.add(drCfg);
        }

        if (startVolatileRegion) {
            DataRegionConfiguration drCfg = new DataRegionConfiguration();
            drCfg.setName(VOLATILE).setInitialSize(size).setMaxSize(size);
            cfgs.add(drCfg);
        }

        dsCfg.setDefaultDataRegionConfiguration(cfgs.get(0));
        cfgs.remove(0);

        if (!cfgs.isEmpty())
            dsCfg.setDataRegionConfigurations(cfgs.toArray(new DataRegionConfiguration[cfgs.size()]));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setDataRegionName(cacheName);
        ccfg.setName(cacheName);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(0);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "false")
    public void testNodeLeftOnStableTopology_Volatile_1() throws Exception {
        testNodeLeftOnStableTopology(false, true, false, true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "true")
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Volatile_2() throws Exception {
        // Baseline auto adjust for volatile caches will prevent the optimization.
        testNodeLeftOnStableTopology(false, true, false, true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Volatile_3() throws Exception {
        testNodeLeftOnStableTopology(false, true, false, false);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Volatile_4() throws Exception {
        testNodeLeftOnStableTopology(false, true, true, true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Persistent_1() throws Exception {
        testNodeLeftOnStableTopology(true, false, false, false);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "true")
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Persistent_2() throws Exception {
        // Auto adjust for volatile caches shouldn't have any effect for persistent caches.
        testNodeLeftOnStableTopology(true, false, false, false);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "false")
    public void testNodeLeftOnStableTopology_Persistent_3() throws Exception {
        testNodeLeftOnStableTopology(true, false, false, false);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Persistent_4() throws Exception {
        testNodeLeftOnStableTopology(true, false, true, true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    @WithSystemProperty(key = IGNITE_PME_FREE_SWITCH_DISABLED, value = "true")
    public void testNodeLeftOnStableTopology_Persistent_5() throws Exception {
        // Explicitly disabling the optimization, PME is expected.
        testNodeLeftOnStableTopology(true, false, false, true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "true")
    @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Mixed_1() throws Exception {
        // Auto adjust for volatile caches shouldn't have any effect for mixed caches.
        testNodeLeftOnStableTopology(true, true, false, false);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Mixed_2() throws Exception {
        testNodeLeftOnStableTopology(true, true, false, false);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "false")
    public void testNodeLeftOnStableTopology_Mixed_3() throws Exception {
        testNodeLeftOnStableTopology(true, true, false, true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    public void testNodeLeftOnStableTopology_Mixed_4() throws Exception {
        testNodeLeftOnStableTopology(true, true, true, true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "true")
    @WithSystemProperty(key = IGNITE_PME_FREE_SWITCH_DISABLED, value = "true")
    public void testNodeLeftOnStableTopology_Mixed_5() throws Exception {
        // Explicitly disabling the optimization, PME is expected.
        testNodeLeftOnStableTopology(true, true, false, true);
    }

    /**
     * Checks node left PME absent/present on stable topology.
     *
     * @param persistent {@code True} to add persistent region.
     * @param inmem {@code True} to add volatile region.
     * @param resetBlt {@code True} to reset BTL on node left.
     * @param expectPME {@code True} if distributed partition states exchange is expected on node left.
     */
    private void testNodeLeftOnStableTopology(
        boolean persistent,
        boolean inmem,
        boolean resetBlt,
        boolean expectPME
    ) throws Exception {
        startPersistentRegion = persistent;
        startVolatileRegion = inmem;

        int nodes = NODES_CNT;

        Ignite crd = startGrids(nodes);

        crd.cluster().active(true);

        if (persistent)
            crd.createCache(cacheConfiguration(PERSISTENT));

        if (inmem)
            crd.createCache(cacheConfiguration(VOLATILE));

        awaitPartitionMapExchange();

        AtomicLong cnt = new AtomicLong();

        for (int i = 0; i < nodes; i++) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg.getClass().equals(GridDhtPartitionsSingleMessage.class) &&
                        ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null)
                        cnt.incrementAndGet();

                    if (expectPME) // Do not block PME if it's expected.
                        return false;

                    return msg.getClass().equals(GridDhtPartitionsSingleMessage.class) ||
                        msg.getClass().equals(GridDhtPartitionsFullMessage.class);
                }
            });
        }

        Random r = new Random();

        while (nodes > 1) {
            G.allGrids().get(r.nextInt(nodes--)).close(); // Stopping random node.

            awaitPartitionMapExchange(true, true, null, true);

            if (resetBlt) {
                resetBaselineTopology();

                awaitPartitionMapExchange(true, true, null, true);
            }

            assertEquals(expectPME ? (nodes - 1) : 0, cnt.get());

            IgniteEx alive = (IgniteEx)G.allGrids().get(0);

            assertTrue(alive.context().cache().context().exchange().lastFinishedFuture().rebalanced());

            cnt.set(0);
        }
    }

    /**
     *
     */
    @Test
    public void testNoTransactionsWaitAtNodeLeftWithZeroBackupsAndLossIgnore() throws Exception {
        testNoTransactionsWaitAtNodeLeft(0, PartitionLossPolicy.IGNORE);
    }

    /**
     *
     */
    @Test
    public void testNoTransactionsWaitAtNodeLeftWithZeroBackupsAndLossSafe() throws Exception {
        testNoTransactionsWaitAtNodeLeft(0, PartitionLossPolicy.READ_WRITE_SAFE);
    }

    /**
     *
     */
    @Test
    public void testNoTransactionsWaitAtNodeLeftWithSingleBackup() throws Exception {
        testNoTransactionsWaitAtNodeLeft(1, PartitionLossPolicy.IGNORE);
    }

    /**
     * Starts 4(2 with 0 backups) * multiplicator threads each spawning a transaction.
     * Each tx locks a topology by single put with various key->failing node type.
     * Single node is failed.
     * Each tx puts another key.
     *
     * Success: all transactions are completed as expected, fast switch happens.
     */
    private void testNoTransactionsWaitAtNodeLeft(int backups, PartitionLossPolicy lossPlc) throws Exception {
        startPersistentRegion = true;
        startVolatileRegion = false;

        String cacheName = "three-partitioned";

        try {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setDataRegionName(PERSISTENT);
            ccfg.setName(cacheName);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setBackups(backups);
            ccfg.setPartitionLossPolicy(lossPlc);
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            ccfg.setAffinity(new Map4PartitionsTo4NodesAffinityFunction());

            int nodes = 4;

            final Ignite crd = startGridsMultiThreaded(nodes, true);
            crd.cluster().active(true);

            crd.createCache(ccfg);

            AtomicLong cnt = new AtomicLong();

            for (int i = 0; i < nodes; i++) {
                TestRecordingCommunicationSpi spi =
                    (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

                spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                    @Override public boolean apply(ClusterNode node, Message msg) {
                        if (msg.getClass().equals(GridDhtPartitionsSingleMessage.class) &&
                            ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null)
                            cnt.incrementAndGet();

                        return msg.getClass().equals(GridDhtPartitionsSingleMessage.class) ||
                            msg.getClass().equals(GridDhtPartitionsFullMessage.class);
                    }
                });
            }

            Random r = new Random();

            Ignite candidate;
            MvccProcessor proc;

            do {
                candidate = G.allGrids().get(r.nextInt(nodes));

                proc = ((IgniteEx)candidate).context().coordinators();
            }
            // MVCC coordinator fail always breaks transactions, excluding.
            while (proc.mvccEnabled() && proc.currentCoordinator().local());

            Ignite failed = candidate;

            int multiplicator = 3;

            AtomicInteger key_from = new AtomicInteger();

            CountDownLatch readyLatch = new CountDownLatch((backups > 0 ? 4 : 2) * multiplicator);
            CountDownLatch failedLatch = new CountDownLatch(1);

            IgniteCache<Integer, Integer> failedCache = failed.getOrCreateCache(cacheName);

            IgniteInternalFuture<?> nearThenNearFut = multithreadedAsync(() -> {
                try {
                    List<Integer> keys = nearKeys(failedCache, 2, key_from.addAndGet(100));

                    Integer key0 = keys.get(0);
                    Integer key1 = keys.get(1);

                    Ignite primary = primaryNode(key0, cacheName);

                    assertNotSame(failed, primary);

                    IgniteCache<Integer, Integer> primaryCache = primary.getOrCreateCache(cacheName);

                    try (Transaction tx = primary.transactions().txStart()) {
                        primaryCache.put(key0, key0);

                        readyLatch.countDown();
                        failedLatch.await();

                        primaryCache.put(key1, key1);

                        tx.commit();
                    }

                    assertEquals(key0, primaryCache.get(key0));
                    assertEquals(key1, primaryCache.get(key1));
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, multiplicator);

            IgniteInternalFuture<?> primaryThenPrimaryFut = backups > 0 ? multithreadedAsync(() -> {
                try {
                    List<Integer> keys = primaryKeys(failedCache, 2, key_from.addAndGet(100));

                    Integer key0 = keys.get(0);
                    Integer key1 = keys.get(1);

                    Ignite backup = backupNode(key0, cacheName);

                    assertNotSame(failed, backup);

                    IgniteCache<Integer, Integer> backupCache = backup.getOrCreateCache(cacheName);

                    try (Transaction tx = backup.transactions().txStart()) {
                        backupCache.put(key0, key0);

                        readyLatch.countDown();
                        failedLatch.await();

                        try {
                            backupCache.put(key1, key1);

                            fail("Should not happen");
                        }
                        catch (Exception ignored) {
                            // Transaction broken because of primary left.
                        }
                    }
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, multiplicator) : new GridFinishedFuture<>();

            IgniteInternalFuture<?> nearThenPrimaryFut = multithreadedAsync(() -> {
                try {
                    Integer key0 = nearKeys(failedCache, 1, key_from.addAndGet(100)).get(0);
                    Integer key1 = primaryKeys(failedCache, 1, key_from.addAndGet(100)).get(0);

                    Ignite primary = primaryNode(key0, cacheName);

                    assertNotSame(failed, primary);

                    IgniteCache<Integer, Integer> primaryCache = primary.getOrCreateCache(cacheName);

                    try (Transaction tx = primary.transactions().txStart()) {
                        primaryCache.put(key0, key0);

                        readyLatch.countDown();
                        failedLatch.await();

                        try {
                            primaryCache.put(key1, key1);

                            fail("Should not happen");
                        }
                        catch (Exception ignored) {
                            // Transaction broken because of primary left.
                        }
                    }
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, multiplicator);

            IgniteInternalFuture<?> nearThenBackupFut = backups > 0 ? multithreadedAsync(() -> {
                try {
                    Integer key0 = nearKeys(failedCache, 1, key_from.addAndGet(100)).get(0);
                    Integer key1 = backupKeys(failedCache, 1, key_from.addAndGet(100)).get(0);

                    Ignite primary = primaryNode(key0, cacheName);

                    assertNotSame(failed, primary);

                    IgniteCache<Integer, Integer> primaryCache = primary.getOrCreateCache(cacheName);

                    try (Transaction tx = primary.transactions().txStart()) {
                        primaryCache.put(key0, key0);

                        readyLatch.countDown();
                        failedLatch.await();

                        primaryCache.put(key1, key1);

                        tx.commit();
                    }

                    assertEquals(key0, primaryCache.get(key0));
                    assertEquals(key1, primaryCache.get(key1));
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, multiplicator) : new GridFinishedFuture<>();

            readyLatch.await();

            failed.close(); // Stopping node.

            awaitPartitionMapExchange();

            failedLatch.countDown();

            nearThenNearFut.get();
            primaryThenPrimaryFut.get();
            nearThenPrimaryFut.get();
            nearThenBackupFut.get();

            int pmeFreeCnt = 0;

            for (Ignite ignite : G.allGrids()) {
                assertEquals(nodes + 1, ignite.cluster().topologyVersion());

                ExchangeContext ctx =
                    ((IgniteEx)ignite).context().cache().context().exchange().lastFinishedFuture().context();

                if (ctx.exchangeFreeSwitch())
                    pmeFreeCnt++;
            }

            assertEquals(nodes - 1, pmeFreeCnt);

            assertEquals(0, cnt.get());
        }
        finally {
            startPersistentRegion = false;
        }
    }

    /**
     *
     */
    @Test
    public void testLateAffinityAssignmentOnBackupLeftAndJoin() throws Exception {
        startPersistentRegion = true;
        startVolatileRegion = false;

        final String cacheName = "single-partitioned";

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setDataRegionName(PERSISTENT);
        ccfg.setName(cacheName);
        ccfg.setAffinity(new Map1PartitionsTo2NodesAffinityFunction());

        startGrid(0); // Primary partition holder.
        startGrid(1); // Backup partition holder.

        grid(0).cluster().active(true);

        final IgniteCache cache = grid(0).createCache(ccfg);

        grid(1).close(); // Stopping backup partition holder.

        cache.put(1, 1); // Updating primary partition to cause rebalance.

        startGrid(1); // Restarting backup partition holder.

        awaitPartitionMapExchange();

        AffinityTopologyVersion topVer = grid(0).context().discovery().topologyVersionEx();

        assertEquals(topVer.topologyVersion(), 4);
        assertEquals(topVer.minorTopologyVersion(), 1); // LAA happen on backup partition holder restart.

        GridDhtPartitionsExchangeFuture fut4 = null;
        GridDhtPartitionsExchangeFuture fut41 = null;

        for (GridDhtPartitionsExchangeFuture fut : grid(0).context().cache().context().exchange().exchangeFutures()) {
            AffinityTopologyVersion ver = fut.topologyVersion();

            if (ver.topologyVersion() == 4) {
                if (ver.minorTopologyVersion() == 0)
                    fut4 = fut;
                else if (ver.minorTopologyVersion() == 1)
                    fut41 = fut;
            }
        }

        assertFalse(fut4.rebalanced()); // Backup partition holder restart cause non-rebalanced state.

        assertTrue(fut41.rebalanced()); // LAA.
    }

    /**
     *
     */
    private static class Map4PartitionsTo4NodesAffinityFunction extends RendezvousAffinityFunction {
        /**
         * Default constructor.
         */
        public Map4PartitionsTo4NodesAffinityFunction() {
            super(false, 4);
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>(4);

            int backups = affCtx.backups();

            if (affCtx.currentTopologySnapshot().size() == 4) {
                List<ClusterNode> p0 = new ArrayList<>();
                List<ClusterNode> p1 = new ArrayList<>();
                List<ClusterNode> p2 = new ArrayList<>();
                List<ClusterNode> p3 = new ArrayList<>();

                p0.add(affCtx.currentTopologySnapshot().get(0));
                p1.add(affCtx.currentTopologySnapshot().get(1));
                p2.add(affCtx.currentTopologySnapshot().get(2));
                p3.add(affCtx.currentTopologySnapshot().get(3));

                if (backups == 1) {
                    p0.add(affCtx.currentTopologySnapshot().get(1));
                    p1.add(affCtx.currentTopologySnapshot().get(2));
                    p2.add(affCtx.currentTopologySnapshot().get(3));
                    p3.add(affCtx.currentTopologySnapshot().get(0));
                }

                res.add(p0);
                res.add(p1);
                res.add(p2);
                res.add(p3);
            }

            return res;
        }
    }

    /**
     *
     */
    private static class Map1PartitionsTo2NodesAffinityFunction extends RendezvousAffinityFunction {
        /**
         * Default constructor.
         */
        public Map1PartitionsTo2NodesAffinityFunction() {
            super(false, 1);
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>(2);

            List<ClusterNode> p0 = new ArrayList<>();

            p0.add(affCtx.currentTopologySnapshot().get(0));

            if (affCtx.currentTopologySnapshot().size() == 2)
                p0.add(affCtx.currentTopologySnapshot().get(1));

            res.add(p0);

            return res;
        }
    }

    @Override protected long getPartitionMapExchangeTimeout() {
        return super.getPartitionMapExchangeTimeout() * 10;
    }
}
