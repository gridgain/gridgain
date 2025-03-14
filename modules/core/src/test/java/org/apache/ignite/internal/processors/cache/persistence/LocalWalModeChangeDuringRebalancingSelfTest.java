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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.file.AbstractFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class LocalWalModeChangeDuringRebalancingSelfTest extends GridCommonAbstractTest {
    /** */
    private static int dfltCacheBackupCnt;

    /** */
    private static final AtomicReference<CountDownLatch> supplyMessageLatch = new AtomicReference<>();

    /** */
    private static final AtomicReference<CountDownLatch> fileIOLatch = new AtomicReference<>();

    /** Replicated cache name. */
    private static final String REPL_CACHE = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
                // Test verifies checkpoint count, so it is essential that no checkpoint is triggered by timeout
                .setCheckpointFrequency(999_999_999_999L)
                .setWalMode(WALMode.LOG_ONLY)
                .setFileIOFactory(new TestFileIOFactory(new DataStorageConfiguration().getFileIOFactory()))
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                // Test checks internal state before and after rebalance, so it is configured to be triggered manually
                .setRebalanceDelay(-1)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setBackups(dfltCacheBackupCnt),

            new CacheConfiguration(REPL_CACHE)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setRebalanceDelay(-1)
                .setCacheMode(CacheMode.REPLICATED)
        );

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                    int grpId = ((GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message()).groupId();

                    if (grpId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        CountDownLatch latch0 = supplyMessageLatch.get();

                        if (latch0 != null)
                            try {
                                latch0.await();
                            }
                            catch (InterruptedException ex) {
                                throw new IgniteException(ex);
                            }
                    }
                }

                super.sendMessage(node, msg);
            }

            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                    int grpId = ((GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message()).groupId();

                    if (grpId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        CountDownLatch latch0 = supplyMessageLatch.get();

                        if (latch0 != null)
                            try {
                                latch0.await();
                            }
                            catch (InterruptedException ex) {
                                throw new IgniteException(ex);
                            }
                    }
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CountDownLatch msgLatch = supplyMessageLatch.get();

        if (msgLatch != null) {
            while (msgLatch.getCount() > 0)
                msgLatch.countDown();

            supplyMessageLatch.set(null);
        }

        CountDownLatch fileLatch = fileIOLatch.get();

        if (fileLatch != null) {
            while (fileLatch.getCount() > 0)
                fileLatch.countDown();

            fileIOLatch.set(null);
        }

        stopAllGrids();

        cleanPersistenceDir();

        dfltCacheBackupCnt = 0;
    }

    /**
     * @return Count of entries to be processed within test.
     */
    protected int getKeysCount() {
        return 10_000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISABLE_WAL_DURING_REBALANCING, value = "true")
    public void testWalDisabledDuringRebalancing() throws Exception {
        doTestSimple();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISABLE_WAL_DURING_REBALANCING, value = "false")
    public void testWalNotDisabledIfParameterSetToFalse() throws Exception {
        doTestSimple();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestSimple() throws Exception {
        boolean disableWalDuringRebalancing = IgniteSystemProperties.getBoolean(IGNITE_DISABLE_WAL_DURING_REBALANCING);

        IgniteEx ignite = startGrids(3);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache2 = ignite.cache(REPL_CACHE);

        int keysCnt = getKeysCount();

        for (int k = 0; k < keysCnt; k++) {
            cache.put(k, k);
            cache2.put(k, k); // Empty cache causes skipped checkpoint.
        }

        IgniteEx newIgnite = startGrid(3);

        final CheckpointHistory cpHist =
            ((GridCacheDatabaseSharedManager)newIgnite.context().cache().context().database()).checkpointHistory();

        assertTrue(
            "Failed to wait for the first checkpoint after logical recovery.",
            waitForCondition(() -> !cpHist.checkpoints().isEmpty(), 10_000));

        U.sleep(10); // To ensure timestamp granularity.

        long newIgniteStartedTimestamp = System.currentTimeMillis();

        newIgnite.cluster().setBaselineTopology(4);

        awaitExchange(newIgnite);

        AffinityTopologyVersion topVer = newIgnite.context().cache().context().exchange().lastTopologyFuture().topologyVersion();

        assertTrue(
            "Failed to wait for the actual rebalance future.",
            waitForCondition(() -> {
                GridDhtPartitionDemander.RebalanceFuture rebFut = (GridDhtPartitionDemander.RebalanceFuture) newIgnite
                    .context()
                    .cache()
                    .utilityCache()
                    .context()
                    .preloader()
                    .rebalanceFuture();

                return rebFut != null && topVer.equals(rebFut.topologyVersion());
            }, 10_000));

        // Wait for rebalance of the system cache.
        newIgnite.context().cache().utilityCache().context().preloader().rebalanceFuture().get(10_000);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertEquals(!disableWalDuringRebalancing, grpCtx.walEnabled());

        U.sleep(10); // To ensure timestamp granularity.

        long rebalanceStartedTimestamp = System.currentTimeMillis();

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue(grpCtx.walEnabled());

        U.sleep(10); // To ensure timestamp granularity.

        long rebalanceFinishedTimestamp = System.currentTimeMillis();

        for (Integer k = 0; k < keysCnt; k++)
            assertEquals("k=" + k, k, cache.get(k));

        int checkpointsBeforeNodeStarted = 0;
        int checkpointsBeforeRebalance = 0;
        int checkpointsAfterRebalance = 0;

        for (Long timestamp : cpHist.checkpoints()) {
            if (timestamp < newIgniteStartedTimestamp)
                checkpointsBeforeNodeStarted++;
            else if (timestamp >= newIgniteStartedTimestamp && timestamp < rebalanceStartedTimestamp)
                checkpointsBeforeRebalance++;
            else if (timestamp >= rebalanceStartedTimestamp && timestamp <= rebalanceFinishedTimestamp)
                checkpointsAfterRebalance++;
        }

        assertEquals(1, checkpointsBeforeNodeStarted); // checkpoint on start
        assertEquals(disableWalDuringRebalancing ? 1 : 0, checkpointsBeforeRebalance); // checkpoint related to rebalanced system cache.

        // Expecting a checkpoint for each group.
        assertEquals(disableWalDuringRebalancing ? newIgnite.context().cache().cacheGroups().size() - 1 : 0,
            checkpointsAfterRebalance); // checkpoint if WAL was re-activated
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalAndGlobalWalStateInterdependence() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < getKeysCount(); k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        newIgnite.cluster().setBaselineTopology(ignite.cluster().nodes());

        awaitExchange(newIgnite);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertFalse(grpCtx.walEnabled());

        ignite.cluster().disableWal(DEFAULT_CACHE_NAME);

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertFalse(grpCtx.walEnabled()); // WAL is globally disabled

        ignite.cluster().enableWal(DEFAULT_CACHE_NAME);

        assertTrue(grpCtx.walEnabled());
    }

    /**
     * Test that local WAL mode changing works well with exchanges merge.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWithExchangesMerge() throws Exception {
        final int nodeCnt = 4;
        final int keyCnt = getKeysCount();

        Ignite ignite = startGrids(nodeCnt);

        ignite.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.cache(REPL_CACHE);

        for (int k = 0; k < keyCnt; k++)
            cache.put(k, k);

        stopGrid(2);
        stopGrid(3);

        // Rewrite data to trigger further rebalance.
        for (int k = 0; k < keyCnt; k++)
            cache.put(k, k * 2);

        // Start several grids in parallel to trigger exchanges merge.
        startGridsMultiThreaded(2, 2);

        for (int nodeIdx = 2; nodeIdx < nodeCnt; nodeIdx++) {
            CacheGroupContext grpCtx = grid(nodeIdx).cachex(REPL_CACHE).context().group();

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return !grpCtx.walEnabled();
                }
            }, 5_000));
        }

        // Invoke rebalance manually.
        for (Ignite g : G.allGrids())
            g.cache(REPL_CACHE).rebalance();

        awaitPartitionMapExchange(false, false, null, false, Collections.singleton(REPL_CACHE));

        for (int nodeIdx = 2; nodeIdx < nodeCnt; nodeIdx++) {
            CacheGroupContext grpCtx = grid(nodeIdx).cachex(REPL_CACHE).context().group();

            assertTrue(grpCtx.walEnabled());
        }

        // Check no data loss.
        for (int nodeIdx = 2; nodeIdx < nodeCnt; nodeIdx++) {
            IgniteCache<Integer, Integer> cache0 = grid(nodeIdx).cache(REPL_CACHE);

            for (int k = 0; k < keyCnt; k++)
                Assert.assertEquals("nodeIdx=" + nodeIdx + ", key=" + k, (Integer)(2 * k), cache0.get(k));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParallelExchangeDuringRebalance() throws Exception {
        doTestParallelExchange(supplyMessageLatch);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParallelExchangeDuringCheckpoint() throws Exception {
        doTestParallelExchange(fileIOLatch);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestParallelExchange(AtomicReference<CountDownLatch> latchRef) throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < getKeysCount(); k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        CountDownLatch latch = new CountDownLatch(1);

        latchRef.set(latch);

        ignite.cluster().setBaselineTopology(ignite.cluster().nodes());

        // Await fully exchange complete.
        awaitExchange(newIgnite);

        assertFalse(grpCtx.walEnabled());

        // TODO : test with client node as well
        startGrid(4); // Trigger exchange

        assertFalse(grpCtx.walEnabled());

        latch.countDown();

        assertFalse(grpCtx.walEnabled());

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue(waitForCondition(grpCtx::walEnabled, 2_000));
    }

    /**
     * Node doesn't delete consistent PDS when WAL was turned off automatically (disable WAL during rebalancing feature).
     *
     * <p>
     * Test scenario:
     * <ol>
     *     <li>
     *         Two server nodes are started, cluster is activated, baseline is set. 2500 keys are put into cache.
     *     </li>
     *     <li>
     *         Checkpoint is started and finished on both nodes.
     *     </li>
     *     <li>
     *         Node n1 is stopped, another 2500 keys are put into the same cache.
     *     </li>
     *     <li>
     *         Node n1 is started back so rebalancing is triggered from n0 to n1. WAL is turned off on n1 automatically.
     *     </li>
     *     <li>
     *         Both nodes are stopped without checkpoint.
     *     </li>
     *     <li>
     *         Node n1 is started and activated. Lost partitions are reset.
     *     </li>
     *     <li>
     *         First 2500 keys are found in cache thus PDS wasn't removed on restart.
     *     </li>
     *     <li>
     *         Second 2500 keys are not found in cache as WAL was disabled during rebalancing
     *         and no checkpoint was triggered.
     *     </li>
     * </ol>
     * </p>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConsistentPdsIsNotClearedAfterRestartWithDisabledWal() throws Exception {
        dfltCacheBackupCnt = 1;

        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);

        ig0.cluster().baselineAutoAdjustEnabled(false);
        ig0.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ig0.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 2500; k++)
            cache.put(k, k);

        GridCacheDatabaseSharedManager dbMrg0 = (GridCacheDatabaseSharedManager) ig0.context().cache().context().database();
        GridCacheDatabaseSharedManager dbMrg1 = (GridCacheDatabaseSharedManager) ig1.context().cache().context().database();

        dbMrg0.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();
        dbMrg1.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();

        stopGrid(1);

        for (int k = 2500; k < 5000; k++)
            cache.put(k, k);

        ig1 = startGrid(1);

        awaitExchange(ig1);

        stopAllGrids(false);

        ig1 = startGrid(1);
        ig1.cluster().state(ACTIVE);

        ig1.resetLostPartitions(Collections.singletonList(DEFAULT_CACHE_NAME));

        awaitExchange(ig1);

        cache = ig1.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 2500; k++)
            assertTrue(cache.containsKey(k));

        for (int k = 2500; k < 5000; k++)
            assertFalse(cache.containsKey(k));
    }

    /**
     * Test is opposite to {@link #testConsistentPdsIsNotClearedAfterRestartWithDisabledWal()}
     *
     * <p>
     * Test scenario:
     * <ol>
     *      <li>
     *          Two server nodes are started, cluster is activated, baseline is set. 2500 keys are put into cache.
     *      </li>
     *      <li>
     *          Checkpoint is started and finished on both nodes.
     *      </li>
     *      <li>
     *          Node n1 is stopped, another 2500 keys are put into the same cache.
     *      </li>
     *      <li>
     *          Node n1 is started back so rebalancing is triggered from n0 to n1. WAL is turned off on n1 automatically.
     *      </li>
     *      <li>
     *          Both nodes are stopped without checkpoint.
     *      </li>
     *      <li>
     *          CP END marker for the first checkpoint is removed on node n1 so node will think it crushed during checkpoint
     *          on the next restart.
     *      </li>
     *      <li>
     *          Node n1 fails to start as it sees potentially corrupted files of one cache. Manual action is required.
     *      </li>
     *      <li>
     *          Cache files are cleaned up manually on node n1 and it starts successfully.
     *      </li>
     * </ol>
     * </p>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPdsWithBrokenBinaryConsistencyIsClearedAfterRestartWithDisabledWal() throws Exception {
        dfltCacheBackupCnt = 1;

        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);

        String ig1Folder = ig1.context().pdsFolderResolver().resolveFolders().folderName();
        File dbDir = U.resolveWorkDirectory(ig1.configuration().getWorkDirectory(), "db", false);

        File ig1LfsDir = new File(dbDir, ig1Folder);
        File ig1CpDir = new File(ig1LfsDir, "cp");

        ig0.cluster().baselineAutoAdjustEnabled(false);
        ig0.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ig0.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 2500; k++)
            cache.put(k, k);

        GridCacheDatabaseSharedManager dbMrg0 = (GridCacheDatabaseSharedManager) ig0.context().cache().context().database();
        GridCacheDatabaseSharedManager dbMrg1 = (GridCacheDatabaseSharedManager) ig1.context().cache().context().database();

        dbMrg0.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();
        dbMrg1.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();

        stopGrid(1);

        for (int k = 2500; k < 5000; k++)
            cache.put(k, k);

        ig1 = startGrid(1);

        awaitExchange(ig1);

        stopAllGrids(false);

        ig0 = startGrid(0);

        File[] cpMarkers = ig1CpDir.listFiles();

        for (File cpMark : cpMarkers) {
            if (cpMark.getName().contains("-END"))
                cpMark.delete();
        }

        assertThrows(null, () -> startGrid(1), Exception.class, null);

        ig1 = startGrid(1);

        assertEquals(1, ig0.cluster().nodes().size());
        assertEquals(1, ig1.cluster().nodes().size());

        AtomicBoolean actionNotFound = new AtomicBoolean(false);

        ig1.compute().run(new IgniteRunnable() {
            @IgniteInstanceResource
            private Ignite ig;

            @Override public void run() {
                MaintenanceRegistry mntcRegistry = ((IgniteEx) ig).context().maintenanceRegistry();

                List<MaintenanceAction<?>> actions = mntcRegistry
                    .actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

                Optional<MaintenanceAction<?>> optional = actions
                    .stream()
                    .filter(a -> a.name().equals(CleanCacheStoresMaintenanceAction.ACTION_NAME)).findFirst();

                if (!optional.isPresent())
                    actionNotFound.set(true);
                else
                    optional.get().execute();

                mntcRegistry.unregisterMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);
            }
        });

        assertFalse("Action to clear corrupted PDS is not found", actionNotFound.get());

        stopAllGrids();

        ig1 = startGrid(1);

        ig1.cluster().state(ACTIVE);

        assertEquals(1, ig1.cluster().nodes().size());

        cache = ig1.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 2500; k++)
            assertFalse(cache.containsKey(k));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWalNotDisabledAfterShrinkingBaselineTopology() throws Exception {
        IgniteEx ignite = startGrids(4);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int keysCnt = getKeysCount();

        for (int k = 0; k < keysCnt; k++)
            cache.put(k, k);

        for (Ignite g : G.allGrids()) {
            CacheGroupContext grpCtx = ((IgniteEx)g).cachex(DEFAULT_CACHE_NAME).context().group();

            assertTrue(grpCtx.walEnabled());
        }

        stopGrid(2);

        ignite.cluster().setBaselineTopology(5);

        ignite.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        // Await fully exchange complete.
        awaitExchange(ignite);

        for (Ignite g : G.allGrids()) {
            CacheGroupContext grpCtx = ((IgniteEx)g).cachex(DEFAULT_CACHE_NAME).context().group();

            assertTrue(grpCtx.walEnabled());

            g.cache(DEFAULT_CACHE_NAME).rebalance();
        }

        awaitPartitionMapExchange();

        for (Ignite g : G.allGrids()) {
            CacheGroupContext grpCtx = ((IgniteEx)g).cachex(DEFAULT_CACHE_NAME).context().group();

            assertTrue(grpCtx.walEnabled());
        }
    }

    /**
     *
     * @param ig Ignite.
     */
    private void awaitExchange(IgniteEx ig) throws IgniteCheckedException {
        ig.context().cache().context().exchange().lastTopologyFuture().get();
    }

    /**
     * Put random values to cache in multiple threads until time interval given expires.
     *
     * @param cache Cache to modify.
     * @param threadCnt Number ot threads to be used.
     * @param duration Time interval in milliseconds.
     * @throws Exception When something goes wrong.
     */
    private void doLoad(IgniteCache<Integer, Integer> cache, int threadCnt, long duration) throws Exception {
        GridTestUtils.runMultiThreaded(() -> {
            long stopTs = U.currentTimeMillis() + duration;

            int keysCnt = getKeysCount();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            do {
                try {
                    cache.put(rnd.nextInt(keysCnt), rnd.nextInt());
                }
                catch (Exception ex) {
                    MvccFeatureChecker.assertMvccWriteConflict(ex);
                }
            }
            while (U.currentTimeMillis() < stopTs);
        }, threadCnt, "load-cache");
    }

    /**
     *
     */
    private static class TestFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegate;

        /**
         * @param delegate Delegate.
         */
        TestFileIOFactory(FileIOFactory delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new TestFileIO(delegate.create(file, modes));
        }
    }

    /**
     *
     */
    private static class TestFileIO extends AbstractFileIO {
        /** */
        private final FileIO delegate;

        /**
         * @param delegate Delegate.
         */
        TestFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public int getFileSystemBlockSize() {
            return delegate.getFileSystemBlockSize();
        }

        /** {@inheritDoc} */
        @Override public long getSparseSize() {
            return delegate.getSparseSize();
        }

        /** {@inheritDoc} */
        @Override public int punchHole(long position, int len) {
            return delegate.punchHole(position, len);
        }

        /** {@inheritDoc} */
        @Override public long position() throws IOException {
            return delegate.position();
        }

        /** {@inheritDoc} */
        @Override public void position(long newPosition) throws IOException {
            delegate.position(newPosition);
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf) throws IOException {
            return delegate.read(destBuf);
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf, long position) throws IOException {
            return delegate.read(destBuf, position);
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] buf, int off, int len) throws IOException {
            return delegate.read(buf, off, len);
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            CountDownLatch latch = fileIOLatch.get();

            if (latch != null && Thread.currentThread().getName().contains("checkpoint"))
                try {
                    latch.await();
                }
                catch (InterruptedException ex) {
                    throw new IgniteException(ex);
                }

            return delegate.write(srcBuf);
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            CountDownLatch latch = fileIOLatch.get();

            if (latch != null && Thread.currentThread().getName().contains("checkpoint"))
                try {
                    latch.await();
                }
                catch (InterruptedException ex) {
                    throw new IgniteException(ex);
                }

            return delegate.write(srcBuf, position);
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            CountDownLatch latch = fileIOLatch.get();

            if (latch != null && Thread.currentThread().getName().contains("checkpoint"))
                try {
                    latch.await();
                }
                catch (InterruptedException ex) {
                    throw new IgniteException(ex);
                }

            return delegate.write(buf, off, len);
        }

        /** {@inheritDoc} */
        @Override public MappedByteBuffer map(int maxWalSegmentSize) throws IOException {
            return delegate.map(maxWalSegmentSize);
        }

        /** {@inheritDoc} */
        @Override public void force() throws IOException {
            delegate.force();
        }

        /** {@inheritDoc} */
        @Override public void force(boolean withMetadata) throws IOException {
            delegate.force(withMetadata);
        }

        /** {@inheritDoc} */
        @Override public long size() throws IOException {
            return delegate.size();
        }

        /** {@inheritDoc} */
        @Override public void clear() throws IOException {
            delegate.clear();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            delegate.close();
        }
    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return super.getPartitionMapExchangeTimeout() * 2;
    }
}
