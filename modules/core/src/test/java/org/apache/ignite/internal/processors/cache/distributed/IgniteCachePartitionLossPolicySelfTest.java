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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.junit.Assume.assumeFalse;

/**
 *
 */
@RunWith(Parameterized.class)
public class IgniteCachePartitionLossPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 32;

    /** */
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{TRANSACTIONAL});

        if (!MvccFeatureChecker.forcedMvcc())
            params.add(new Object[]{ATOMIC});

        return params;
    }

    /** */
    private static boolean client;

    /** */
    private static int backups;

    /** */
    private static final AtomicBoolean delayPartExchange = new AtomicBoolean(false);

    /** */
    private final TopologyChanger killSingleNode = new TopologyChanger(
        false, singletonList(3), asList(0, 1, 2, 4), 0);

    /** */
    private static boolean isPersistenceEnabled;

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicity;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(10000000L);
        cfg.setClientFailureDetectionTimeout(10000000L);

        cfg.setCommunicationSpi(new TestDelayingCommunicationSpi() {
            /** {@inheritDoc} */
            @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
                return delayPartExchange.get() &&
                    (msg instanceof GridDhtPartitionsFullMessage || msg instanceof GridDhtPartitionsAbstractMessage);
            }

        });

        cfg.setClientMode(client);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setConsistentId(gridName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(isPersistenceEnabled).setMaxSize(50 * 1024 * 1024)
                ).setWalSegmentSize(4 * 1024 * 1024).setWalMode(WALMode.LOG_ONLY));

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(backups);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));
        cacheCfg.setAtomicityMode(atomicity);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // TODO test is needed for logged LOST state.
        cleanPersistenceDir();

        delayPartExchange.set(false);

        backups = 0;

        isPersistenceEnabled = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlySafe() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        checkLostPartition(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlySafeWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        checkLostPartition(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlyAll() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        checkLostPartition(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10041")
    @Test
    public void testReadOnlyAllWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        checkLostPartition(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafe() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        checkLostPartition(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        checkLostPartition(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteAll() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        checkLostPartition(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10041")
    @Test
    public void testReadWriteAllWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        checkLostPartition(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillTwoNodes() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        checkLostPartition(new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillTwoNodesWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        checkLostPartition(new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillTwoNodesWithDelay() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        checkLostPartition(new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 20));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillTwoNodesWithDelayWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        checkLostPartition(new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 20));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsAfterKillThreeNodes() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        backups = 1;

        checkLostPartition(new TopologyChanger(true, asList(3, 2, 1), asList(0, 4), 0));
    }

    /**
     * @throws Exception if failed.
     *
     * TODO same test with full restart in lost state.
     */
    @Test
    public void testReadWriteSafeWithBackupsAfterKillThreeNodesWithPersistence() throws Exception {
        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(new TopologyChanger(true, asList(3, 2, 1), asList(0, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillCrd() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        checkLostPartition(new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillCrdWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        checkLostPartition(new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackups() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        backups = 1;

        checkLostPartition(new TopologyChanger(true, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsWithPersistence() throws Exception {
        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(new TopologyChanger(true, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsAfterKillCrd() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        backups = 1;

        checkLostPartition(new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsAfterKillCrdWithPersistence() throws Exception {
        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgnore() throws Exception {
        checkIgnore(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgnoreKillThreeNodes() throws Exception {
        TopologyChanger onlyCrdIsAlive = new TopologyChanger(false, Arrays.asList(1, 2, 3), Arrays.asList(0, 4), 0);

        checkIgnore(onlyCrdIsAlive);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testResetLostPartitionsWithNoOwners() throws Exception {
        backups = 1;

        isPersistenceEnabled = true;

        final TopologyChanger changer = new TopologyChanger(false, asList(3, 2, 1), asList(0, 4), 0);

        final List<Integer> lostParts = changer.changeTopology();

        assertFalse(lostParts.isEmpty());

        Stream.of(grid(4), grid(0)).forEach(new Consumer<IgniteEx>() {
            @Override public void accept(IgniteEx ex) {
                try {
                    ex.resetLostPartitions(singletonList(DEFAULT_CACHE_NAME));

                    fail();
                }
                catch (ClusterTopologyException ignored) {
                    // Expected.
                }
            }
        });
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testResetBaselineWithLostPartitions() throws Exception {
        backups = 1;

        isPersistenceEnabled = true;

        final TopologyChanger changer = new TopologyChanger(false, asList(3, 2, 1), asList(0, 4), 0);

        final List<Integer> lostParts = changer.changeTopology();

        assertFalse(lostParts.isEmpty());

        resetBaselineTopology(); // Should recreate partitions on new owners in LOST state.

        printPartitionState(DEFAULT_CACHE_NAME, 0);

        grid(4).resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception if failed.
     *
     * TODO FIXME add test for joining node having stale data.
     */
    @Test
    public void testResetBaselineLostPartitionsWithAddedDataNode() throws Exception {
        backups = 1;

        isPersistenceEnabled = true;

        final TopologyChanger changer = new TopologyChanger(false, asList(3, 2, 1), asList(0, 4), 0);

        final List<Integer> lostParts = changer.changeTopology();

        assertFalse(lostParts.isEmpty());

        final IgniteEx g5 = startGrid(5);

        assertEquals(new HashSet<>(lostParts), new HashSet<>(g5.cache(DEFAULT_CACHE_NAME).lostPartitions()));

        resetBaselineTopology();

        // Wait for ideal switch for available partitions.
        final GridDhtPartitionTopology top5 = g5.cachex(DEFAULT_CACHE_NAME).context().topology();
        waitForReadyTopology(top5, new AffinityTopologyVersion(9, 2));

        // Check puts to available partitions.
        for (int p = 0; p < PARTS_CNT; p++) {
            if (!lostParts.contains(p))
                grid(4).cache(DEFAULT_CACHE_NAME).put(p, p);
        }

        // Check owning partitions for joined node are in LOST state.
        final AffinityAssignment ideal =
            grid(4).cachex(DEFAULT_CACHE_NAME).context().affinity().assignment(new AffinityTopologyVersion(9, 2));

        assertTrue(ideal.partitionPrimariesDifferentToIdeal().isEmpty());

        for (int p = 0; p < PARTS_CNT; p++) {
            if (lostParts.contains(p)) {
                final List<ClusterNode> cur = ideal.get(p);

                final int idx = cur.indexOf(grid(5).localNode());

                if (idx >= 0)
                    assertEquals(GridDhtPartitionState.LOST, top5.localPartition(p).state());
            }
        }

        g5.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();

        Stream.of(grid(4), grid(0), grid(5)).forEach(new Consumer<IgniteEx>() {
            @Override public void accept(IgniteEx ex) {
                assertTrue(ex.cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
            }
        });
    }

    /**
     * @param topChanger topology changer.
     * @throws Exception if failed.
     */
    private void checkIgnore(TopologyChanger topChanger) throws Exception {
        topChanger.changeTopology();

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

            Collection<Integer> lost = cache.lostPartitions();

            assertTrue("[grid=" + ig.name() + ", lost=" + lost.toString() + ']', lost.isEmpty());

            int parts = ig.affinity(DEFAULT_CACHE_NAME).partitions();

            for (int i = 0; i < parts; i++) {
                cache.get(i);

                cache.put(i, i);
            }
        }
    }

    /**
     * @param topChanger topology changer.
     * @throws Exception if failed.
     */
    private void checkLostPartition(TopologyChanger topChanger) throws Exception {
        List<Integer> lostParts = topChanger.changeTopology();

        // Wait for all grids (servers and client) have same topology version
        // to make sure that all nodes received map with lost partition.
        boolean success = GridTestUtils.waitForCondition(() -> {
            AffinityTopologyVersion last = null;
            for (Ignite ig : G.allGrids()) {
                AffinityTopologyVersion ver = ((IgniteEx)ig).context().cache().context().exchange().readyAffinityVersion();

                if (last != null && !last.equals(ver))
                    return false;

                last = ver;
            }

            return true;
        }, 10000);

        assertTrue("Failed to wait for new topology", success);

        for (Ignite ig : G.allGrids()) {
            info("Checking node: " + ig.cluster().localNode().id());

            verifyLostPartitions(ig, isPersistenceEnabled, lostParts);

            verifyCacheOps(isPersistenceEnabled, ig);

            // TODO queries must pass for insafe.
            if (isPersistenceEnabled)
                validateQuery(true, ig);
        }

        checkNewNode(true, isPersistenceEnabled);
        checkNewNode(false, isPersistenceEnabled);

        // Bring all nodes back.
        for (int i : topChanger.killNodes) {
            IgniteEx grd = startGrid(i);

            info("Newly started node: " + grd.cluster().localNode().id());

            // Check that partition state does not change after we start each node.
            // TODO With persistence enabled LOST partitions become OWNING after a node joins back - https://issues.apache.org/jira/browse/IGNITE-10044.
            if (!isPersistenceEnabled) {
                for (Ignite ig : G.allGrids()) {
                    verifyCacheOps(isPersistenceEnabled, ig);

                    // TODO Query effectively waits for rebalance due to https://issues.apache.org/jira/browse/IGNITE-10057
                    // TODO and after resetLostPartition there is another OWNING copy in the cluster due to https://issues.apache.org/jira/browse/IGNITE-10058.
                    // TODO Uncomment after https://issues.apache.org/jira/browse/IGNITE-10058 is fixed.
//                    validateQuery(safe, ig);
                }
            }
        }

        ignite(4).resetLostPartitions(singletonList(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange(true, true, null);

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

            assertTrue(cache.lostPartitions().isEmpty());

            int parts = ig.affinity(DEFAULT_CACHE_NAME).partitions();

            for (int i = 0; i < parts; i++) {
                cache.get(i);

                cache.put(i, i);
            }

            for (int i = 0; i < parts; i++) {
                checkQueryPasses(ig, false, i);

                if (shouldExecuteLocalQuery(ig, i))
                    checkQueryPasses(ig, true, i);

            }

            checkQueryPasses(ig, false);
        }
    }

    /**
     * @param client Client flag.
     * @param safe Safe flag.
     * @throws Exception If failed to start a new node.
     */
    private void checkNewNode(
        boolean client,
        boolean safe
    ) throws Exception {
        this.client = client;

        try {
            IgniteEx cl = (IgniteEx)startGrid("newNode");

            CacheGroupContext grpCtx = cl.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

            assertTrue(!safe || grpCtx.needsRecovery());

            verifyCacheOps(safe, cl);

            if (safe)
                validateQuery(true, cl);
        }
        finally {
            stopGrid("newNode", false);

            this.client = false;
        }
    }

    /**
     * @param node Node.
     * @param safe Expect loss.
     * @param lostParts Lost partition IDs.
     */
    private void verifyLostPartitions(Ignite node, boolean safe, List<Integer> lostParts) {
        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

        if (!safe) {
            assertTrue(cache.lostPartitions().isEmpty());

            return;
        }

        Set<Integer> actualSortedLostParts = new TreeSet<>(cache.lostPartitions());
        Set<Integer> expSortedLostParts = new TreeSet<>(lostParts);

        assertEqualsCollections(expSortedLostParts, actualSortedLostParts);
    }

    /**
     * @param safe {@code True} if lost partition should trigger exception.
     * @param ig Ignite instance.
     */
    private void verifyCacheOps(boolean safe, Ignite ig) {
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        int parts = ig.affinity(DEFAULT_CACHE_NAME).partitions();

        // Check read.
        for (int i = 0; i < parts; i++) {
            try {
                Integer actual = cache.get(i);

                if (safe)
                    assertEquals((Integer)i, actual);
            }
            catch (CacheException e) {
                assertTrue("Read exception should only be triggered for a lost partition " +
                    "[ex=" + e + ", part=" + i + ']', cache.lostPartitions().contains(i));
            }
        }

        // Check write.
        if (safe) {
            for (int i = 0; i < parts; i++) {
                try {
                    cache.put(i, i);
                }
                catch (CacheException e) {
                    assertTrue("Write exception should only be triggered for a lost partition: " + e,
                            cache.lostPartitions().contains(i));
                }
            }
        }
    }

    /**
     * @param nodes List of nodes to find partition.
     * @return List of partitions that aren't primary or backup for specified nodes.
     */
    private List<Integer> noPrimaryOrBackupPartition(List<Integer> nodes) {
        Affinity<Object> aff = ignite(4).affinity(DEFAULT_CACHE_NAME);

        List<Integer> parts = new ArrayList<>();

        Integer part;

        for (int i = 0; i < aff.partitions(); i++) {
            part = i;

            for (Integer id : nodes) {
                if (aff.isPrimaryOrBackup(grid(id).cluster().localNode(), i)) {
                    part = null;

                    break;
                }
            }

            if (part != null)
                parts.add(i);
        }

        return parts;
    }

    /**
     * Validate query execution on a node.
     *
     * @param safe Safe flag.
     * @param node Node.
     */
    private void validateQuery(boolean safe, Ignite node) {
        // Get node lost and remaining partitions.
        IgniteCache<?, ?> cache = node.cache(DEFAULT_CACHE_NAME);

        Collection<Integer> lostParts = cache.lostPartitions();

        int part = cache.lostPartitions().stream().findFirst().orElseThrow(AssertionError::new);

        Integer remainingPart = null;

        for (int i = 0; i < node.affinity(DEFAULT_CACHE_NAME).partitions(); i++) {
            if (lostParts.contains(i))
                continue;

            remainingPart = i;

            break;
        }

        assertNotNull("Failed to find a partition that isn't lost", remainingPart);

        // 1. Check query against all partitions.
        validateQuery0(safe, node);

        // 2. Check query against LOST partition.
        validateQuery0(safe, node, part);

        // 3. Check query on remaining partition.
        checkQueryPasses(node, false, remainingPart);

        if (shouldExecuteLocalQuery(node, remainingPart))
            checkQueryPasses(node, true, remainingPart);

        // 4. Check query over two partitions - normal and LOST.
        validateQuery0(safe, node, part, remainingPart);
    }

    /**
     * Query validation routine.
     *
     * @param safe Safe flag.
     * @param node Node.
     * @param parts Partitions.
     */
    private void validateQuery0(boolean safe, Ignite node, int... parts) {
        if (safe)
            checkQueryFails(node, false, parts);
        else
            checkQueryPasses(node, false, parts);

        if (shouldExecuteLocalQuery(node, parts)) {
            if (safe)
                checkQueryFails(node, true, parts);
            else
                checkQueryPasses(node, true, parts);
        }
    }

    /**
     * @return true if the given node is primary for all given partitions.
     */
    private boolean shouldExecuteLocalQuery(Ignite node, int... parts) {
        if (parts == null || parts.length == 0)
            return false;

        int numOfPrimaryParts = 0;

        for (int nodePrimaryPart : node.affinity(DEFAULT_CACHE_NAME).primaryPartitions(node.cluster().localNode())) {
            for (int part : parts) {
                if (part == nodePrimaryPart)
                    numOfPrimaryParts++;
            }
        }

        return numOfPrimaryParts == parts.length;
    }

    /**
     * @param node Node.
     * @param loc Local flag.
     * @param parts Partitions.
     */
    protected void checkQueryPasses(Ignite node, boolean loc, int... parts) {
        // Scan queries don't support multiple partitions.
        if (parts != null && parts.length > 1)
            return;

        // TODO Local scan queries fail in non-safe modes - https://issues.apache.org/jira/browse/IGNITE-10059.
        if (loc)
            return;

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        ScanQuery qry = new ScanQuery();

        if (parts != null && parts.length > 0)
            qry.setPartition(parts[0]);

        if (loc)
            qry.setLocal(true);

        cache.query(qry).getAll();
    }

    /**
     * @param node Node.
     * @param loc Local flag.
     * @param parts Partitions.
     */
    protected void checkQueryFails(Ignite node, boolean loc, int... parts) {
        if (parts.length == 0)
            return;

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        String msg = loc ? "forced local query" : "partition has been lost";
        GridTestUtils.assertThrows(log, () -> {
            List res = null;
            for (int partition : parts) {
                ScanQuery qry = new ScanQuery();
                qry.setPartition(partition);

                if (loc)
                    qry.setLocal(true);

                res = cache.query(qry).getAll();
            }

            return res;
        }, IgniteCheckedException.class, msg);
    }

    /** */
    private class TopologyChanger {
        /** Flag to delay partition exchange */
        private boolean delayExchange;

        /** List of nodes to kill */
        private List<Integer> killNodes;

        /** List of nodes to be alive */
        private List<Integer> aliveNodes;

        /** Delay between node stops */
        private long stopDelay;

        /**
         * @param delayExchange Flag for delay partition exchange.
         * @param killNodes List of nodes to kill.
         * @param aliveNodes List of nodes to be alive.
         * @param stopDelay Delay between stopping nodes.
         */
        private TopologyChanger(boolean delayExchange, List<Integer> killNodes, List<Integer> aliveNodes,
            long stopDelay) {
            this.delayExchange = delayExchange;
            this.killNodes = killNodes;
            this.aliveNodes = aliveNodes;
            this.stopDelay = stopDelay;
        }

        /**
         * @return Lost partition ID.
         * @throws Exception If failed.
         */
        private List<Integer> changeTopology() throws Exception {
            startGrids(4);

            TestRecordingCommunicationSpi.spi(grid(0)).record(GridDhtPartitionsFullMessage.class);

            if (isPersistenceEnabled)
                grid(0).cluster().active(true);

            Affinity<Object> aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

            for (int i = 0; i < aff.partitions(); i++)
                ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i);

            client = true;

            startGrid(4);

            client = false;

            for (int i = 0; i < 5; i++)
                info(">>> Node [idx=" + i + ", nodeId=" + ignite(i).cluster().localNode().id() + ']');

            awaitPartitionMapExchange();

            final List<Integer> parts = noPrimaryOrBackupPartition(aliveNodes);

            if (parts.isEmpty())
                throw new IllegalStateException("No partition on nodes: " + killNodes);

            final List<Map<Integer, Semaphore>> lostMap = new ArrayList<>();

            for (int i : aliveNodes) {
                HashMap<Integer, Semaphore> semaphoreMap = new HashMap<>();

                for (Integer part : parts)
                    semaphoreMap.put(part, new Semaphore(0));

                lostMap.add(semaphoreMap);

                grid(i).events().localListen(new P1<Event>() {
                    @Override public boolean apply(Event evt) {
                        assert evt.type() == EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

                        CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent)evt;

                        if (F.eq(DEFAULT_CACHE_NAME, cacheEvt.cacheName())) {
                            if (semaphoreMap.containsKey(cacheEvt.partition()))
                                semaphoreMap.get(cacheEvt.partition()).release();
                        }

                        return true;
                    }
                }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);
            }

            if (delayExchange)
                delayPartExchange.set(true);

            ExecutorService executor = Executors.newFixedThreadPool(killNodes.size());

            for (Integer node : killNodes) {
                executor.submit(new Runnable() {
                    @Override public void run() {
                        grid(node).close();
                    }
                });

                doSleep(stopDelay);
            }

            executor.shutdown();

            delayPartExchange.set(false);

            doSleep(5_000L);

            // Events are triggered only in persistent mode.
            if (isPersistenceEnabled) {
                for (Map<Integer, Semaphore> map : lostMap) {
                    for (Map.Entry<Integer, Semaphore> entry : map.entrySet())
                        assertTrue("Failed to wait for partition LOST event for partition: " + entry.getKey(), entry.getValue().tryAcquire(1));
                }

                for (Map<Integer, Semaphore> map : lostMap) {
                    for (Map.Entry<Integer, Semaphore> entry : map.entrySet())
                        assertFalse("Partition LOST event raised twice for partition: " + entry.getKey(), entry.getValue().tryAcquire(1));
                }
            }

            return parts;
        }
    }
}
