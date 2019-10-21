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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.cluster.ClusterState.READ_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class IgniteClusterActivateDeactivateTest extends GridCommonAbstractTest {
    /** */
    static final String CACHE_NAME_PREFIX = "cache-";

    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";
    /** */
    private static final int DEFAULT_CACHES_COUNT = 2;

    /** */
    boolean client;

    /** */
    private ClusterState stateOnStart = ACTIVE;

    /** */
    CacheConfiguration[] ccfgs;

    /** */
    private boolean testSpi;

    /** */
    private boolean testReconnectSpi;

    /** */
    private Class[] testSpiRecord;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (testReconnectSpi) {
            TcpDiscoverySpi spi = new IgniteClientReconnectAbstractTest.TestTcpDiscoverySpi();

            cfg.setDiscoverySpi(spi);

            spi.setJoinTimeout(2 * 60_000);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(sharedStaticIpFinder);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setClientMode(client);

        cfg.setClusterStateOnStart(stateOnStart);

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        DataStorageConfiguration memCfg = new DataStorageConfiguration();
        memCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(150L * 1024 * 1024)
            .setPersistenceEnabled(persistenceEnabled()));
        memCfg.setWalSegments(2);
        memCfg.setWalSegmentSize(512 * 1024);

        memCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setMaxSize(150L * 1024 * 1024)
            .setName(NO_PERSISTENCE_REGION)
            .setPersistenceEnabled(false));

        if (persistenceEnabled())
            memCfg.setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);
        cfg.setFailureDetectionTimeout(60_000);

        if (testSpi) {
            TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

            if (testSpiRecord != null)
                spi.record(testSpiRecord);

            cfg.setCommunicationSpi(spi);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @return {@code True} if test with persistence.
     */
    protected boolean persistenceEnabled() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_SingleNode() throws Exception {
        activateSimple(1, 0, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_SingleNode() throws Exception {
        activateSimple(1, 0, 0, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_5_Servers() throws Exception {
        activateSimple(5, 0, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_5_Servers() throws Exception {
        activateSimple(5, 0, 0, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_5_Servers2() throws Exception {
        activateSimple(5, 0, 4, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_5_Servers2() throws Exception {
        activateSimple(5, 0, 4, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_5_Servers_5_Clients() throws Exception {
        activateSimple(5, 4, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_5_Servers_5_Clients() throws Exception {
        activateSimple(5, 4, 0, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        activateSimple(5, 4, 6, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateInReadOnlySimple_5_Servers_5_Clients_FromClient() throws Exception {
        activateSimple(5, 4, 6, READ_ONLY);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param activateFrom Index of node stating activation.
     * @param state Activation state.
     * @throws Exception If failed.
     */
    private void activateSimple(int srvs, int clients, int activateFrom, ClusterState state) throws Exception {
        assertTrue(state.toString(), ClusterState.active(state));

        changeStateSimple(srvs, clients, activateFrom, INACTIVE, state);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReActivateSimple_5_Servers_4_Clients_FromClient() throws Exception {
        reactivateSimple(5, 4, 6, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReActivateInReadOnlySimple_5_Servers_4_Clients_FromClient() throws Exception {
        reactivateSimple(5, 4, 6, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReActivateSimple_5_Servers_4_Clients_FromServer() throws Exception {
        reactivateSimple(5, 4, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReActivateInReadOnlySimple_5_Servers_4_Clients_FromServer() throws Exception {
        reactivateSimple(5, 4, 0, READ_ONLY);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param activateFrom Index of node stating activation.
     * @param state Activation state.
     * @throws Exception If failed.
     */
    public void reactivateSimple(int srvs, int clients, int activateFrom, ClusterState state) throws Exception {
        activateSimple(srvs, clients, activateFrom, state);

        if (state == ACTIVE)
            rolloverSegmentAtLeastTwice(activateFrom);

        for (int i = 0; i < srvs + clients; i++) {
            for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);

            checkCache(ignite(i), CU.UTILITY_CACHE_NAME, true);
        }

        ignite(activateFrom).cluster().state(INACTIVE);

        ignite(activateFrom).cluster().state(state);

        if (state == ACTIVE)
            rolloverSegmentAtLeastTwice(activateFrom);

        for (int i = 0; i < srvs + clients; i++) {
            for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);

            checkCache(ignite(i), CU.UTILITY_CACHE_NAME, true);
        }
    }

    /**
     * Work directory have 2 segments by default. This method do full circle.
     */
    private void rolloverSegmentAtLeastTwice(int activateFrom) {
        for (int c = 0; c < DEFAULT_CACHES_COUNT; c++) {
            IgniteCache<Object, Object> cache = ignite(activateFrom).cache(CACHE_NAME_PREFIX + c);
            //this should be enough including free-,meta- page and etc.
            for (int i = 0; i < 1000; i++)
                cache.put(i, i);
        }
    }

    /**
     * @param nodes Number of nodes.
     * @param caches Number of caches.
     */
    final void checkCaches(int nodes, int caches) throws InterruptedException {
        checkCaches(nodes, caches, true);
    }

    /**
     * @param nodes Number of nodes.
     * @param caches Number of caches.
     */
    final void checkCaches(int nodes, int caches, boolean awaitExchange) throws InterruptedException {
        if (awaitExchange)
            awaitPartitionMapExchange();

        for (int i = 0; i < nodes; i++) {
            for (int c = 0; c < caches; c++) {
                IgniteCache<Integer, Integer> cache = ignite(i).cache(CACHE_NAME_PREFIX + c);

                for (int j = 0; j < 10; j++) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Integer key = rnd.nextInt(1000);

                    cache.put(key, j);

                    assertEquals((Integer)j, cache.get(key));
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivate1_Server() throws Exception {
        joinWhileActivate1(false, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivateInReadOnly1_Server() throws Exception {
        joinWhileActivate1(false, false, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivate1_WithCache_Server() throws Exception {
        joinWhileActivate1(false, true, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivateInReadOnly1_WithCache_Server() throws Exception {
        joinWhileActivate1(false, true, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivate1_Client() throws Exception {
        joinWhileActivate1(true, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileActivateInReadOnly1_Client() throws Exception {
        joinWhileActivate1(true, false, READ_ONLY);
    }

    /**
     * @param startClient If {@code true} joins client node, otherwise server.
     * @param withNewCache If {@code true} joining node has new cache in configuration.
     * @param state If {@code true} joining node has new cache in configuration.
     * @throws Exception If failed.
     */
    private void joinWhileActivate1(boolean startClient, boolean withNewCache, ClusterState state) throws Exception {
        assertTrue(ClusterState.active(state));

        IgniteInternalFuture<?> activeFut = startNodesAndBlockStatusChange(2, 0, 0, INACTIVE, state);

        IgniteInternalFuture<?> startFut = runAsync((Callable<Void>)() -> {
            client = startClient;

            ccfgs = withNewCache ? cacheConfigurations2() : cacheConfigurations1();

            startGrid(2);

            return null;
        });

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(ignite(1));

        spi1.stopBlock();

        activeFut.get();
        startFut.get();

        for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
            checkCache(ignite(2), CACHE_NAME_PREFIX + c, true);

        if (withNewCache) {
            for (int i = 0; i < 3; i++) {
                for (int c = 0; c < 4; c++)
                    checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
            }
        }

        awaitPartitionMapExchange();

        if (state == ACTIVE)
            checkCaches(3, withNewCache ? 4 : 2);

        client = false;

        startGrid(3);

        if (state == ACTIVE)
            checkCaches(4, withNewCache ? 4 : 2);

        client = true;

        startGrid(4);

        if (state == ACTIVE)
            checkCaches(5, withNewCache ? 4 : 2);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param stateChangeFrom Index of node initiating changes.
     * @param initialState  Cluster state on start nodes.
     * @param targetState  State of started cluster.
     * @param blockMsgNodes Nodes whcis block exchange messages.
     * @return State change future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture<?> startNodesAndBlockStatusChange(
        int srvs,
        int clients,
        final int stateChangeFrom,
        final ClusterState initialState,
        final ClusterState targetState,
        int... blockMsgNodes
    ) throws Exception {
        checkStatesAreDifferent(initialState, targetState);

        stateOnStart = initialState;
        testSpi = true;

        startWithCaches1(srvs, clients);

        int minorVer = 1;

        if (ClusterState.active(initialState)) {
            ignite(0).cluster().state(initialState);

            awaitPartitionMapExchange();

            minorVer++;
        }

        if (blockMsgNodes.length == 0)
            blockMsgNodes = new int[] {1};

        final AffinityTopologyVersion STATE_CHANGE_TOP_VER = new AffinityTopologyVersion(srvs + clients, minorVer);

        List<TestRecordingCommunicationSpi> spis = new ArrayList<>();

        for (int idx : blockMsgNodes) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite(idx));

            spis.add(spi);

            blockExchangeSingleMessage(spi, STATE_CHANGE_TOP_VER);
        }

        IgniteInternalFuture<?> stateChangeFut = runAsync(() ->
            ignite(stateChangeFrom).cluster().state(targetState)
        );

        for (TestRecordingCommunicationSpi spi : spis)
            spi.waitForBlocked();

        U.sleep(500);

        assertFalse(stateChangeFut.isDone());

        return stateChangeFut;
    }

    /**
     * @param spi SPI.
     * @param topVer Exchange topology version.
     */
    private void blockExchangeSingleMessage(TestRecordingCommunicationSpi spi, final AffinityTopologyVersion topVer) {
        spi.blockMessages((IgniteBiPredicate<ClusterNode, Message>)(clusterNode, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage pMsg = (GridDhtPartitionsSingleMessage)msg;

                if (pMsg.exchangeId() != null && pMsg.exchangeId().topologyVersion().equals(topVer))
                    return true;
            }

            return false;
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivate1_Server() throws Exception {
        joinWhileDeactivate1(false, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivateFromReadOnly1_Server() throws Exception {
        joinWhileDeactivate1(false, false, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivate1_WithCache_Server() throws Exception {
        joinWhileDeactivate1(false, true, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivateFromReadOnly1_WithCache_Server() throws Exception {
        joinWhileDeactivate1(false, true, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivate1_Client() throws Exception {
        joinWhileDeactivate1(true, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWhileDeactivateFromReadOnly1_Client() throws Exception {
        joinWhileDeactivate1(true, false, READ_ONLY);
    }

    /**
     * @param startClient If {@code true} joins client node, otherwise server.
     * @param withNewCache If {@code true} joining node has new cache in configuration.
     * @param state Initial cluster state.
     * @throws Exception If failed.
     */
    private void joinWhileDeactivate1(boolean startClient, boolean withNewCache, ClusterState state) throws Exception {
        assertTrue(ClusterState.active(state));

        IgniteInternalFuture<?> activeFut = startNodesAndBlockStatusChange(2, 0, 0, state, INACTIVE);

        IgniteInternalFuture<?> startFut = runAsync((Callable<Void>)() -> {
            client = startClient;

            ccfgs = withNewCache ? cacheConfigurations2() : cacheConfigurations1();

            startGrid(2);

            return null;
        });

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(ignite(1));

        spi1.stopBlock();

        activeFut.get();
        startFut.get();

        checkNoCaches(3);

        ignite(2).cluster().state(state);

        for (int c = 0; c < DEFAULT_CACHES_COUNT; c++)
            checkCache(ignite(2), CACHE_NAME_PREFIX + c, true);

        if (withNewCache) {
            for (int i = 0; i < 3; i++) {
                for (int c = 0; c < 4; c++)
                    checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
            }
        }

        awaitPartitionMapExchange();

        if (state == ACTIVE)
            checkCaches(3, withNewCache ? 4 : 2);

        client = false;

        startGrid(3);

        if (state == ACTIVE)
            checkCaches(4, withNewCache ? 4 : 2);

        client = true;

        startGrid(4);

        if (state == ACTIVE)
            checkCaches(5, withNewCache ? 4 : 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentJoinAndActivate() throws Exception {
        testConcurrentJoinAndActivate(ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentJoinAndActivateInReadOnly() throws Exception {
        testConcurrentJoinAndActivate(READ_ONLY);
    }

    /** */
    private void testConcurrentJoinAndActivate(ClusterState activateState) throws Exception {
        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            stateOnStart = INACTIVE;

            for (int i = 0; i < 3; i++) {
                ccfgs = cacheConfigurations1();

                startGrid(i);
            }

            final int START_NODES = 3;

            final CyclicBarrier b = new CyclicBarrier(START_NODES + 1);

            IgniteInternalFuture<Void> fut1 = runAsync(() -> {
                b.await();

                U.sleep(ThreadLocalRandom.current().nextLong(100) + 1);

                ignite(0).cluster().state(activateState);

                return null;
            });

            final AtomicInteger nodeIdx = new AtomicInteger(3);

            IgniteInternalFuture<Long> fut2 = GridTestUtils.runMultiThreadedAsync((Callable<Void>)() -> {
                int idx = nodeIdx.getAndIncrement();

                b.await();

                startGrid(idx);

                return null;
            }, START_NODES, "start-node");

            fut1.get();
            fut2.get();

            if (activateState == ACTIVE)
                checkCaches(6, 2);

            afterTest();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_SingleNode() throws Exception {
        deactivateSimple(1, 0, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_SingleNode() throws Exception {
        deactivateSimple(1, 0, 0, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_5_Servers() throws Exception {
        deactivateSimple(5, 0, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_5_Servers() throws Exception {
        deactivateSimple(5, 0, 0, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_5_Servers2() throws Exception {
        deactivateSimple(5, 0, 4, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_5_Servers2() throws Exception {
        deactivateSimple(5, 0, 4, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_5_Servers_5_Clients() throws Exception {
        deactivateSimple(5, 4, 0, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_5_Servers_5_Clients() throws Exception {
        deactivateSimple(5, 4, 0, READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        deactivateSimple(5, 4, 6, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFromReadOnlySimple_5_Servers_5_Clients_FromClient() throws Exception {
        deactivateSimple(5, 4, 6, READ_ONLY);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param changeFrom Index of node starting cluster state change from {@code initialState} to {@code targetState}.
     * @param initialState Initial cluster state.
     * @param targetState Target cluster state.
     * @throws Exception If failed.
     */
    private void changeStateSimple(
        int srvs,
        int clients,
        int changeFrom,
        ClusterState initialState,
        ClusterState targetState
    ) throws Exception {
        checkStatesAreDifferent(initialState, targetState);

        stateOnStart = initialState;

        int nodesCnt = srvs + clients;

        startWithCaches1(srvs, clients);

        if (!ClusterState.active(initialState))
            checkNoCaches(nodesCnt);

        checkClusterState(nodesCnt, initialState);

        ignite(changeFrom).cluster().state(initialState); // Should be no-op.
        ignite(changeFrom).cluster().state(targetState);

        checkClusterState(nodesCnt, targetState);

        if (ClusterState.active(targetState)) {
            for (int i = 0; i < nodesCnt; i++)
                checkCachesOnNode(i, DEFAULT_CACHES_COUNT);

            if (targetState == ACTIVE)
                checkCaches(nodesCnt, DEFAULT_CACHES_COUNT);
        }
        else
            checkNoCaches(nodesCnt);

        nodesCnt = startNodeAndCheckCaches(nodesCnt, false);
        nodesCnt = startNodeAndCheckCaches(nodesCnt, true);

        if (!ClusterState.active(targetState)) {
            checkNoCaches(nodesCnt);

            checkClusterState(nodesCnt, targetState);

            ignite(changeFrom).cluster().state(initialState);

            checkClusterState(nodesCnt, initialState);

            for (int i = 0; i < nodesCnt; i++) {
                if (ignite(i).configuration().isClientMode())
                    checkCache(ignite(i), CU.UTILITY_CACHE_NAME, true);
                else
                    checkCachesOnNode(i, DEFAULT_CACHES_COUNT);
            }
        }
    }

    /** */
    private int startNodeAndCheckCaches(int nodeNumber, boolean client) throws Exception {
        int nodesCnt = nodeNumber;

        this.client = client;

        startGrid(nodeNumber);

        nodesCnt++;

        ClusterState state = grid(0).cluster().state();

        if (ClusterState.active(state)) {
            checkCachesOnNode(nodeNumber, DEFAULT_CACHES_COUNT);

            if (state == ACTIVE)
                checkCaches(nodeNumber + 1, DEFAULT_CACHES_COUNT);
        }

        return nodesCnt;
    }

    /** */
    private void checkClusterState(int nodesCnt, ClusterState state) {
        for (int i = 0; i < nodesCnt; i++)
            assertEquals(ignite(i).name(), state, ignite(i).cluster().state());
    }

    /** */
    private void checkCachesOnNode(int nodeNumber, int cachesCnt) throws IgniteCheckedException {
        for (int c = 0; c < cachesCnt; c++)
            checkCache(ignite(nodeNumber), CACHE_NAME_PREFIX + c, true);

        checkCache(ignite(nodeNumber), CU.UTILITY_CACHE_NAME, true);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param deactivateFrom Index of node stating deactivation.
     * @param initialState Initial cluster state.
     * @throws Exception If failed.
     */
    private void deactivateSimple(int srvs, int clients, int deactivateFrom, ClusterState initialState) throws Exception {
        assertTrue(initialState + "", ClusterState.active(initialState));

        changeStateSimple(srvs, clients, deactivateFrom, initialState, INACTIVE);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @throws Exception If failed.
     */
    private void startWithCaches1(int srvs, int clients) throws Exception {
        for (int i = 0; i < srvs + clients; i++) {
            ccfgs = cacheConfigurations1();

            client = i >= srvs;

            startGrid(i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActive() throws Exception {
        testClientReconnect(ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActiveReadOnly() throws Exception {
        testClientReconnect(READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterInactive() throws Exception {
        testClientReconnect(INACTIVE);
    }

    /** */
    private void testClientReconnect(ClusterState initialState) throws Exception {
        testReconnectSpi = true;

        stateOnStart = initialState;

        ccfgs = cacheConfigurations1();

        final int SRVS = 3;
        final int CLIENTS = 3;
        int nodesCnt = SRVS + CLIENTS;

        startWithCaches1(SRVS, CLIENTS);

        if (persistenceEnabled() && ClusterState.active(initialState))
            ignite(0).cluster().state(initialState);

        Ignite srv = ignite(0);
        Ignite client = ignite(SRVS);

        if (ClusterState.active(initialState)) {
            checkCache(client, CU.UTILITY_CACHE_NAME, true);

            if (initialState == ACTIVE)
                checkCaches(nodesCnt);
        }
        else
            checkNoCaches(nodesCnt);

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, client, srv, null);

        if (!ClusterState.active(initialState)) {
            checkNoCaches(nodesCnt);

            srv.cluster().state(ACTIVE);
        }

        checkCache(client, CU.UTILITY_CACHE_NAME, true);

        checkCaches(nodesCnt);

        nodesCnt = startNodeAndCheckCaches(nodesCnt, false);
        nodesCnt = startNodeAndCheckCaches(nodesCnt, true);

        checkCaches(nodesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterDeactivated() throws Exception {
        clientReconnectClusterState(ACTIVE, INACTIVE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterDeactivatedFromReadOnly() throws Exception {
        clientReconnectClusterState(READ_ONLY, INACTIVE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterDeactivateInProgress() throws Exception {
        clientReconnectClusterState(ACTIVE, INACTIVE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterDeactivateFromReadOnlyInProgress() throws Exception {
        clientReconnectClusterState(READ_ONLY, INACTIVE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActivated() throws Exception {
        clientReconnectClusterState(INACTIVE, ACTIVE, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActivatedReadOnly() throws Exception {
        clientReconnectClusterState(INACTIVE, READ_ONLY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActivateInProgress() throws Exception {
        clientReconnectClusterState(INACTIVE, ACTIVE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectClusterActivateReadOnlyInProgress() throws Exception {
        clientReconnectClusterState(INACTIVE, READ_ONLY, true);
    }

    /**
     * @param initialState Initial cluster state.
     * @param targetState Cluster state after transition.
     * @param transition If {@code true} client reconnects while cluster state transition is in progress.
     * @throws Exception If failed.
     */
    private void clientReconnectClusterState(
        ClusterState initialState,
        ClusterState targetState,
        final boolean transition
    ) throws Exception {
        testReconnectSpi = true;
        testSpi = transition;
        stateOnStart = initialState;

        final int SRVS = 3;
        final int CLIENTS = 3;
        int nodesCnt = SRVS + CLIENTS;

        startWithCaches1(SRVS, CLIENTS);

        final Ignite srv = ignite(0);
        IgniteEx client = grid(SRVS);

        if (persistenceEnabled() && ClusterState.active(initialState))
            ignite(0).cluster().state(initialState);

        if (ClusterState.active(initialState)) {
            checkCache(client, CU.UTILITY_CACHE_NAME, true);

            if (initialState == ACTIVE)
                checkCaches(nodesCnt);

            // Wait for late affinity assignment to finish.
            awaitPartitionMapExchange();
        }
        else
            checkNoCaches(nodesCnt);

        final AffinityTopologyVersion STATE_CHANGE_TOP_VER = new AffinityTopologyVersion(nodesCnt + 1, 1);

        final TestRecordingCommunicationSpi spi1 = transition ? TestRecordingCommunicationSpi.spi(ignite(1)) : null;

        final AtomicReference<IgniteInternalFuture> stateFut = new AtomicReference<>();

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, client, srv, () -> {
            if (transition) {
                blockExchangeSingleMessage(spi1, STATE_CHANGE_TOP_VER);

                stateFut.set(runAsync(() -> srv.cluster().state(targetState), initialState + "->" + targetState));

                try {
                    U.sleep(500);
                }
                catch (IgniteInterruptedCheckedException e) {
                    U.error(log, e);
                }
            }
            else
                srv.cluster().state(targetState);
        });

        if (transition) {
            assertFalse(stateFut.get().isDone());

            assertTrue(client.context().state().clusterState().transition());

            // Public API method would block forever because we blocked the exchange message.
            assertEquals(targetState, client.context().state().publicApiState(false));

            spi1.waitForBlocked();

            spi1.stopBlock();

            stateFut.get().get();
        }

        if (!ClusterState.active(targetState)) {
            checkNoCaches(nodesCnt);

            ignite(0).cluster().state(initialState);

            checkClusterState(nodesCnt, initialState);
        }

        checkCache(client, CU.UTILITY_CACHE_NAME, true);

        if (client.cluster().state() == ACTIVE)
            checkCaches(nodesCnt);

        checkCache(client, CACHE_NAME_PREFIX + 0, true);

        startGrid(nodesCnt++, false);
        startGrid(nodesCnt++, true);

        if (client.cluster().state() == ACTIVE)
            checkCaches(nodesCnt);
    }

    /** */
    private void startGrid(int nodeNumber, boolean client) throws Exception {
        this.client = client;

        startGrid(nodeNumber);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInactiveTopologyChanges() throws Exception {
        checkInactiveTopologyChanges(ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInactiveTopologyChangesReadOnly() throws Exception {
        checkInactiveTopologyChanges(READ_ONLY);
    }

    /** */
    private void checkInactiveTopologyChanges(ClusterState state) throws Exception {
        assertTrue(state + "", ClusterState.active(state));

        testSpi = true;

        testSpiRecord = new Class[] {GridDhtPartitionsSingleMessage.class, GridDhtPartitionsFullMessage.class};

        stateOnStart = INACTIVE;

        final int SRVS = 4;
        final int CLIENTS = 4;
        int nodesCnt = SRVS + CLIENTS;

        startWithCaches1(SRVS, CLIENTS);

        checkRecordedMessages(false);

        for (int i = 0; i < 2; i++) {
            stopGrid(i);

            startGrid(i, false);
        }

        checkRecordedMessages(false);

        for (int i = 0; i < 2; i++) {
            stopGrid(SRVS + i);

            startGrid(SRVS + i, true);
        }

        checkRecordedMessages(false);

        ignite(0).cluster().state(state);

        if (state == ACTIVE)
            checkCaches(nodesCnt);

        checkRecordedMessages(true);

        startGrid(nodesCnt++, false);
        startGrid(nodesCnt++, true);

        checkRecordedMessages(true);

        if (state == ACTIVE)
            checkCaches(nodesCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateFailover1() throws Exception {
        stateChangeFailover1(INACTIVE, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFailover1() throws Exception {
        stateChangeFailover1(ACTIVE, INACTIVE);
    }

    /**
     * @param activate If {@code true} tests activation, otherwise deactivation.
     * @throws Exception If failed.
     */
    private void stateChangeFailover1(ClusterState initialState, ClusterState targetState) throws Exception {

        // Nodes 1 and 4 do not reply to coordinator.
        IgniteInternalFuture<?> fut = startNodesAndBlockStatusChange(4, 4, 3, initialState, targetState , 1, 4);

        client = false;

        // Start one more node while transition is in progress.
        IgniteInternalFuture<Void> startFut = runAsync(() -> {
            startGrid(8);

            return null;
        }, "start-node");

        U.sleep(500);

        stopGrid(getTestIgniteInstanceName(1), true, false);
        stopGrid(getTestIgniteInstanceName(4), true, false);

        fut.get();
        startFut.get();

        startGrid(1, false);
        startGrid(4, true);

        if (!ClusterState.active(targetState)) {
            checkNoCaches(9);

            ignite(0).cluster().state(initialState);
        }

        checkCaches(9);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateFailover2() throws Exception {
        stateChangeFailover2(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFailover2() throws Exception {
        stateChangeFailover2(false);
    }

    /**
     * @param activate If {@code true} tests activation, otherwise deactivation.
     * @throws Exception If failed.
     */
    private void stateChangeFailover2(boolean activate) throws Exception {
        ClusterState initialState = !activate ? INACTIVE : ACTIVE;
        ClusterState targetState = initialState == ACTIVE ? INACTIVE : ACTIVE;
        // Nodes 1 and 4 do not reply to coordinator.
        IgniteInternalFuture<?> fut = startNodesAndBlockStatusChange(4, 4, 3, initialState, targetState, 1, 4);

        client = false;

        // Start more nodes while transition is in progress.
        IgniteInternalFuture<Void> startFut1 = runAsync(() -> {
            startGrid(8);

            return null;
        }, "start-node1");

        IgniteInternalFuture<Void> startFut2 = runAsync(() -> {
            startGrid(9);

            return null;
        }, "start-node2");

        U.sleep(500);

        // Stop coordinator.
        stopGrid(getTestIgniteInstanceName(0), true, false);

        stopGrid(getTestIgniteInstanceName(1), true, false);
        stopGrid(getTestIgniteInstanceName(4), true, false);

        fut.get();

        startFut1.get();
        startFut2.get();

        client = false;

        startGrid(0);
        startGrid(1);

        client = true;

        startGrid(4);

        if (!activate) {
            checkNoCaches(10);

            ignite(0).cluster().active(true);
        }

        checkCaches(10);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateFailover3() throws Exception {
        stateChangeFailover3(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateFailover3() throws Exception {
        stateChangeFailover3(false);
    }

    /**
     * @param activate If {@code true} tests activation, otherwise deactivation.
     * @throws Exception If failed.
     */
    private void stateChangeFailover3(boolean activate) throws Exception {
        ClusterState initialState = !activate ? INACTIVE : ACTIVE;
        ClusterState targetState = initialState == ACTIVE ? INACTIVE : ACTIVE;

        testReconnectSpi = true;

        startNodesAndBlockStatusChange(4, 0, 0, initialState, targetState);

        client = false;

        IgniteInternalFuture<?> startFut1 = runAsync(() -> {
            startGrid(4);

            return null;
        }, "start-node1");

        IgniteInternalFuture<?> startFut2 = runAsync(() -> {
            startGrid(5);

            return null;
        }, "start-node2");

        U.sleep(1000);

        // Stop all nodes participating in state change and not allow last node to finish exchange.
        for (int i = 0; i < 4; i++)
            ((IgniteDiscoverySpi)ignite(i).configuration().getDiscoverySpi()).simulateNodeFailure();

        for (int i = 0; i < 4; i++)
            stopGrid(getTestIgniteInstanceName(i), true, false);

        startFut1.get();
        startFut2.get();

        assertFalse(ignite(4).cluster().active());
        assertFalse(ignite(5).cluster().active());

        ignite(4).cluster().active(true);

        doFinalChecks();
    }

    /**
     * Verifies correctness of cache operations when working in in-memory mode.
     */
    protected void doFinalChecks() throws Exception {
        for (int i = 0; i < 4; i++)
            startGrid(i);

        checkCaches(6);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterStateNotWaitForDeactivation() throws Exception {
        testSpi = true;

        final int nodes = 2;

        IgniteEx crd = (IgniteEx)startGrids(nodes);

        crd.cluster().active(true);

        AffinityTopologyVersion curTopVer = crd.context().discovery().topologyVersionEx();

        AffinityTopologyVersion deactivationTopVer = new AffinityTopologyVersion(
            curTopVer.topologyVersion(),
            curTopVer.minorTopologyVersion() + 1
        );

        for (int gridIdx = 0; gridIdx < nodes; gridIdx++) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(gridIdx));

            blockExchangeSingleMessage(spi, deactivationTopVer);
        }

        IgniteInternalFuture deactivationFut = runAsync(() -> crd.cluster().active(false));

        // Wait for deactivation start.
        GridTestUtils.waitForCondition(() -> {
            DiscoveryDataClusterState clusterState = crd.context().state().clusterState();

            return clusterState.transition() && !clusterState.active();
        }, getTestTimeout());

        // Check that deactivation transition wait is not happened.
        Assert.assertFalse(crd.context().state().publicApiActiveState(true));

        for (int gridIdx = 0; gridIdx < nodes; gridIdx++) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(gridIdx));

            spi.stopBlock();
        }

        deactivationFut.get();
    }

    /**
     * @param exp If {@code true} there should be recorded messages.
     */
    private void checkRecordedMessages(boolean exp) {
        for (Ignite node : G.allGrids()) {
            List<Object> recorded =
                TestRecordingCommunicationSpi.spi(node).recordedMessages(false);

            if (exp)
                assertFalse(F.isEmpty(recorded));
            else
                assertTrue(F.isEmpty(recorded));
        }
    }

    /**
     * @param nodes Expected nodes number.
     */
    private void checkCaches(int nodes) throws InterruptedException {
        checkCaches(nodes, 2, false);
    }

    /**
     * @return Cache configurations.
     */
    final CacheConfiguration[] cacheConfigurations1() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[2];

        ccfgs[0] = cacheConfiguration(CACHE_NAME_PREFIX + 0, ATOMIC);
        ccfgs[1] = cacheConfiguration(CACHE_NAME_PREFIX + 1, TRANSACTIONAL);

        return ccfgs;
    }

    /**
     * @return Cache configurations.
     */
    final CacheConfiguration[] cacheConfigurations2() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[5];

        ccfgs[0] = cacheConfiguration(CACHE_NAME_PREFIX + 0, ATOMIC);
        ccfgs[1] = cacheConfiguration(CACHE_NAME_PREFIX + 1, TRANSACTIONAL);
        ccfgs[2] = cacheConfiguration(CACHE_NAME_PREFIX + 2, ATOMIC);
        ccfgs[3] = cacheConfiguration(CACHE_NAME_PREFIX + 3, TRANSACTIONAL);
        ccfgs[4] = cacheConfiguration(CACHE_NAME_PREFIX + 4, TRANSACTIONAL);

        ccfgs[4].setDataRegionName(NO_PERSISTENCE_REGION);
        ccfgs[4].setDiskPageCompression(null);

        return ccfgs;
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    protected final CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @param cacheName Cache name.
     * @param node Node.
     * @param exp {@code True} if expect that cache is started on node.
     */
    void checkCache(Ignite node, String cacheName, boolean exp) throws IgniteCheckedException {
        GridTestUtils.waitForCondition(
            () -> ((IgniteEx)node).context().cache().context().exchange().lastTopologyFuture() != null,
            2000
        );

        ((IgniteEx)node).context().cache().context().exchange().lastTopologyFuture().get();

        ((IgniteEx)node).context().state().publicApiState(true);

        GridCacheAdapter cache = ((IgniteEx)node).context().cache().internalCache(cacheName);

        if (exp)
            assertNotNull("Cache not found [cache=" + cacheName + ", node=" + node.name() + ']', cache);
        else
            assertNull("Unexpected cache found [cache=" + cacheName + ", node=" + node.name() + ']', cache);
    }

    /**
     * @param nodes Number of nodes.
     */
    final void checkNoCaches(int nodes) {
        for (int i = 0; i < nodes; i++) {
            assertEquals(INACTIVE, grid(i).context().state().publicApiState(true));

            GridCacheProcessor cache = ignite(i).context().cache();

            assertTrue(cache.caches().isEmpty());
            assertTrue(cache.internalCaches().stream().allMatch(c -> c.context().isRecoveryMode()));
        }
    }

    /** */
    private static void checkStatesAreDifferent(ClusterState state1, ClusterState state2) {
        assertTrue(state1 + " " + state2, ClusterState.active(state1) != ClusterState.active(state2));
    }
}
