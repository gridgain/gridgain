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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 * Tests for "wait for backups on shutdown" flag.
 */
@WithSystemProperty(key = IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN, value = "true")
public class GridCacheDhtPreloadWaitForBackupsTest extends GridCommonAbstractTest {
    /** Key to store list of gracefully stopping nodes within metastore. */
    private static final String GRACEFUL_SHUTDOWN_METASTORE_KEY =
        DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown";

    /** */
    private CacheMode cacheMode;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private CacheWriteSynchronizationMode synchronizationMode;

    /** */
    private CacheRebalanceMode rebalanceMode;

    /** */
    private boolean clientNodes;

    /** */
    private int backups;

    /** */
    public GridCacheDhtPreloadWaitForBackupsTest() {
        super(false);
    }

    /**
     * @return {@code True} if persistence must be enabled for test.
     */
    protected boolean persistenceEnabled() {
        return false;
    }

    /** */
    protected int cacheSize() {
        return 10000;
    }

    /** */
    protected int iterations() {
        return 10;
    }

    /** */
    @Override protected void afterTest() throws Exception {
        IgnitionEx.stopAll(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLeavesRebalanceCompletesAtomicReplicated() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.SYNC;
        clientNodes = false;
        backups = 1;

        nodeLeavesRebalanceCompletes();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLeavesRebalanceCompletesTransactionalPartitioned() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_SYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 1;

        nodeLeavesRebalanceCompletes();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLeavesRebalanceCompletesClientNode() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = true;
        backups = 1;

        nodeLeavesRebalanceCompletes();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeForceShutdown() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 1;

        startGrids(2);

        if (persistenceEnabled())
            grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("partitioned" + (1 + (i >> 3) % 3)).put(i, new byte[i]);

        grid(0).close();

        final CountDownLatch latch = new CountDownLatch(1);

        Thread stopper = new Thread(() -> {
            grid(1).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertFalse(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, 3000));

        IgnitionEx.stop(grid(1).configuration().getIgniteInstanceName(), true, false, false);

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, 3000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShutdownWithoutBackups() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 1;

        startGrids(2);

        if (persistenceEnabled())
            grid(0).cluster().active(true);

        grid(1).cache("partitioned1").destroy();
        grid(1).cache("partitioned2").destroy();
        grid(1).cache("partitioned3").destroy();

        grid(1).createCache(new CacheConfiguration<>("no-backups").setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0));

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("no-backups").put(i, new byte[i]);

        grid(0).close();
        grid(1).close();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThatItsNotPossibleToStopLastOwnerIfAnotherOwnerIsStopping() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = true;
        backups = 1;

        startGrids(2);

        grid(0).context().distributedMetastorage().write(
            DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown",
            new HashSet<>(Collections.singleton(grid(0).localNode().id())));

        final CountDownLatch latch = new CountDownLatch(1);

        Thread stopper = new Thread(() -> {
            grid(1).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertFalse(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, 3000));

        grid(0).context().distributedMetastorage().write(
            GRACEFUL_SHUTDOWN_METASTORE_KEY,
            new HashSet<>());

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, 3000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThatItsPossibleToStopNodeIfExludedNodeListWithinMetastoreIsntEmpty() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 2;

        startGrids(3);

        grid(0).context().distributedMetastorage().write(
            DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown",
            new HashSet<>(Collections.singleton(grid(0).localNode().id())));

        final CountDownLatch latch = new CountDownLatch(1);

        UUID node1Id = grid(1).localNode().id();

        Thread stopper = new Thread(() -> {
            grid(1).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, 3000));

        HashSet<UUID> expMetastoreContent = new HashSet<>();
        expMetastoreContent.add(grid(0).localNode().id());
        expMetastoreContent.add(node1Id);

        assertEquals(
            expMetastoreContent,
            grid(0).context().distributedMetastorage().read(GRACEFUL_SHUTDOWN_METASTORE_KEY));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThatExcludedNodeListWithinMetastoreCleanedUpAfterUpdatingFullMap() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 3;

        startGrids(4);

        grid(0).context().distributedMetastorage().write(
            DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown",
            new HashSet<>(Collections.singleton(grid(0).localNode().id())));

        final CountDownLatch latch = new CountDownLatch(1);

        UUID node1Id = grid(1).localNode().id();

        Thread stopper = new Thread(() -> {
            grid(1).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, 3000));

        HashSet<UUID> expMetastoreContent = new HashSet<>();
        expMetastoreContent.add(grid(0).localNode().id());
        expMetastoreContent.add(node1Id);

        assertEquals(
            expMetastoreContent,
            grid(0).context().distributedMetastorage().read(GRACEFUL_SHUTDOWN_METASTORE_KEY));

        UUID node2Id = grid(2).localNode().id();

        grid(2).close();

        expMetastoreContent.remove(node1Id);
        expMetastoreContent.add(node2Id);

        assertEquals(
            expMetastoreContent,
            grid(0).context().distributedMetastorage().read(GRACEFUL_SHUTDOWN_METASTORE_KEY));
    }

    /**
     * @throws Exception If failed.
     */
    private void nodeLeavesRebalanceCompletes() throws Exception {
        startGrids(4);

        if (persistenceEnabled())
            grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 4).cache("partitioned" + (1 + (i >> 3) % 3)).put(i, new byte[i]);

        int nextGrid = 4;

        Thread th0 = null;

        Thread th1 = null;

        for (int n = 0; n < iterations(); n++) {
            int startGrid = nextGrid;
            int stopPerm = (nextGrid + 1) % 5;
            int stopTmp = (nextGrid + 2) % 5;

            startGrid(startGrid);

            grid(stopTmp).close();

            (th0 = new Thread(() -> grid(stopPerm).close(), "Stop-" + stopPerm)).start();

            Thread.sleep(1000);

            (th1 = new Thread(() ->
            {
                try {
                    startGrid(stopTmp);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }, "Start-" + stopTmp)).start();

            nextGrid = stopPerm;

            th1.join();
            th0.join();
        }

        for (int i = 0; i < cacheSize(); i++) {
            byte[] val = (byte[])grid((i >> 2) % 4).cache("partitioned" + (1 + (i >> 3) % 3)).get(i);

            assertNotNull(Integer.toString(i), val);
            assertEquals(i, val.length);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] ccfgs = new CacheConfiguration[3];
        for (int i = 1; i <= 3; i++) {
            CacheConfiguration ccfg = new CacheConfiguration("partitioned" + i);

            ccfg.setCacheMode(cacheMode);
            ccfg.setAtomicityMode(atomicityMode);
            ccfg.setWriteSynchronizationMode(synchronizationMode);
            ccfg.setRebalanceMode(rebalanceMode);
            ccfg.setBackups(backups);
            ccfg.setAffinity(new RendezvousAffinityFunction().setPartitions(32));

            ccfgs[i - 1] = ccfg;
        }

        cfg.setCacheConfiguration(ccfgs);

        if (persistenceEnabled()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(LOG_ONLY)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
        }

        if (clientNodes && (igniteInstanceName.endsWith("2") || igniteInstanceName.endsWith("3")))
            cfg.setClientMode(true);

        return cfg;
    }
}
