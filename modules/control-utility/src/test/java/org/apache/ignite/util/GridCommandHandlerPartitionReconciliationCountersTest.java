/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** Tests for partition counter consistency reconciliation. */
public class GridCommandHandlerPartitionReconciliationCountersTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    CacheAtomicityMode atomicityMode;

    /** */
    boolean persistenceEnabled;

    /** */
    private static CommandHandler hnd;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /**
     * @param name Name.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(false);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));

        return ccfg;
    }

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        hnd = new CommandHandler();
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

    /** Partition counter consistency reconciliation of transactional cache. */
    @Test
    public void testReconciliationCountersFixedTransactionalCache() throws Exception {
        testReconciliationCountersFixed(TRANSACTIONAL, false,
            new String[]{"--cache", "partition_reconciliation", "--repair", "--local-output"});
    }

    /** Partition counter consistency reconciliation of atomic cache. */
    @Test
    public void testReconciliationCountersFixedAtomicCache() throws Exception {
        testReconciliationCountersFixed(ATOMIC, false,
            new String[]{"--cache", "partition_reconciliation", "--repair", "--local-output"});
    }

    /** Partition counter consistency reconciliation of transactional cache with persistence. */
    @Test
    public void testReconciliationCountersFixedTransactionalCachePersistence() throws Exception {
        testReconciliationCountersFixed(TRANSACTIONAL, true,
            new String[]{"--cache", "partition_reconciliation", "--repair", "--local-output"});
    }

    /** Partition counter consistency reconciliation of atomic cache with persistence. */
    @Test
    public void testReconciliationCountersFixedAtomicCachePersistence() throws Exception {
        testReconciliationCountersFixed(ATOMIC, true,
            new String[]{"--cache", "partition_reconciliation", "--repair", "--local-output"});
    }

    /** Partition counter consistency reconciliation with explicite arg in control.sh command. */
    @Test
    public void testReconciliationCountersFixedTransactionalCacheWithExplicitArg() throws Exception {
        testReconciliationCountersFixed(TRANSACTIONAL, false,
            new String[]{"--cache", "partition_reconciliation", "--repair",
                "--partition-counter-consistency-reconciliation", "true"});
    }

    /**
     * <ul>
     *   <li>Start two nodes.</li>
     *   <li>Create cache.</li>
     *   <li>Break counters.</li>
     *   <li>Invoke a reconciliation util for partition counter consistency reconciliation.</li>
     *   <li>Check that counters was fixed.</li>
     *   <li>Restart grid and check counters again if persistence enabled.</li>
     * </ul>
     */
    public void testReconciliationCountersFixed(CacheAtomicityMode atomicityMode, boolean persistenceEnabled, String[] cmdArgs) throws Exception {
        this.atomicityMode = atomicityMode;

        this.persistenceEnabled = persistenceEnabled;

        int finalCounterValue = 16;

        IgniteEx crd = startGrids(2);

        crd.cluster().state(ClusterState.ACTIVE);

        IgniteEx prim = grid(1);

        int part = primaryKey(prim.cache(DEFAULT_CACHE_NAME));

        crd.cache(DEFAULT_CACHE_NAME).put(part, 1);

        PartitionUpdateCounter cntr0 = counter(part, grid(0).name());
        PartitionUpdateCounter cntr1 = counter(part, grid(1).name());

        if (atomicityMode == TRANSACTIONAL && persistenceEnabled) {
            cntr0.update(finalCounterValue - 4, 4);
            cntr1.update(finalCounterValue - 3, 3);
        }
        else {
            cntr0.init(finalCounterValue - 6, null);
            cntr1.init(finalCounterValue, null);
        }

        assertFalse("Counter0: " + cntr0 + ", counter1: " + cntr1, cntr0.equals(cntr1));

        if (persistenceEnabled)
            forceCheckpoint();

        GridCachePartitionExchangeManager<Object, Object> exchangeMgr = grid(1).context().cache().context().exchange();

        AffinityTopologyVersion topVerBeforeRepair = exchangeMgr.lastTopologyFuture().topologyVersion();

        assertEquals(EXIT_CODE_OK, execute(hnd, cmdArgs));

        GridTestUtils.waitForCondition(() -> {
            GridDhtPartitionsExchangeFuture fut = exchangeMgr.lastTopologyFuture();

            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }

            return fut.topologyVersion()
                .equals(topVerBeforeRepair.nextMinorVersion());
        }, 5000);

        if (atomicityMode == TRANSACTIONAL) {
            assertEquals("Counter0: " + cntr0 + ", counter1: " + cntr1, cntr0, cntr1);
            assertTrue("Counter0: " + cntr0 + ", counter1: " + cntr1, cntr0.get() == finalCounterValue);
        }
        else {
            assertEquals("Counter0: " + cntr0 + ", counter1: " + cntr1, finalCounterValue, cntr0.get());
            assertEquals("Counter0: " + cntr0 + ", counter1: " + cntr1, finalCounterValue, cntr1.get());
        }

        assertCountersSame(part, true);

        if (persistenceEnabled) {
            stopAllGrids();

            startGrids(2);

            grid(0).cluster().state(ClusterState.ACTIVE);

            cntr0 = counter(part, grid(0).name());
            cntr1 = counter(part, grid(1).name());

            if (atomicityMode == TRANSACTIONAL) {
                assertEquals("Counter0: " + cntr0 + ", counter1: " + cntr1, cntr0, cntr1);
                assertTrue("Counter0: " + cntr0 + ", counter1: " + cntr1, cntr0.get() == finalCounterValue);
            }
            else {
                assertEquals("Counter0: " + cntr0 + ", counter1: " + cntr1, finalCounterValue, cntr0.get());
                assertEquals("Counter0: " + cntr0 + ", counter1: " + cntr1, finalCounterValue, cntr1.get());
            }

            assertCountersSame(part, true);
        }
    }

    /** Test that partition counters not fixed if it's not need. */
    @Test
    public void testReconciliationCountersNotFixed1() throws Exception {
        testReconciliationCountersNotFixed(
            new String[]{"--cache", "partition_reconciliation", "--repair",
                "--local-output", "--partition-counter-consistency-reconciliation", "false"}
        );
    }

    /** Test that partition counters not fixed if it's not need. */
    @Test
    public void testReconciliationCountersNotFixed2() throws Exception {
        testReconciliationCountersNotFixed(
            new String[]{"--cache", "partition_reconciliation",
                "--local-output", "--partition-counter-consistency-reconciliation", "true"}
        );
    }

    /**
     * <ul>
     *   <li>Start two nodes.</li>
     *   <li>Create cache.</li>
     *   <li>Break counters.</li>
     *   <li>Invoke a reconciliation util for partition counter consistency reconciliation.</li>
     *   <li>Check that counters not was fixed.</li>
     * </ul>
     */
    public void testReconciliationCountersNotFixed(String[] cmdArgs) throws Exception {
        atomicityMode = TRANSACTIONAL;

        int finalCounterValue = 16;

        IgniteEx crd = startGrids(2);

        crd.cluster().state(ClusterState.ACTIVE);

        IgniteEx prim = grid(1);

        int part = primaryKey(prim.cache(DEFAULT_CACHE_NAME));

        crd.cache(DEFAULT_CACHE_NAME).put(part, 1);

        PartitionUpdateCounter cntr0 = counter(part, grid(0).name());
        PartitionUpdateCounter cntr1 = counter(part, grid(1).name());

        if (atomicityMode == TRANSACTIONAL && persistenceEnabled) {
            cntr0.update(finalCounterValue - 4, 4);
            cntr1.update(finalCounterValue - 3, 3);
        }
        else {
            cntr0.init(finalCounterValue - 6, null);
            cntr1.init(finalCounterValue, null);
        }

        assertFalse("Counter0: " + cntr0 + ", counter1: " + cntr1, cntr0.equals(cntr1));

        if (persistenceEnabled)
            forceCheckpoint();

        GridCachePartitionExchangeManager<Object, Object> exchangeMgr = grid(1).context().cache().context().exchange();

        AffinityTopologyVersion topVerBeforeRepair = exchangeMgr.lastTopologyFuture().topologyVersion();

        assertEquals(EXIT_CODE_OK, execute(hnd, cmdArgs));

        assertFalse("Counter0: " + cntr0 + ", counter1: " + cntr1, cntr0.equals(cntr1));

    }
}
