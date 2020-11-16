/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests scenarious then tombstone clearing is blocked in the middle.
 */
public class BlockedTombstoneClearingTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSystemThreadPoolSize(1); // Avoid assignState parallelization.

        cfg.setFailureDetectionTimeout(100000L);
        cfg.setClientFailureDetectionTimeout(100000L);

        // Need at least 2 threads in pool to avoid deadlock on clearing.
        cfg.setRebalanceThreadPoolSize(ThreadLocalRandom.current().nextInt(3) + 2);
        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(200 * 1024 * 1024);
        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(2).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        return cfg;
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyOnCounterTriggeredRebalance() throws Exception {
        IgniteEx crd = startGrids(2);

        crd.cluster().state(ClusterState.ACTIVE);

        int testPart = 0;

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        cache.put(testPart, 0);

        forceCheckpoint();

        stopGrid(1);
        awaitPartitionMapExchange();

        crd.cache(DEFAULT_CACHE_NAME).remove(testPart);

        clearTombstones(cache);

        // Start node and delay preloading in the middle of partition clearing.
        IgniteEx g2 = startGrid(1);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * @param size Size.
     */
    private void validadate(int size) {
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        for (Ignite grid : G.allGrids())
            assertEquals(size, grid.cache(DEFAULT_CACHE_NAME).size());
    }

    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 100000;
    }

    @Override protected long getPartitionMapExchangeTimeout() {
        return super.getPartitionMapExchangeTimeout() * 100000;
    }
}
