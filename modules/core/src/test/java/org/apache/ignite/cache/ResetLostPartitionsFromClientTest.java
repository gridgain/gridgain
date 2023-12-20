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

package org.apache.ignite.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

/**
 * Tests for the reset lost partition by client node.
 */
public class ResetLostPartitionsFromClientTest extends GridCommonAbstractTest {
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
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()));

        return cfg;
    }

    /**
     * Tests the reset lost partition by client node for a cache which was created by another node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResetLostPartitions() throws Exception {
        IgniteEx ignite = startGrids(2);

        IgniteEx client = startClientGrid(3);

        String cacheName = "test_cache";

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);
        cacheCfg.setBackups(0);

        IgniteCache<Integer, String> cache = ignite.createCache(cacheCfg);

        for (int i = 0; i < 100; i++) {
            cache.put(i, "val-" + i);
        }

        stopGrid(1);

        startGrid(1);

        client.resetLostPartitions(Arrays.asList(cacheName));

        Set<Integer> lostParts = client.cachex(cacheName).context().topology().lostPartitions();
        assertTrue("Lost partitions: " + lostParts, lostParts.isEmpty());

        lostParts = ignite.cachex(cacheName).context().topology().lostPartitions();
        assertTrue("Lost partitions: " + lostParts, lostParts.isEmpty());
    }
}
