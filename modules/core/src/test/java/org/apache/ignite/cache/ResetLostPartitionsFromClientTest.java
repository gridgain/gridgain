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

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
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
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setPersistenceEnabled(true)));

        return cfg;
    }

    /**
     * Tests the reset lost partition from a restarted client node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResetLostPartitions() throws Exception {
        startGrids(2);

        Ignite node = startClientGrid(3);

        node.cluster().state(ClusterState.ACTIVE);

        String cacheName = "test";

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setBackups(0);

        IgniteCache<Integer, String> cache = node.getOrCreateCache(cacheCfg);

        for (int i = 0; i < 100; i++) {
            cache.put(i, "val-" + i);
        }

        node.close();

        stopGrid(1);

        startGrid(1);

        node = startClientGrid(3);

        node.resetLostPartitions(Arrays.asList(cacheName));

        cache = node.cache(cacheName);

        for (int i = 0; i < 100; i++)
            cache.get(i);
    }
}
