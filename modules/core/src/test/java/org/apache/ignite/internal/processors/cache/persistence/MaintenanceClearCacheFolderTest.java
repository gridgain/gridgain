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
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.maintenance.ClearFolderWorkflow;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link ClearFolderWorkflow}.
 */
public class MaintenanceClearCacheFolderTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMaintenanceTask() throws Exception {
        IgniteEx ignite0 = startGrids(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        startCacheAndPreload("cache_to_delete_1");
        startCacheAndPreload("cache_to_delete_2");
        startCacheAndPreload(DEFAULT_CACHE_NAME);

        forceCheckpoint();

        ignite(1).close();

        ignite0.destroyCache("cache_to_delete_1");
        ignite0.destroyCache("cache_to_delete_2");

        try {
            startGrid(1);
            fail();
        }
        catch (Exception ignored) {
            // It's ok, we should not be able to start with the cache whcih is already removed.
        }

        stopGrid(1);

        // Starts in maintenance
        IgniteEx ignite1 = startGrid(1);

        assertTrue(ignite1.context().maintenanceRegistry().isMaintenanceMode());

        ignite1.close();

        // Restart node again, it should start normally
        ignite1 = startGrid(1);

        assertFalse(ignite1.context().maintenanceRegistry().isMaintenanceMode());

        awaitPartitionMapExchange();

        checkTopology(2);

        assertEquals(1, ignite1.cacheNames().size());

        checkCacheData(DEFAULT_CACHE_NAME);
    }

    /**
     * @throws Exception If fialed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DISABLE_MAINTENANCE_CLEAR_FOLDER_TASK, value = "true")
    public void testDisbaleMaintenanceTask() throws Exception {
        IgniteEx ignite0 = startGrids(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        startCacheAndPreload("cache_to_delete_1");

        forceCheckpoint();

        ignite(1).close();

        ignite0.destroyCache("cache_to_delete_1");

        try {
            startGrid(1);
            fail();
        }
        catch (Exception ignored) {
            // It's ok, we should not be able to start with the cache whcih is already removed.
        }

        stopAllGrids();

        IgniteEx ignite1 = startGrid(1);

        assertFalse(ignite1.context().maintenanceRegistry().isMaintenanceMode());

        ignite1.cluster().state(ClusterState.ACTIVE);

        assertEquals(1, ignite1.cacheNames().size());
    }

    /**
     * Start cacge and loads data.
     *
     * @param cahceName Cache name.
     */
    private void startCacheAndPreload(String cahceName) {
        IgniteEx ignite = ignite(0);

        ignite.createCache(new CacheConfiguration<>(cahceName)
            .setAffinity(new RendezvousAffinityFunction(false, 8))
            .setBackups(1));

        try (IgniteDataStreamer streamer = ignite.dataStreamer(cahceName)) {
            for (int i = 0; i < 100; i++) {
                streamer.addData(i, i);
            }
        }
    }

    /**
     * Checks cache data.
     *
     * @param cahceName Cache name.
     */
    private void checkCacheData(String cahceName) {
        IgniteEx ignite = ignite(0);

        IgniteCache cache = ignite.cache(cahceName);

        for (int i = 0; i < 100; i++) {
            assertEquals(i, cache.get(i));
        }
    }
}
