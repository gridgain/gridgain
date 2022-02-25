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

import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test checks correctness of simultaneous node join and massive caches stopping.
 */
public class IgnitePdsNodeJoinWithCachesStopping extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(200 * 1024 * 1024)
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

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        final Ignite ig = startGridsMultiThreaded(2);

        for (int i = 0; i < 100; i++)
            ig.createCache(new CacheConfiguration<>("test0" + i).setBackups(0));

        IgniteInternalFuture<Boolean> gridStartFut = GridTestUtils.runAsync(() ->
        {
            try {
                startGrid(2);
            }
            catch (Exception e) {
                return false;
            }

            return true;
        }, "new-server-start-thread");

        for (int k = 0; k < 5; k++) {
            final int l = k;
            GridTestUtils.runAsync(() -> {
                for (int m = l * 20; m < (l + 1) * 20; m++)
                    ig.destroyCache("test0" + m);

            }, "cache-destroy-thread");
        }

        assertTrue(gridStartFut.get());
    }

    @Test
    public void testStartStopCacheWithLongPME() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(2);

        IgniteEx client = startClientGrid(2);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(grid(1));

        spi1.blockMessages((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);

        // Start a new cache and block PME in order to start/stop this cache during the blocked PME.
        IgniteInternalFuture<?> startFut1 = GridTestUtils.runAsync(() -> {
            try {
                client.getOrCreateCache("test-npe-cache");
            }
            catch (CacheException e) {
                throw new RuntimeException("Failed to create a new cache (step 1)", e);
            }
        });

        // Wait for initialization phase of PME.
        spi1.waitForBlocked();

        // Let's destroy the cache that is beign created at this time.
        // This request should lead to removing the corresponding cache desriptor.
        // See ClusterCachesInfo.onCacheChangeRequested(DynamicCacheChangeBatch, AffinityTopologyVersion)
        IgniteInternalFuture<?> stopFut1 = GridTestUtils.runAsync(() -> {
            try {
                client.destroyCache("test-npe-cache");
            }
            catch (CacheException e) {
                throw new RuntimeException("Failed to destroy new cache (step 1)", e);
            }
        });

        assertTrue(
            "Failed to wait for DynamicCacheChangeBatch message (destroy, step 1)",
            waitForCondition(() -> crd.context().discovery().topologyVersionEx().minorTopologyVersion() == 2, getTestTimeout()));

        // Let's start and stop the cache once again to clean up ClusterCachesInfo, i.e.
        // registeredCaches and markedForDeletionCaches will be cleaned,
        // and therefore, the corresponding cache descriptor will be lost.
        IgniteInternalFuture<?> startFut2 = GridTestUtils.runAsync(() -> {
            try {
                client.getOrCreateCache("test-npe-cache");
            }
            catch (CacheException e) {
                throw new RuntimeException("Failed to create a new cache (step 2)", e);
            }
        });

        assertTrue(
            "Failed to wait for DynamicCacheChangeBatch message (create, step 2)",
            waitForCondition(() -> crd.context().discovery().topologyVersionEx().minorTopologyVersion() == 3, getTestTimeout()));

        IgniteInternalFuture<?> stopFut2 = GridTestUtils.runAsync(() -> {
            try {
                client.destroyCache("test-npe-cache");
            }
            catch (CacheException e) {
                throw new RuntimeException("Failed to destroy new cache (step 1)", e);
            }
        });

        assertTrue(
            "Failed to wait for DynamicCacheChangeBatch message (create, step 2)",
            waitForCondition(() -> crd.context().discovery().topologyVersionEx().minorTopologyVersion() == 4, getTestTimeout()));

        // Unblock the initial PME.
        spi1.stopBlock();

        startFut1.get();
        stopFut1.get();
        startFut2.get();
        stopFut2.get();
    }
}
