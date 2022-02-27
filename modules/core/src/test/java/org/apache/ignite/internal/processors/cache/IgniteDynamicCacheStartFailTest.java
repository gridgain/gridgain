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

import javax.cache.CacheException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests the recovery after a dynamic cache start failure.
 */
public class IgniteDynamicCacheStartFailTest extends IgniteAbstractDynamicCacheStartFailTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(gridCount());

        awaitPartitionMapExchange();
    }

    @Test
    public void testAAA() throws Exception {
        IgniteEx client = startClientGrid(gridCount());

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(grid(1));

        spi1.blockMessages((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);

        String cacheName = "test-npe-cache";

        IgniteInternalFuture<?> startFut = GridTestUtils.runAsync(() -> {
            CacheConfiguration<?, ?> cfg =
                createCacheConfigsWithBrokenCacheStore(true, 0, 0, 1, false).get(0);

            cfg.setName(cacheName);

            client.getOrCreateCache(cfg);
        });

        spi1.waitForBlocked();

        AffinityTopologyVersion currVer = grid(0).context().discovery().topologyVersionEx();
        AffinityTopologyVersion expVer = currVer.nextMinorVersion();

        IgniteInternalFuture<?> stopFut = GridTestUtils.runAsync(() -> {
            client.destroyCache(cacheName);
        });

        assertTrue(
            "Failed to wait for DynamicCacheChangeBatch message (destroy)",
            waitForCondition(() -> grid(0).context().discovery().topologyVersionEx().equals(expVer), getTestTimeout()));

        spi1.stopBlock();

        // Creating a new cache should lead to a exception.
        assertThrows(log, () -> startFut.get(getTestTimeout()), CacheException.class, null);

        stopFut.get(getTestTimeout());
    }
}
