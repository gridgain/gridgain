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

package org.apache.ignite.cache.affinity;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_REBALANCING_CANCELLATION_OPTIMIZATION;

/**
 * Test creates two exchange in same moment, in order to when first executing second would already in queue.
 */
@WithSystemProperty(key = IGNITE_DISABLE_REBALANCING_CANCELLATION_OPTIMIZATION, value = "false")
public class PendingExchangeTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setBackups(1));
    }

    /**
     * Test checks that pending exchange will lead to stable topology.
     *
     * @throws Exception Id failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        try (IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 1000; i++)
                streamer.addData(i, i);
        }

        IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        startGrid(2);

        awaitPartitionMapExchange();

        GridCachePartitionExchangeManager exchangeManager1 = ignite1.context().cache().context().exchange();

        CountDownLatch exchangeLatch = new CountDownLatch(1);

        AffinityTopologyVersion readyTop = exchangeManager1.readyAffinityVersion();

        exchangeManager1.registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    exchangeLatch.await();
                }
                catch (InterruptedException e) {
                    fail("Thread was interrupted.");
                }
            }
        });

        IgniteInternalFuture startNodeFut = GridTestUtils.runAsync(() -> stopGrid(2));

        assertTrue(GridTestUtils.waitForCondition(() ->
            exchangeManager1.lastTopologyFuture().initialVersion().after(readyTop), 10_000));

        IgniteInternalFuture startCacheFut = GridTestUtils.runAsync(() -> ignite0.createCache(DEFAULT_CACHE_NAME + 1));

        assertTrue(GridTestUtils.waitForCondition(exchangeManager1::hasPendingExchange, 10_000));

        exchangeLatch.countDown();

        startNodeFut.get(10_000);
        startCacheFut.get(10_000);

        awaitPartitionMapExchange();
    }
}
