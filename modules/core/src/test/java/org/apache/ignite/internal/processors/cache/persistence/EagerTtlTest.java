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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks a work of eager the TTL policy with a short time to leave.
 */
public class EagerTtlTest extends GridCommonAbstractTest {

    /** Text will print to log when assertion error happens. */
    private static final String ASSERTION_ERR = "java.lang.AssertionError: Invalid topology version [topVer=" +
        AffinityTopologyVersion.NONE +
        ", group=" + DEFAULT_CACHE_NAME + ']';

    /** Listening logger. */
    private ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setClusterStateOnStart(ClusterState.INACTIVE)
            .setCacheConfiguration(getCacheConfiguration(DEFAULT_CACHE_NAME))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        listeningLog = new ListeningTestLogger(log);
    }

    /**
     * Creates a configuration for cache with the name specified.
     *
     * @param cacheName Cahce name.
     * @return Cache configuration.
     */
    private CacheConfiguration getCacheConfiguration(String cacheName) {
        return new CacheConfiguration(cacheName)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 10)));
    }

    @Test
    public void testOneNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        ignite.close();

        LogListener assertListener = LogListener.matches(ASSERTION_ERR).build();

        listeningLog.registerListener(assertListener);

        ignite = startGrid(0);

        CountDownLatch exchnageHangLatch = new CountDownLatch(1);

        ignite.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {

                try {
                    exchnageHangLatch.await();
                }
                catch (InterruptedException e) {
                    log.error("Interruped of waiting latch", e);

                    fail(e.getMessage());
                }
            }
        });

        IgniteInternalFuture activeFut = GridTestUtils.runAsync(() -> ignite(0).cluster().state(ClusterState.ACTIVE));

        assertFalse(activeFut.isDone());

        assertFalse(GridTestUtils.waitForCondition(assertListener::check, 2_000));

        exchnageHangLatch.countDown();

        activeFut.get();

        awaitPartitionMapExchange();
    }

    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrids(2);

        checkTopology(2);

        CountDownLatch exchnageHangLatch = new CountDownLatch(1);

        ignite.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    exchnageHangLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        ignite.cluster().state(ClusterState.ACTIVE);

        LogListener assertListener = LogListener.matches(ASSERTION_ERR).build();

        assertFalse(GridTestUtils.waitForCondition(assertListener::check, 5_000));

        exchnageHangLatch.countDown();

        awaitPartitionMapExchange();

//        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);
//
//        for (int i = 0; i < 100; i++)
//            cache.put(i, i);
//
//        CountDownLatch exchnageHangLatch1 = new CountDownLatch(1);
//
//        ignite.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
//            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
//                try {
//                    exchnageHangLatch1.await();
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
////         TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite(1));
//
////        spi.blockMessages(GridDhtPartitionsSingleMessage.class, getTestIgniteInstanceName(0));
//
//        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
//            IgniteCache cache1 = ignite.getOrCreateCache(getCacheConfiguration(DEFAULT_CACHE_NAME + 1));
//
//            for (int i = 0; i < 100; i++)
//                cache1.put(i, i);
//        });
//
////        spi.waitForBlocked();
//
//        assertFalse(fut.isDone());
//
//        info("Exchnage blocked.");
//
//        Thread.sleep(10_000);
//
//        exchnageHangLatch1.countDown();
//
//        fut.get();

//        spi.stopBlock();
    }
}
