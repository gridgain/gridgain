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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplier;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

/**
 */
@WithSystemProperty(key = "IGNITE_BASELINE_AUTO_ADJUST_ENABLED", value = "false")
public class CacheRebalanceThreadsUtilizationTest extends GridCommonAbstractTest {
    /** */
    private static final int REBALANCE_POOL_SIZE = 4;

    /** */
    private static final int PARTS_CNT = 32;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean persistenceEnabled;

    /** */
    private boolean delayDemandMsg = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setActiveOnStart(false);

        TcpDiscoverySpi discoverySpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);

        cfg.setRebalanceThreadPoolSize(REBALANCE_POOL_SIZE);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        if (delayDemandMsg) {
            spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionDemandMessage &&
                ((GridDhtPartitionDemandMessage)msg).groupId() == CU.cacheId(DEFAULT_CACHE_NAME));

            delayDemandMsg = false;
        }

        cfg.setCommunicationSpi(spi);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)).setBackups(1));

        long sz = 100 * 1024 * 1024;

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setPageSize(1024)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled).setInitialSize(sz).setMaxSize(sz))
            .setWalSegmentSize(8 * 1024 * 1024)
            .setWalHistorySize(1000)
            .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(24L * 60 * 60 * 1000);

        cfg.setDataStorageConfiguration(memCfg);

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

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testResourceUtilization_Volatile_ManyPartitions() throws Exception {
        doTestResourceUtilization(false, false, false);
    }

    /** */
    @Test
    public void testResourceUtilization_Volatile_SinglePartition() throws Exception {
        doTestResourceUtilization(false, false, true);
    }

    /** */
    @Test
    public void testResourceUtilization_Persistent_ManyPartitions() throws Exception {
        doTestResourceUtilization(true, false, false);
    }

    /** */
    @Test
    public void testResourceUtilization_Persistent_SinglePartition() throws Exception {
        doTestResourceUtilization(true, false, true);
    }

    /** */
    @Test
    public void testResourceUtilization_Historical_ManyPartitions() throws Exception {
        doTestResourceUtilization(true, true, false);
    }

    /** */
    @Test
    public void testResourceUtilization_Historical_SinglePartition() throws Exception {
        doTestResourceUtilization(true, true, true);
    }

    /**
     */
    private void doTestResourceUtilization(boolean persistenceEnabled, boolean historical, boolean singlePart) throws Exception {
        if (historical)
            System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        try {
            this.persistenceEnabled = persistenceEnabled;

            IgniteEx ex = startGrids(2);
            ex.cluster().active(true);

            if (persistenceEnabled) {
                for (int p = 0; p < PARTS_CNT; p++)
                    ex.cache(DEFAULT_CACHE_NAME).put(p, p);

                forceCheckpoint();
            }

            stopGrid(1);

            GridDhtPreloader preloader0 = (GridDhtPreloader)ex.cachex(DEFAULT_CACHE_NAME).context().group().preloader();

            ConcurrentSkipListSet<String> supplierThreads = new ConcurrentSkipListSet<>();

            mockSupplier(preloader0, new GridAbsClosure() {
                @Override public void apply() {
                    supplierThreads.add(Thread.currentThread().getName());
                }
            });

            if (singlePart)
                loadDataToPartition(0, ex.name(), DEFAULT_CACHE_NAME, 100_000, PARTS_CNT, 3);
            else {
                try (IgniteDataStreamer<Object, Object> streamer = ex.dataStreamer(DEFAULT_CACHE_NAME)) {
                    for (int k = 0; k < 100_000; k++)
                        streamer.addData(k + PARTS_CNT, k + PARTS_CNT);
                }
            }

            ConcurrentSkipListSet<String> demanderThreads = new ConcurrentSkipListSet<>();

            delayDemandMsg = true;

            IgniteEx g2 = startGrid(1);

            TestRecordingCommunicationSpi.spi(g2).waitForBlocked();

            GridDhtPreloader preloader1 = (GridDhtPreloader)g2.cachex(DEFAULT_CACHE_NAME).context().group().preloader();
            mockDemander(preloader1, new GridAbsClosure() {
                @Override public void apply() {
                    demanderThreads.add(Thread.currentThread().getName());
                }
            });

            TestRecordingCommunicationSpi.spi(g2).stopBlock();

            awaitPartitionMapExchange();

            assertPartitionsSame(idleVerify(ex, DEFAULT_CACHE_NAME));

            assertEquals(REBALANCE_POOL_SIZE, supplierThreads.size());
            assertEquals(REBALANCE_POOL_SIZE, demanderThreads.size());

            assertTrue(supplierThreads.stream().allMatch(s -> s.contains(ex.configuration().getIgniteInstanceName())));
            assertTrue(demanderThreads.stream().allMatch(s -> s.contains(g2.configuration().getIgniteInstanceName())));
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

            stopAllGrids();
        }
    }

    /**
     * @param preloader Preloader.
     * @param clo Closure to call before demand message processing.
     */
    private void mockSupplier(GridDhtPreloader preloader, GridAbsClosure clo) {
        GridDhtPartitionSupplier supplier = preloader.supplier();

        GridDhtPartitionSupplier mockedSupplier = Mockito.spy(supplier);

        Mockito.doAnswer(invocation -> {
            clo.run();

            invocation.callRealMethod();

            return null;
        }).when(mockedSupplier).handleDemandMessage(Mockito.anyInt(), Mockito.any(), Mockito.any());

        preloader.supplier(mockedSupplier);
    }

    /**
     * @param preloader Preloader.
     * @param clo Closure to call before supply message processing.
     */
    private void mockDemander(GridDhtPreloader preloader, GridAbsClosure clo) {
        GridDhtPartitionDemander demander = preloader.demander();

        GridDhtPartitionDemander mockedDemander = Mockito.spy(demander);

        Mockito.doAnswer(invocation -> {
            clo.run();

            invocation.callRealMethod();

            return null;
        }).when(mockedDemander).handleSupplyMessage(Mockito.any(), Mockito.any());

        preloader.demander(mockedDemander);
    }
}
