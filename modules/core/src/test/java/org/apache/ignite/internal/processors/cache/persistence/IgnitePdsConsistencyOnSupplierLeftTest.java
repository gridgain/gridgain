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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.WalStateManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class IgnitePdsConsistencyOnSupplierLeftTest extends GridCommonAbstractTest {
    /** Parts. */
    public static final int PARTS = 128;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(100000L);
        cfg.setClientFailureDetectionTimeout(100000L);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(50L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalSegmentSize(4 * 1024 * 1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(100000); // Disable automatic checkpoints.

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testSupplierLeft() throws Exception {
        IgniteEx crd = startGrids(4); // TODO multithreaded.
        crd.cluster().active(true);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint();

        stopGrid(1);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i + 1);

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(grid(0));
        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(grid(2));
        TestRecordingCommunicationSpi spi3 = TestRecordingCommunicationSpi.spi(grid(3));

        IgniteBiPredicate<ClusterNode, Message> pred = new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                return msg instanceof GridDhtPartitionSupplyMessage;
            }
        };

        spi0.blockMessages(pred);
        spi2.blockMessages(pred);
        spi3.blockMessages(pred);

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        });

        spi0.waitForBlocked();
        spi2.waitForBlocked();
        spi3.waitForBlocked();

//        TestRecordingCommunicationSpi.spi(grid(1)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
//            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
//                if (msg instanceof GridDhtPartitionDemandMessage) {
//                    GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage) msg;
//
//                    return msg0.topologyVersion().equals(new AffinityTopologyVersion(7, 0));
//                }
//
//                return false;
//            }
//        });


        CountDownLatch stopLatch = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);


        grid(1).context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                latch3.countDown();
            }
        });

        AtomicBoolean check = new AtomicBoolean(true);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) grid(1).context().cache().context().database();
        dbMgr.addCheckpointListener(new DbCheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                if (check.get()) {
                    String reason = ctx.progress().reason();

                    assertTrue(reason, reason.startsWith(WalStateManager.ENABLE_DURABILITY_AFTER_REBALANCING));

                    stopLatch.countDown();

                    try {
                        latch2.await(10000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        fail();
                    }

                    check.set(false);
                }
            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // No-op.
            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // No-op.
            }
        });

        TestRecordingCommunicationSpi.spi(grid(1)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionsSingleMessage) {
                    GridDhtPartitionsSingleMessage msg0 = (GridDhtPartitionsSingleMessage) msg;

                    return true;
                }

                return false;
            }
        });

        spi0.stopBlock();
        spi2.stopBlock();
        spi3.stopBlock();

        // Wait for cp.
        stopLatch.await(10000, TimeUnit.MILLISECONDS);

        // Will cause rebalancing remap.
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                stopGrid(2);
            }
        });

        // Wait for PME start.
        latch3.await(10000, TimeUnit.MILLISECONDS);

        // Trigger spurious switch.
        latch2.countDown();

        TestRecordingCommunicationSpi.spi(grid(1)).waitForBlocked(2);

        TestRecordingCommunicationSpi.spi(grid(0)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionsFullMessage) {
                    GridDhtPartitionsFullMessage msg0 = (GridDhtPartitionsFullMessage) msg;

                    return msg0.exchangeId() != null;
                }

                return false;
            }
        });

        TestRecordingCommunicationSpi.spi(grid(1)).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> tuple) {
                GridIoMessage msg = tuple.get2();
                GridDhtPartitionsSingleMessage msg0 = (GridDhtPartitionsSingleMessage) msg.message();

                return msg0.exchangeId() != null;
            }
        });

        TestRecordingCommunicationSpi.spi(grid(0)).waitForBlocked();

        // Finish exchange.
        TestRecordingCommunicationSpi.spi(grid(1)).stopBlock();

        //doSleep(3000000);

        //awaitPartitionMapExchange();
    }

    @Test
    public void testSupplierLeft2() throws Exception {
        IgniteEx crd = startGrids(4); // TODO multithreaded.
        crd.cluster().active(true);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint();

        stopGrid(1);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i + 1);

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(grid(0));
        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(grid(2));
        TestRecordingCommunicationSpi spi3 = TestRecordingCommunicationSpi.spi(grid(3));

        IgniteBiPredicate<ClusterNode, Message> pred = new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                return msg instanceof GridDhtPartitionSupplyMessage;
            }
        };

        spi0.blockMessages(pred);
        spi2.blockMessages(pred);
        spi3.blockMessages(pred);

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        });

        spi0.waitForBlocked();
        spi2.waitForBlocked();
        spi3.waitForBlocked();

        spi0.stopBlock();
        spi2.stopBlock();

        CountDownLatch delayListener = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);

        AtomicBoolean first = new AtomicBoolean(true);

        IgniteInternalFuture<Boolean> rebFut = grid(1).cachex(DEFAULT_CACHE_NAME).context().preloader().rebalanceFuture();
        rebFut.listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
            @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                if (first.get()) {
                    delayListener.countDown();

                    try {
                        l2.await(115000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        fail();
                    }

                    first.set(false);
                }
            }
        });

        TestRecordingCommunicationSpi.spi(grid(1)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionDemandMessage) {
                    GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage) msg;

                    return msg0.topologyVersion().equals(new AffinityTopologyVersion(7, 0));
                }

                return false;
            }
        });

        grid(1).context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                delayListener.countDown();
            }
        });

        // Will cause rebalancing remap.
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                stopGrid(2);
            }
        });

        delayListener.await();

        spi3.stopBlock(); // Finish rebalancing but delay on "group rebalanced" listener

        TestRecordingCommunicationSpi.spi(grid(1)).waitForBlocked();

        l2.countDown();

        //TestRecordingCommunicationSpi.spi(grid(1)).stopBlock();

        doSleep(10000);
    }
}
