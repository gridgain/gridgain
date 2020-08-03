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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@code IgniteDataStreamerImpl}.
 */
public class MyTest extends GridCommonAbstractTest {
    private static final String CACHE_NAME = "testCache";

//    private static final String CACHE_2 = "testCache2";

    private static final String GROUP_NAME = "testGroup";

//    private static final String GROUP_2 = "testGroup2";

    private static final int nodeCount = 3;

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

    /**
     * @return Grid test configuration.
     * @throws Exception If failed.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        // Generate unique Ignite instance name.

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsConfig = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024)
                .setPersistenceEnabled(true));
//
        cfg.setDataStorageConfiguration(dsConfig);

        cfg.setCommunicationSpi(new MySpi());

        return cfg;
    }

//    /** {@inheritDoc} */
//    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
//
//        cfg.setCommunicationSpi(new StaleTopologyCommunicationSpi());
//
//        if (cnt < MAX_CACHE_COUNT)
//            cfg.setCacheConfiguration(cacheConfiguration());
//
//        cnt++;
//
//        return cfg;
//    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMy() throws Exception {
        startGrids(nodeCount);

        Ignite g0 = grid(0);

        g0.active(true);

        IgniteEx client1 = startClientGrid(nodeCount);

        IgniteEx client2 = startClientGrid(nodeCount + 1);

        IgniteEx client3 = startClientGrid(nodeCount + 2);

        client1.createCache(getCacheConfiguration(0));

        awaitPartitionMapExchange();

        Map<String, IgniteInternalFuture<Boolean>> rebalanceFutures1 = getAllRebalanceFutures(grid(0));

        AtomicBoolean doLoad = new AtomicBoolean(true);

        IgniteInternalFuture<IgniteFuture> fut = GridTestUtils.runAsync(() -> {
//            Map<Integer, Integer> map = new HashMap<>();
//
//            int counter = 0;
//
//            for (int i = counter; i < Long.MAX_VALUE; i++)
//                map.put(i, i);
//
//            IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME + 0);
//
//            streamer.allowOverwrite(false);
//
//            streamer.addData(map);
//
            IgniteDataStreamer<Integer, Integer> streamer = client1.dataStreamer(CACHE_NAME + 0);

            streamer.allowOverwrite(false);

            streamer.perNodeBufferSize(50);

            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                streamer.addData(i, i);
//                streamer.flush();
                if (!doLoad.get())
                    break;
            }

            return null;
        });

        IgniteInternalFuture<IgniteFuture> fut1 = GridTestUtils.runAsync(() -> {
//            Map<Integer, Integer> map = new HashMap<>();
//
//            int counter = 0;
//
//            for (int i = counter; i < Long.MAX_VALUE; i++)
//                map.put(i, i);
//
//            IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME + 0);
//
//            streamer.allowOverwrite(false);
//
//            streamer.addData(map);
//
            IgniteDataStreamer<Integer, Integer> streamer = client2.dataStreamer(CACHE_NAME + 0);

            streamer.allowOverwrite(false);

            streamer.perNodeBufferSize(50);

            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                streamer.addData(i, i);
//                streamer.flush();
                if (!doLoad.get())
                    break;
            }

            return null;
        });

        IgniteInternalFuture<IgniteFuture> fut2 = GridTestUtils.runAsync(() -> {
//            Map<Integer, Integer> map = new HashMap<>();
//
//            int counter = 0;
//
//            for (int i = counter; i < Long.MAX_VALUE; i++)
//                map.put(i, i);
//
//            IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME + 0);
//
//            streamer.allowOverwrite(false);
//
//            streamer.addData(map);
//
            IgniteDataStreamer<Integer, Integer> streamer = client3.dataStreamer(CACHE_NAME + 0);

            streamer.allowOverwrite(false);

            streamer.perNodeBufferSize(1);

            for (int i = Integer.MAX_VALUE; i > 0; i--) {
                streamer.addData(i, i);
                streamer.flush();
                if (!doLoad.get())
                    break;
            }

            return null;
        });

        doSleep(1000);

        for (int i = 1; i < 11; i++)
            client2.createCache(getCacheConfiguration(i));

        doSleep(500);

        Map<String, IgniteInternalFuture<Boolean>> rebalanceFutures2 = getAllRebalanceFutures(grid(0));

        assert(!fut.isDone());
        assert(!fut1.isDone());
        assert(!fut2.isDone());

        System.out.println();
        doSleep(1000);

//        stopGrid(2);
//        awaitPartitionMapExchange();
//        startGrid(2);
//        awaitPartitionMapExchange();

//        stopGrid(3);
//        awaitPartitionMapExchange();
//        startGrid(3);
//
//        awaitPartitionMapExchange();

        System.out.println();

        doLoad.set(false);
//        streamer.close();
//        ((MySpi) grid(0).configuration().getCommunicationSpi())
    //        MySpi.allRebalances()
        //...569601 - id кеш группы для системного кеша, ...274243 - для кешгруппы0, ...274244 - для кешгруппы1, ...274245 - для кешгруппы2
    }

    private CacheConfiguration<Object, Object> getCacheConfiguration(int idx) {
        return new CacheConfiguration<>(CACHE_NAME + idx)
            .setGroupName(GROUP_NAME + idx)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(32));
    }

//    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
//
//        DataStorageConfiguration dsConfig = new DataStorageConfiguration()
//            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024)
//                .setPersistenceEnabled(true));
//
//        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
//
//        return cfg
//            .setDataStorageConfiguration(dsConfig)
//            .setCacheConfiguration(new CacheConfiguration("cache")
//                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
//                .setWriteSynchronizationMode(syncMode).setBackups(0));
//    }


    /**
     * Finds all existed rebalance future by all cache for Ignite's instance specified.
     *
     * @param ignite Ignite.
     * @return Array of rebelance futures.
     */
    private Map<String, IgniteInternalFuture<Boolean>> getAllRebalanceFutures(IgniteEx ignite) {
        Map<String, IgniteInternalFuture<Boolean>> futs = new HashMap<>();

        for (CacheGroupContext grp : ignite.context().cache().cacheGroups()) {
            if (grp.cacheOrGroupName().contains("test")) {
                IgniteInternalFuture<Boolean> fut = grp.preloader().rebalanceFuture();

                futs.put(grp.cacheOrGroupName(), fut);
            }
        }

        return futs;
    }

    /**
     * Wrapper of communication spi to detect on what topology versions WAL rebalance has happened.
     */
    public static class MySpi extends TestRecordingCommunicationSpi {
        /** (Group ID, Set of topology versions). */
        private static final Map<Integer, Set<Long>> topVers = new HashMap<>();

        public static final List<Message> msgs = new ArrayList<>();

        /** Lock object. */
        private static final Object mux = new Object();

        /**
         * @param grpId Group ID.
         * @return Set of topology versions where WAL history has been used for rebalance.
         */
        Set<Long> walRebalanceVersions(int grpId) {
            synchronized (mux) {
                return Collections.unmodifiableSet(topVers.getOrDefault(grpId, Collections.emptySet()));
            }
        }

        /**
         * @return All topology versions for all groups where WAL rebalance has been used.
         */
        public static Map<Integer, Set<Long>> allRebalances() {
            synchronized (mux) {
                return Collections.unmodifiableMap(topVers);
            }
        }

        /**
         * Cleans all rebalances history.
         */
        public static void cleanup() {
            synchronized (mux) {
                topVers.clear();
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {

            if (((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage) ((GridIoMessage)msg).message();

                IgniteDhtDemandedPartitionsMap map = demandMsg.partitions();

                    int grpId = demandMsg.groupId();
                    long topVer = demandMsg.topologyVersion().topologyVersion();

                    synchronized (mux) {
                        topVers.computeIfAbsent(grpId, v -> new HashSet<>()).add(topVer);
                    }
            }

            if (((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage supplyMsg = (GridDhtPartitionSupplyMessage) ((GridIoMessage)msg).message();

                int grpId = supplyMsg.groupId();
                long topVer = supplyMsg.topologyVersion().topologyVersion();

                synchronized (mux) {
                    topVers.computeIfAbsent(grpId, v -> new HashSet<>()).add(topVer);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

}
