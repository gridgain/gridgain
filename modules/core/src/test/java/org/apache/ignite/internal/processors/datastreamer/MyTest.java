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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.ignite.TestStorageUtils.corruptDataEntry;

/**
 * Tests for {@code IgniteDataStreamerImpl}.
 */
public class MyTest extends GridCommonAbstractTest {
    private static final String CACHE_NAME = "testCache";

//    private static final String CACHE_2 = "testCache2";

    private static final String GROUP_NAME = "testGroup";

//    private static final String GROUP_2 = "testGroup2";

    private static final int nodeCount = 2;

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

        cfg.setCommunicationSpi(new MySpi());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(200 * 1024 * 1024)
        ));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMy() throws Exception {
        startGrids(nodeCount);

        Ignite g0 = grid(0);

        g0.cluster().state(ClusterState.ACTIVE);

        g0.createCache(getCacheConfiguration(0));

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache0 = g0.cache(CACHE_NAME + 0);

        for (int i = 0; i < 100; i++)
            cache0.put(i, i);

        awaitPartitionMapExchange();

        GridCacheContext<Object, Object> cacheCtx0 = grid(0).cachex(CACHE_NAME + 0).context();

        corruptDataEntry(cacheCtx0, 1, true, false, new GridCacheVersion(0, 0, 0), "broken");
        corruptDataEntry(cacheCtx0, 2, true, false, new GridCacheVersion(0, 5, 0), "broken");
        corruptDataEntry(cacheCtx0, 3, true, true, new GridCacheVersion(0, 0, 4), "broken");
        corruptDataEntry(cacheCtx0, 4, true, false, new GridCacheVersion(0, 0, 0), "broken");
        corruptDataEntry(cacheCtx0, 4, true, false, new GridCacheVersion(0, 0, 0), "broken");
        corruptDataEntry(cacheCtx0, 4, true, false, new GridCacheVersion(0, 0, 0), "broken");

            g0.createCache(getCacheConfiguration(1));

        awaitPartitionMapExchange(true, true, null);


        System.out.println("qr" + MySpi.allRebalances());

        System.out.println("grpIds: " + MySpi.rebGrpIds);

//        grpIds: [1251687456]
//          [ignite-sys-cache -2100569601, testCache1 1251687457, testCache0 1251687456]

        System.out.println(ignite(0).context().cache().caches().stream().map(cache -> cache.name() + " " + CU.cacheId(cache.name())).collect(Collectors.toList()));
    }

    private CacheConfiguration<Object, Object> getCacheConfiguration(int idx) {
        return new CacheConfiguration<>(CACHE_NAME + idx)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(8));
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

        public static final Set<Integer> rebGrpIds = new HashSet<>();

        public static final List<Message> msgs = new ArrayList<>();

        /** Lock object. */
        private static final Object mux = new Object();

        private static final AtomicInteger i = new AtomicInteger(0);

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
//            if (i.incrementAndGet() / 5 == 0) {
//                doSleep(100);
//            }

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
                    rebGrpIds.add(supplyMsg.groupId());

                    topVers.computeIfAbsent(grpId, v -> new HashSet<>()).add(topVer);
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

}
