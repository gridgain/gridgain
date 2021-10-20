/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

public class TempTest extends GridCommonAbstractTest {

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setRebalanceTimeout(1_000_000);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
        );

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    @Test
    public void testIt() throws Exception {
        IgniteEx fooNode = startGrid("foo");
        IgniteEx barNode = startGrid("bar");
        IgniteEx qweNode = startGrid("qwe");

        fooNode.cluster().state(ClusterState.ACTIVE);

        IgniteCache<String, String> cache = fooNode.getOrCreateCache(
            new CacheConfiguration<String, String>("txCache")
                .setBackups(2)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        String barKey = "foo";

        assert fooNode.affinity("txCache").isPrimary(barNode.localNode(), barKey);

        try (Transaction tx = fooNode.transactions().txStart()) {
            cache.put(barKey, "bar");

            tx.commit();
        }

        Thread.sleep(10_000);

        System.out.println("test");
    }

    @Test
    public void testIdleVerify() throws Exception {
        IgniteEx fooNode = startGrid("foo");
        IgniteEx barNode = startGrid("bar");
        IgniteEx qweNode = startGrid("qwe");

        IgniteEx clientNode = fooNode;


        fooNode.cluster().state(ClusterState.ACTIVE);

        String cacheName = "txCache";

        IgniteCache<String, String> cache = clientNode.getOrCreateCache(
            new CacheConfiguration<String, String>(cacheName)
                .setBackups(2)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        String barKey = "aa ";

        int partNum = fooNode.affinity(cacheName).partition(barKey);

        assert fooNode.affinity(cacheName).isPrimary(barNode.localNode(), barKey);

        List<ClusterNode> nodes = new ArrayList<>(fooNode.affinity(cacheName).mapPartitionToPrimaryAndBackups(partNum));

        assert barNode.name().equals(nodes.get(0).consistentId());
        assert qweNode.name().equals(nodes.get(1).consistentId());
        assert fooNode.name().equals(nodes.get(2).consistentId());

        try (Transaction tx = fooNode.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {
            cache.put(barKey, "bar");

            tx.commit();
        }

        GridDhtLocalPartition part
            = fooNode.context().cache().context().cacheContext(CU.cacheId(cacheName)).topology().localPartition(partNum);

//        part.updateCounter()



//        assertEquals(
//            5L,
//            qweNode.context().cache().context().cacheContext(CU.cacheId(cacheName)).topology().lastTopologyChangeVersion().topologyVersion()
//        );


        try (Transaction tx = fooNode.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {
            GridDhtPartitionsExchangeFuture.fut.reset();

            IgniteInternalFuture nodeStop = GridTestUtils.runAsync(
                () -> stopGrid(barNode.name(), true, false)
            );

            Thread.sleep(500);

//            assert GridTestUtils.waitForCondition(
//                () -> fooNode.context().cache().context().cacheContext(CU.cacheId(cacheName)).topology().lastTopologyChangeVersion().topologyVersion() == 4L,
//                10000
//            );

            cache.put(barKey, "qwe");

//            GridTestUtils.runAsync(
//                () -> {
//                    Thread.sleep(1000);
//                    GridDhtPartitionsExchangeFuture.fut.onDone();
//                    return 10;
//                }
//            );

            tx.commit();

            GridDhtPartitionsExchangeFuture.fut.onDone();
        }

        IdleVerifyResultV2 verify = idleVerify(fooNode, cacheName);

        System.out.println(verify);
    }

    @Test
    public void foo() throws Exception {
        GridFutureAdapter fut = new GridFutureAdapter();

        fut.get();
    }
}
