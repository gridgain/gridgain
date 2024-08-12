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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/** */
public class ScanQueriesTopologyMappingTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setClientMode(client);
    }

    /** */
    @Test
    public void testPartitionedQueryWithRebalance() throws Exception {
        checkQueryWithRebalance(CacheMode.PARTITIONED);
    }

    /** */
    @Test
    public void testReplicatedQueryWithRebalance() throws Exception {
        checkQueryWithRebalance(CacheMode.REPLICATED);
    }

    /** */
    @Test
    public void testPartitionedQueryWithNodeFilter() throws Exception {
        checkQueryWithNodeFilter(CacheMode.PARTITIONED);
    }

    /** */
    @Test
    public void testReplicatedQueryWithNodeFilter() throws Exception {
        checkQueryWithNodeFilter(CacheMode.REPLICATED);
    }

    /** */
    @Test
    public void testLocalCacheQueryMapping() throws Exception {
        IgniteEx ign0 = startGrid(0);

        IgniteCache<Object, Object> cache = ign0.createCache(new CacheConfiguration<>(GridAbstractTest.DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.LOCAL));

        cache.put(1, 2);

        startGrid(1);

        ScanQuery<Object, Object> qry = new ScanQuery<>();

        {
            List<Cache.Entry<Object, Object>> res0 = grid(0).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

            assertEquals(1, res0.size());
            assertEquals(1, res0.get(0).getKey());
            assertEquals(2, res0.get(0).getValue());
        }

        {
            List<Cache.Entry<Object, Object>> res1 = grid(1).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

            assertTrue(res1.isEmpty());
        }
    }

    /**
     * Check if local scan query inside tx do not wait for exchange.
     */
    @Test
    public void testReplicatedQueryWithTx() throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx srv2 = startGrid(1);

        IgniteCache<Object, Object> cache0 = srv.createCache(new CacheConfiguration<>(GridAbstractTest.DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCopyOnRead(false)
        );

        IntStream.range(1, 100).forEach(i -> cache0.put(i, i));

        client = true;

        IgniteEx client = startGrid(10);
        IgniteCache<Integer, Integer> clientCache = client.cache(GridAbstractTest.DEFAULT_CACHE_NAME);

        CyclicBarrier barrier = new CyclicBarrier(2);

        IgniteInternalFuture asyncOpFuture = GridTestUtils.runAsync(() -> {
            barrier.await(); // Await tx start.

            srv.createCache("myCache"); // Force Exchange.
        });

        try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            // Lock key.
            clientCache.get(1);

            // Register exchange listener.
            srv.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
                @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                    try {
                        barrier.await(); // Release main thread.
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            // Start query from server.
            QueryCursor<Cache.Entry<Object, Object>> query = cache0.query(new ScanQuery<>().setPageSize(10));

            barrier.await(); // Release async task thread.
            barrier.await(); // Await next exchange.

            query.getAll();

            tx.commit();
        }

        asyncOpFuture.get();
    }

    /**  */
    private void checkQueryWithRebalance(CacheMode cacheMode) throws Exception {
        IgniteEx ign0 = startGrid(0);

        IgniteCache<Object, Object> cache0 = ign0.createCache(new CacheConfiguration<>(GridAbstractTest.DEFAULT_CACHE_NAME)
            .setCacheMode(cacheMode));

        cache0.put(1, 2);

        blockRebalanceSupplyMessages(ign0, DEFAULT_CACHE_NAME, getTestIgniteInstanceName(1));

        startGrid(1);

        client = true;

        startGrid(10);

        int part = ign0.affinity(DEFAULT_CACHE_NAME).partition(1);

        for (int i = 0; i < 100; i++) {
            for (Ignite ign : G.allGrids()) {
                IgniteCache<Object, Object> cache = ign.cache(DEFAULT_CACHE_NAME);

                //check scan query
                List<Cache.Entry<Object, Object>> res = cache.query(new ScanQuery<>()).getAll();

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());

                res = new ArrayList<>();

                //check iterator
                for (Cache.Entry<Object, Object> entry : cache)
                    res.add(entry);

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());

                // check scan query by partition
                res = cache.query(new ScanQuery<>(part)).getAll();

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());
            }

            ScanQuery<Object, Object> qry = new ScanQuery<>().setLocal(true);

            {
                List<Cache.Entry<Object, Object>> res0 = grid(0).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

                assertEquals(1, res0.size());
                assertEquals(1, res0.get(0).getKey());
                assertEquals(2, res0.get(0).getValue());
            }

            {
                List<Cache.Entry<Object, Object>> res1 = grid(1).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

                assertTrue(res1.isEmpty());
            }

            {
                List<Cache.Entry<Object, Object>> res1 = grid(10).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

                assertTrue(res1.isEmpty());
            }
        }
    }

    /** */
    private void checkQueryWithNodeFilter(CacheMode cacheMode) throws Exception {
        IgniteEx ign0 = startGrid(0);
        String name0 = ign0.name();

        IgniteCache<Object, Object> cache0 = ign0.createCache(new CacheConfiguration<>(GridAbstractTest.DEFAULT_CACHE_NAME)
            .setCacheMode(cacheMode)
            .setNodeFilter(node -> name0.equals(node.attribute(ATTR_IGNITE_INSTANCE_NAME))));

        cache0.put(1, 2);

        startGrid(1);

        client = true;

        startGrid(10);

        int part = ign0.affinity(DEFAULT_CACHE_NAME).partition(1);

        for (int i = 0; i < 100; i++) {
            for (Ignite ign : G.allGrids()) {
                IgniteCache<Object, Object> cache = ign.cache(GridAbstractTest.DEFAULT_CACHE_NAME);

                List<Cache.Entry<Object, Object>> res = cache.query(new ScanQuery<>()).getAll();

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());

                res = new ArrayList<>();

                for (Cache.Entry<Object, Object> entry : cache)
                    res.add(entry);

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());

                res = cache.query(new ScanQuery<>(part)).getAll();

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());
            }

            ScanQuery<Object, Object> qry = new ScanQuery<>().setLocal(true);

            {
                List<Cache.Entry<Object, Object>> res0 = grid(0).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

                assertEquals(1, res0.size());
                assertEquals(1, res0.get(0).getKey());
                assertEquals(2, res0.get(0).getValue());
            }

            {
                List<Cache.Entry<Object, Object>> res1 = grid(1).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

                assertTrue(res1.isEmpty());
            }

            {
                List<Cache.Entry<Object, Object>> res1 = grid(10).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

                assertTrue(res1.isEmpty());
            }
        }
    }

    /** */
    private void blockRebalanceSupplyMessages(IgniteEx sndNode, String cacheName, String dstNodeName) {
        int grpId = sndNode.cachex(cacheName).context().groupId();

        TestRecordingCommunicationSpi comm0 = (TestRecordingCommunicationSpi)sndNode.configuration().getCommunicationSpi();
        comm0.blockMessages((node, msg) -> {
            String dstName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

            if (dstNodeName.equals(dstName) && msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)msg;
                return msg0.groupId() == grpId;
            }

            return false;
        });
    }
}
