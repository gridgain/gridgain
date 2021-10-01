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

package org.apache.ignite.internal.processors.cache.transaction;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class SDSB12162 extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)
                )
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return super.getTestIgniteInstanceName() + '_';
    }

    /**
     * Start a cluster of 3 nodes
     * Set the topology
     * Launch client
     * Create a cache with backup 1
     * Run a transactional load in the cache with select and getAndPutIfAbsent
     * Stop one node
     * Clear the LFS on it
     * Start the node
     *
     * TODO ожидаем появления
     * Failed to lock keys (all partition nodes left the grid)
     * Failed to find data nodes for cache
     *
     * @see org.apache.ignite.internal.processors.cache.distributed.dht.NotMappedPartitionInTxTest
     * @see org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractSqlCoordinatorFailoverTest#testStartLastServerFails()
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
        IgniteEx n0 = startGrid(0);
        IgniteEx n1 = startGrid(1);
        IgniteEx n2 = startGrid(2);

        String problemNodeName = n2.name();

        n0.cluster().state(ACTIVE);

        IgniteEx client = startGrid(getTestIgniteInstanceName(3), cfg -> {
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

            // Set only n2 and the client will connect only to it.
            for (IgniteEx n : Arrays.asList(n0, n1, n2))
                ipFinder.registerAddresses(((TcpDiscoveryNode)n.localNode()).socketAddresses());

            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

            return cfg.setClientMode(true);
        });

        IgniteCache<Integer, String> cache = client.createCache(cacheConfig(DEFAULT_CACHE_NAME));

        log.warning("!!!! Start transaction load");

        AtomicBoolean stop = new AtomicBoolean(false);

        List<Integer> integers = primaryKeys(n2.cache(DEFAULT_CACHE_NAME), 1_000);

        IgniteInternalFuture<Object> loadFut = runAsync(() -> {
            int i = 0;

            while (!stop.get()) {
                try (Transaction tx = client.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                    cache.query(new SqlFieldsQuery("SELECT * FROM \"" + DEFAULT_CACHE_NAME + "\".String")).getAll();

                    Integer val = integers.get(i++ % integers.size());

                    cache.getAndPutIfAbsent(val, Integer.toHexString(val));

                    tx.commit();

                    if ((i % 100) == 0)
                        log.warning("!!!! Iteration: " + i);
                }
                catch (Throwable t) {
                    log.error("!!!! ERROR", t);
                }
            }

            return null;
        });

        U.sleep(5_000);

        stopGrid(2);

        awaitPartitionMapExchange();

        log.warning("!!!! Clear LFS: " + problemNodeName);

        cleanPersistenceDir(problemNodeName);

        U.sleep(5_000);

        IgniteEx n2_new = startGrid(2);

        U.sleep(5_000);

        stop.set(true);

        loadFut.get(getTestTimeout());

        n2_new.cluster().state(INACTIVE);
    }

    private CacheConfiguration<Integer, String> cacheConfig(String cacheName) {
        return new CacheConfiguration<Integer, String>(cacheName)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setIndexedTypes(Integer.class, String.class);
    }
}
