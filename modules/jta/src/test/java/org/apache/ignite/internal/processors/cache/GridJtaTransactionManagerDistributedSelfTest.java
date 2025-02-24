/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.objectweb.jotm.Current;
import org.objectweb.jotm.Jotm;

import javax.cache.configuration.Factory;
import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

/**
 * JTA Tx Manager distributed test.
 */
public class GridJtaTransactionManagerDistributedSelfTest extends GridCommonAbstractTest {
    /** IP finder for the first topology. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER_1 = new TcpDiscoveryVmIpFinder(true);

    /** IP finder for the second topology. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER_2 = new TcpDiscoveryVmIpFinder(true);

    /** Cluster 1 server instance name. */
    private static final String CLUSTER_1_SERVER_NAME = "cluster1srv";

    /** Cluster 1 client instance name. */
    private static final String CLUSTER_1_CLIENT_NAME = "cluster1client";

    /** Cluster 2 server instance name. */
    private static final String CLUSTER_2_SERVER_NAME = "cluster2srv";

    /** Cluster 2 client instance name. */
    private static final String CLUSTER_2_CLIENT_NAME = "cluster2client";

    /** Java Open Transaction Manager facade. */
    private static Jotm jotm;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        if (igniteInstanceName.contains("client"))
            cfg.getTransactionConfiguration().setTxManagerFactory(new TestTxManagerFactory());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        jotm = new Jotm(true, false);

        Current.setAppServer(false);

        IgniteConfiguration cluster1SrvCfg = getConfiguration(CLUSTER_1_SERVER_NAME)
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER_1));

        startGrid(cluster1SrvCfg);

        IgniteConfiguration cluster1ClientCfg = getConfiguration(CLUSTER_1_CLIENT_NAME)
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER_1));

        startClientGrid(cluster1ClientCfg);

        IgniteConfiguration cluster2SrvCfg = getConfiguration(CLUSTER_2_SERVER_NAME)
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER_2));

        startGrid(cluster2SrvCfg);

        IgniteConfiguration cluster2ClientCfg = getConfiguration(CLUSTER_2_CLIENT_NAME)
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER_2));

        startClientGrid(cluster2ClientCfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        jotm.stop();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJtaTxCommit() throws Exception {
        IgniteEx cluster1client = grid(CLUSTER_1_CLIENT_NAME);
        IgniteEx cluster2client = grid(CLUSTER_2_CLIENT_NAME);

        TransactionManager jtaTm = jotm.getTransactionManager();

        IgniteCache<Object, Object> cluster1cache = cluster1client.cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cluster2cache = cluster2client.cache(DEFAULT_CACHE_NAME);

        jtaTm.begin();

        cluster1cache.put(1, Integer.toString(1));
        cluster2cache.put(1, Integer.toString(1));

        jtaTm.commit();

        assertTrue("Record not found", cluster1cache.containsKey(1));
        assertTrue("Record not found", cluster2cache.containsKey(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJtaTxRollbackOnCommitFailure() throws Exception {
        IgniteEx cluster1client = grid(CLUSTER_1_CLIENT_NAME);
        IgniteEx cluster2client = grid(CLUSTER_2_CLIENT_NAME);

        TransactionManager jtaTm = jotm.getTransactionManager();

        IgniteCache<Object, Object> cluster1cache = cluster1client.cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cluster2cache = cluster2client.cache(DEFAULT_CACHE_NAME);

        jtaTm.begin();

        cluster1cache.put(1, Integer.toString(1));
        cluster2cache.put(1, Integer.toString(1));

        cluster2client.close();

        try {
            jtaTm.commit();

            fail();
        }
        catch (RollbackException ex) {
            assertFalse("No records when rolling back", cluster1cache.containsKey(1));
        }
    }

    /**
     *
     */
    static class TestTxManagerFactory implements Factory<TransactionManager> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public TransactionManager create() {
            return jotm.getTransactionManager();
        }
    }
}
