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

package org.apache.ignite.spi.communication.tcp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.EnvironmentType;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

public class GridTcpCommunicationInverseConnectionEstablishingTest extends GridCommonAbstractTest {
    private static final IgnitePredicate<ClusterNode> CLIENT_PRED = c -> c.isClient();

    private boolean client;

    /** */
    CacheConfiguration[] ccfgs;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(true);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        if (client) {
            cfg.setEnvironmentType(EnvironmentType.VIRTUALIZED);
            cfg.setClientMode(true);
        }

        return cfg;
    }

    @Test
    public void testUnreachableClient() throws Exception {
        int srvs = 5;

        for (int i = 0; i < srvs; i++) {
            client = i >= srvs;

            ccfgs = cacheConfigurations1();

            startGrid(i);
        }

        client = false;

        startGrid(srvs);

        client = true;

        startGrid(srvs + 1);

        checkCaches(srvs + 2, 2, false);
    }

    /** */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
            if (CLIENT_PRED.apply(node)) {
                Map<String, Object> attrs = new HashMap<>(node.attributes());

                attrs.put(createAttributeName(ATTR_ADDRS), Collections.singleton("172.31.30.132"));
                attrs.put(createAttributeName(ATTR_PORT), 47200);
                attrs.put(createAttributeName(ATTR_EXT_ADDRS), Collections.emptyList());
                attrs.put(createAttributeName(ATTR_HOST_NAMES), Collections.emptyList());

                ((TcpDiscoveryNode)(node)).setAttributes(attrs);
            }

            return super.createTcpClient(node, connIdx);
        }

        /**
         * @param name Name.
         */
        private String createAttributeName(String name) {
            return getClass().getSimpleName() + '.' + name;
        }
    }

    /** */
    static final String CACHE_NAME_PREFIX = "cache-";

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    protected final CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @return Cache configurations.
     */
    final CacheConfiguration[] cacheConfigurations1() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[2];

        ccfgs[0] = cacheConfiguration(CACHE_NAME_PREFIX + 0, ATOMIC);
        ccfgs[1] = cacheConfiguration(CACHE_NAME_PREFIX + 1, TRANSACTIONAL);

        return ccfgs;
    }

    /**
     * @param nodes Number of nodes.
     * @param caches Number of caches.
     */
    final void checkCaches(int nodes, int caches, boolean awaitExchange) throws InterruptedException {
        if (awaitExchange)
            awaitPartitionMapExchange();

        for (int i = 0; i < nodes; i++) {
            for (int c = 0; c < caches; c++) {
                IgniteEx ig = ignite(i);

                if (ig.configuration().isClientMode()) {
                    IgniteCache<Integer, Integer> cache = ig.cache(CACHE_NAME_PREFIX + c);

                    for (int j = 0; j < 10; j++) {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        Integer key = rnd.nextInt(1000);

                        cache.put(key, j);

                        assertEquals((Integer)j, cache.get(key));
                    }
                }
            }
        }
    }
}
