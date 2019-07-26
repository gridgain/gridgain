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

package org.apache.ignite.spi.discovery.isolated;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.isolated.IsolatedCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test for {@link IsolatedDiscoverySpi}.
 */
public class IsolatedDiscoverySpiSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @param instanceName Instance name.
     * @return Ignite isolated configuration.
     */
    private IgniteConfiguration isolatedConfiguration(String instanceName) {
        return new IgniteConfiguration()
            .setIgniteInstanceName(instanceName)
            .setDiscoverySpi(new IsolatedDiscoverySpi())
            .setCommunicationSpi(new IsolatedCommunicationSpi())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            );
    }

    /**
     * Test that node starts and can execute cache operations.
     */
    @Test
    public void testCacheOperations() {
        Ignite ignite = Ignition.start(isolatedConfiguration("isolated-node"));

        ignite.cluster().active(true);

        assertTrue(ignite.cluster().active());

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("test1")
            .setCacheMode(REPLICATED)
            .setAtomicityMode(TRANSACTIONAL);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(ccfg);

        try(Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(1, 2);
            cache.put(3, new Pojo(UUID.randomUUID(), "Test"));

            tx.commit();
        }

        assertEquals(2, cache.get(1));

        CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>()
            .setName("test2")
            .setCacheMode(REPLICATED);

        IgniteCache<Object, Object> c2 = ignite.getOrCreateCache(ccfg2);

        c2.put(1, 2);

        assertEquals(2, c2.get(1));
    }

    /**
     * Test that isolated node restarts correctly.
     */
    @Test
    public void testRestatrt() {
        Ignite ignite = Ignition.start(isolatedConfiguration("isolated-node"));

        ignite.cluster().active(true);

        Object consistentId = ignite.cluster().localNode().consistentId();

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("test-restart");

        cache.put(1, 2);

        stopAllGrids();

        ignite = Ignition.start(isolatedConfiguration("isolated-node"));

        cache = ignite.getOrCreateCache("test-restart");

        assertEquals(2, cache.get(1));
        assertEquals(consistentId, ignite.cluster().localNode().consistentId());
    }

    /**
     * Test that several isolated nodes do not see each other.
     */
    @Test
    public void testSeveralIsolatedNodes() {
        Ignite ignite1 = Ignition.start(isolatedConfiguration("isolated-node-1"));
        ignite1.cluster().active(true);

        Ignite ignite2 = Ignition.start(isolatedConfiguration("isolated-node-2"));
        ignite2.cluster().active(true);

        assertEquals(1, ignite1.cluster().currentBaselineTopology().size());
        assertEquals(1, ignite2.cluster().currentBaselineTopology().size());
    }

    /**
     * Test that several isolated nodes do not see each other.
     * @throws Exception If failed
     */
    @Test
    public void testIsolatedNodeWithNormalNode() throws Exception {
        Ignite ignite1 = Ignition.start(isolatedConfiguration("isolated-node"));
        ignite1.cluster().active(true);

        Ignite ignite2 = Ignition.start(getConfiguration("normal-node"));
        ignite2.cluster().active(true);

        assertEquals(1, ignite1.cluster().currentBaselineTopology().size());
        assertEquals(1, ignite2.cluster().currentBaselineTopology().size());
    }

    /** Test POJO. */
    private static class Pojo {
        /** */
        private UUID id;

        /** */
        private String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        Pojo(UUID id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        public UUID getId() {
            return id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }
    }
}
