/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.big;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class BigPageTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize((long) (3.5 * 1024 * 1024 * 1024))
            )
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(128 * 1024);
//            .setPageSize(1024);

        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(memCfg);
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
    }

    @Test
    public void test() throws Exception {
        IgniteEx node = startGrid("node");

        IgniteConfiguration clientCfg = getConfiguration("test-client");
        clientCfg.setClientMode(true);
        clientCfg.setCacheConfiguration(new CacheConfiguration().setName("test"));

        IgniteEx ex = startClientGrid(clientCfg);

        node.cluster().state(ClusterState.ACTIVE);

        node.cache("test").put(1, 1);

        assertEquals(1, ex.cache("test").get(1));

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<Integer, String>("some-cache")
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        IgniteCache<Integer, String> cache = node.getOrCreateCache(cacheCfg);

        System.out.println("asd");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append(i);
        }

        String tmp = sb.toString();

        for (int i = 0; i < 100_000; i++) {
            cache.put(i, tmp + i);
        }

        for (int i = 0; i < 100_000; i++) {
            assertEquals(tmp + i, cache.get(i));
        }

        forceCheckpoint(node);

        for (int i = 0; i < 100_000; i++) {
            if ((i % 2) == 0)
                cache.remove(i);
        }

        forceCheckpoint(node);

        for (int i = 0; i < 100_000; i++) {
            if ((i % 2) == 0)
                assertNull(cache.get(i));
            else
                assertEquals(tmp + i, cache.get(i));
        }

        node.close();

        System.out.println("restart!");

        node = startGrid("node");

        node.cluster().state(ClusterState.ACTIVE);

        cache = node.cache("some-cache");

        for (int i = 0; i < 100_000; i++) {
            if ((i % 2) == 0)
                assertNull(cache.get(i));
            else
                assertEquals(tmp + i, cache.get(i));
        }

        System.out.println("done");
    }
}
