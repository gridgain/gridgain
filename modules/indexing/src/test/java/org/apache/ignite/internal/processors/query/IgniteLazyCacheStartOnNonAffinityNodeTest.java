/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests that statically configured caches properly registered in the SQL engine
 * when the
 */
public class IgniteLazyCacheStartOnNonAffinityNodeTest extends AbstractIndexingCommonTest {
    /** Name of the 'proxy' cache. */
    private static final String PROXY_TABLE = "PROXY";

    /** Name of statically configured cache. */
    private static final String STATICALLY_CONFIGURED_TABLE = "TEST_TABLE";

    /** Optional cache configuration that should be registered on start. */
    private CacheConfiguration<Integer, String> staticallyConfiguredClientCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        List<CacheConfiguration<Integer, String>> cacheCfgs = new ArrayList<>();

        cacheCfgs.add(new CacheConfiguration<>(PROXY_TABLE));

        if (staticallyConfiguredClientCache != null)
            cacheCfgs.add(staticallyConfiguredClientCache);

        cfg.setCacheConfiguration(cacheCfgs.toArray(new CacheConfiguration[0]));

        return cfg;
    }

    /**
     * Creates a cache configuration.
     *
     * @return Cache configuration.
     */
    static CacheConfiguration<Integer, String> createSqlCacheCfg() {
        return new CacheConfiguration<Integer, String>()
            .setName(STATICALLY_CONFIGURED_TABLE)
            .setCacheMode(PARTITIONED)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setNodeFilter(node -> !node.consistentId().toString().endsWith("1"))
            .setSqlSchema("PUBLIC")
            .setQueryEntities(
                singletonList(new QueryEntity(Integer.class, String.class).setTableName(STATICALLY_CONFIGURED_TABLE))
            );
    }

    /**
     * Tests that a statically configured cache can be queried from a client node.
     *
     * @throws Exception Exception in case of failure.
     */
    @Test
    public void testStaticallyConfiguredCacheCanBeQueried() throws Exception {
        Ignite crd = startGridsMultiThreaded(2);

        Ignite clientA = startClientGrid(2);

        // Start a new client with statically configured cache.
        staticallyConfiguredClientCache = createSqlCacheCfg();
        startClientGrid(3);

        // Check that the cache can be queried via proxy.
        clientA.cache(PROXY_TABLE).query(new SqlFieldsQuery("SELECT * FROM " + STATICALLY_CONFIGURED_TABLE));

        // Check that the cache can be queried via proxy on server nodes (affinity and non-affinity).
        crd.cache(PROXY_TABLE).query(new SqlFieldsQuery("SELECT * FROM " + STATICALLY_CONFIGURED_TABLE));
        grid(1).cache(PROXY_TABLE).query(new SqlFieldsQuery("SELECT * FROM " + STATICALLY_CONFIGURED_TABLE));
    }
}
