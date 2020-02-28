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
package org.apache.ignite.internal.processors.query.oom;

import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test cases for interaction of static and dynamic configuration.
 */
public class MemoryQuotaStaticAndDynamicConfigurationTest extends AbstractMemoryQuotaStaticConfigurationTest {
    /**
     * start with quotas turn off
     * enable quota runtime via JMX
     * ensure quota enabled
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalQuota() throws Exception {
        initGrid("0", "0", null);

        final String qry = "SELECT * FROM person";

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry);

        setGlobalQuota("100");

        checkQuery(Result.ERROR_GLOBAL_QUOTA, qry);

        setGlobalQuota("0"); // Disable global quota.

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry);
    }

    /**
     * start with default configuration
     * enable offloading runtime via JMX
     * ensure offloading enabled
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOffloading() throws Exception {
        initGrid("0", "0", null);

        final String qry = "SELECT * FROM person";

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry);

        setDefaultQueryQuota("100");

        checkQuery(Result.ERROR_QUERY_QUOTA, qry);

        setDefaultQueryQuota("0"); // Disable query quota.

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry);
    }

    /**
     * start with default configuration
     * enable per-query quota runtime via JMX
     * ensure per-query enabled
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryQuota() throws Exception {
        initGrid("0", "0", null);

        final String qry = "SELECT * FROM person";

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry);

        setDefaultQueryQuota("100");

        checkQuery(Result.ERROR_QUERY_QUOTA, qry);

        setDefaultQueryQuota("0"); // Disable query quota.

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry);
    }

    /**
     * start with offloading enabled
     * disable offloading runtime via JMX
     * ensure offloading disabled
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOffloadingEnabledByDefault() throws Exception {
        initGrid(null, "100", true);

        final String qry = "SELECT * FROM person ORDER BY id";

        checkQuery(Result.SUCCESS_WITH_OFFLOADING, qry);

        setOffloadingEnabled(false);

        checkQuery(Result.ERROR_QUERY_QUOTA, qry);

        setOffloadingEnabled(true);

        checkQuery(Result.SUCCESS_WITH_OFFLOADING, qry);
    }

    /**
     * start with two nodes with default settings
     * dynamically set tiny quota on one of nodes via JMX
     * execute local query on both nodes.
     * ensure meaningful exception is thrown on node where tiny quota is configured
     * ensure no exception is thrown on node with default quota
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTwoNodesDifferentSettings() throws Exception {
        initGrid(null, null, null);

        startGrid(1);

        awaitPartitionMapExchange();

        final String qry = "SELECT * FROM person ORDER BY name";

        grid(1).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(qry).setLocal(true)).getAll();
        grid(1).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(qry).setLocal(true)).getAll();

        memoryManager(grid(0)).setQueryQuota("10");

        GridTestUtils.assertThrows(log, () -> {
            grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(qry).setLocal(true)).getAll();
        }, CacheException.class, "SQL query run out of memory: Query quota exceeded.");

        grid(1).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(qry).setLocal(true)).getAll();
    }

    /** */
    private void setGlobalQuota(String newQuota) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setGlobalQuota(newQuota);
        }
    }

    /** */
    private void setDefaultQueryQuota(String newQuota) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setQueryQuota(newQuota);
        }
    }

    /** */
    private void setOffloadingEnabled(boolean enabled) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setOffloadingEnabled(enabled);
        }
    }
}
