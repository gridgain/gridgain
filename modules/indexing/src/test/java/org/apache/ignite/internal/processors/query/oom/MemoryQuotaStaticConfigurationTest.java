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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

/**
 * Test case for static memory configuration.
 */
public class MemoryQuotaStaticConfigurationTest extends AbstractMemoryQuotaStaticConfigurationTest {
    /** */
    private static String qryMore60Percent;

    /** */
    private static String qry50Percent;

    /** */
    private static String qry25Percent;

    /** */
    private static String qry10Percent;

    /** {@inheritDoc} */
    @Override protected boolean startClient() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean fromClient() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        initGrid("0", "50%", false);

        String qry = "SELECT listagg(p1.name), listagg(p1.name), listagg(p1.name), listagg(p1.name), " +
            "listagg(p1.name), listagg(p1.name), listagg(p1.name), listagg(p1.name), " +
            "listagg(p1.name), listagg(p1.name), listagg(p1.name), listagg(p1.name) " +
            "FROM person p1 JOIN person p2 WHERE p1.id < ";
        int param = 0;

        // Find queries which consume 10%, 25%, 50% and more than 60% of heap.
        for (int i = PERS_CNT; i >= 0; i -= 100) {
            try {
                grid("client").cache(DEFAULT_CACHE_NAME)
                    .query(new SqlFieldsQuery(qry + i )
                    .setLazy(true))
                    .getAll();

                param = i; // We found first value with memory consumption less than 60%.

                break;
            }
            catch (CacheException e) {
                IgniteSQLException sqlEx = X.cause(e, IgniteSQLException.class);

                assertNotNull("Wrong exception: " + X.getFullStackTrace(e), sqlEx);

                assertTrue("Wrong message:" + sqlEx.getMessage(), sqlEx.getMessage().contains("Query quota exceeded."));
            }
        }

        if (param <= 0 || param >= PERS_CNT)
            throw new IllegalStateException("Can not start test, quota can not be determined. " +
                "Consider changing the query. Query parameter=" + param);

        qry50Percent = qry + param;
        qry25Percent = qry + (param / 2);
        qry10Percent = qry + (param / 5);
        qryMore60Percent = qry + PERS_CNT;

        if (log.isInfoEnabled()) {
            log.info("Query with memory consumption more than 60%: " + qryMore60Percent);
            log.info("Query with memory consumption a bit less than 50%: " + qry50Percent);
            log.info("Query with memory consumption about 25%: " + qry25Percent);
            log.info("Query with memory consumption about 10%: " + qry10Percent);

        }

        afterTest();
    }

    /**
     * Test 1.1. Check default sql memory quota and offloading configuration values applied
     * Start node with default config
     * Execute non-OOM query
     * Ensure query succeeds
     * Execute query that would take 60% heap / DFLT_QUERY_THREAD_POOL_SIZE memory
     * Ensure that query succeeds
     * Execute OOM SQL query that take > 60% heap
     * Ensure that SqlException quota exceed is thrown.
     * Ensure that no disk spilling occurs
     * Execute in parallel N queries that would fit memory altogether
     * Ensure that all queries succeeds
     * Execute in parallel N queries such that each take less than 60% heap / DFLT_QUERY_THREAD_POOL_SIZE but in total would overflow all memory
     * Ensure that OutOfMemoryError is thrown
     * @throws Exception If failed.
     */
    @Test
    public void testDefaults() throws Exception {
        initGrid(null, null, null);

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry50Percent);

        checkQuery(Result.ERROR_GLOBAL_QUOTA, qryMore60Percent);

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry10Percent, 3, 1);

        checkQuery(Result.ERROR_GLOBAL_QUOTA, qry25Percent, 3, 1);
    }

    /**
     * Test 1.2. Check user can override default offloading configuration value
     *
     * Scenario
     * Start node with SqlOffloadingEnabled set to true
     * Execute non-OOM SQL query
     * Ensure that query succeeds
     * Ensure that no disk spilling occurs
     * Execute OOM SQL query
     * Ensure that query succeeds
     * Ensure that disk spilling occurs
     * Ensure that temporary files are deleted
     * Execute in parallel N queries that would overflow memory altogether
     * Ensure that offloading happened
     * @throws Exception If failed.
     */
    @Test
    public void testOffloadingEnabled() throws Exception {
        initGrid(null, null, true);

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry50Percent);

        checkQuery(Result.SUCCESS_WITH_OFFLOADING, qryMore60Percent);

        checkQuery(Result.SUCCESS_WITH_OFFLOADING, qry50Percent, 2, 1);
    }

    /**
     * Test 1.3. Check user can override default per-query quota configuration value
     *
     * Scenario
     * Start node with SqlQueryMemoryQuota set to 60% of heap
     * Execute two queries that both fit into memory
     * Ensure that both queries succeeds
     * Execute two queries that would overflow memory in parallel
     * Ensure that SqlException quota exceed is thrown for one of queries and other query succeeds
     * @throws Exception If failed.
     */
    @Test
    public void testQueryQuota() throws Exception {
        initGrid("0", "60%", null);

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry25Percent, 2, 1);

        checkQuery(Result.ERROR_QUERY_QUOTA, qry50Percent, 2, 1);
    }

    /**
     * Test 1.4. Check user can override default global configuration value
     *
     * Scenario
     * Start node with SqlGlobalMemoryQuota set to 1024
     * Execute SQL query that requires > 1024 bytes for result
     * Ensure that SqlException quota exceed is thrown.
     * Ensure that no disk spilling occurs
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalQuotaOverride() throws Exception {
        initGrid(null, "1024", null);

        checkQuery(Result.ERROR_QUERY_QUOTA, qry25Percent, 2, 1);
    }

    /**
     * Test 2.1. Check offloading happens with per-query quota configuration
     *
     * Scenario
     * Start node with SqlQueryMemoryQuota set to 60% of heap and SqlOffloadingEnabled set to true
     * Execute in parallel two queries that would overflow memory
     * Ensure that both queries succeeds
     * Ensure that disk spilling happens
     * Ensure that temporary files are deleted
     * @throws Exception If failed.
     */
    @Test
    public void testOffloadingWithPerQueryQuota() throws Exception {
        initGrid("0", "60%", true);

        checkQuery(Result.SUCCESS_NO_OFFLOADING, qry25Percent, 1, 1);

        checkQuery(Result.SUCCESS_WITH_OFFLOADING, qryMore60Percent, 2, 1);
    }

    /**
     * Test 2.3. Check legacy property for disk offload is not supported
     *
     * Scenario
     * Start node with default configuration and IGNITE_SQL_USE_DISK_OFFLOAD set to true
     * Execute OOM SQL query
     * Ensure that SqlException query quota exceeded is thrown
     * Ensure that no disk spilling happens.
     * @throws Exception If failed.
     */
    @Test
    public void testLegacyOffloadPropertyNotSupported() throws Exception {
        System.setProperty("IGNITE_SQL_USE_DISK_OFFLOAD", "true");
        try {
            initGrid(null, null, null);

            checkQuery(Result.ERROR_GLOBAL_QUOTA, qryMore60Percent);
        }
        finally {
            System.clearProperty("IGNITE_SQL_USE_DISK_OFFLOAD");
        }
    }

    /**
     * Test 2.4. Check legacy property for global memory quota is not supported
     *
     * Scenario
     * Start node with default configuration and IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE set to 0
     * Execute OOM SQL query
     * Ensure that SqlException query quota exceeded is thrown
     * Ensure that no disk spilling happens.
     * @throws Exception If failed.
     */
    @Test
    public void testLegacyGlobalQuotaPropertyNotSupported() throws Exception {
        System.setProperty("IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE", "0");
        try {
            initGrid(null, null, null);

            checkQuery(Result.ERROR_GLOBAL_QUOTA, qryMore60Percent);
        }
        finally {
            System.clearProperty("IGNITE_SQL_USE_DISK_OFFLOAD");
        }
    }

    /**
     * Test 2.5. Check legacy property for global memory quota is not supported
     *
     * Scenario
     * Start node with default configuration and IGNITE_DEFAULT_SQL_QUERY_MEMORY_LIMIT set to 1024
     * Execute in parallel N queries that would overflow memory altogether
     * Ensure that OutOfMemoryError is thrown
     * @throws Exception If failed.
     */
    @Test
    public void testLegacyQueryQuotaPropertyNotSupported() throws Exception {
        System.setProperty("IGNITE_DEFAULT_SQL_QUERY_MEMORY_LIMIT", "1024");
        try {
            initGrid(null, null, null);

            checkQuery(Result.ERROR_QUERY_QUOTA, qry25Percent);
        }
        finally {
            System.clearProperty("IGNITE_SQL_USE_DISK_OFFLOAD");
        }
    }
}
