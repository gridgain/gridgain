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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Query quota test for client
 */
public class ClientQueryQuotaTest extends DiskSpillingAbstractTest {
    /** */
    private static final String QUERY_512_TO_1024 = "SELECT DISTINCT id FROM person WHERE id < 8";

    /** */
    private static final String QUERY_1024_TO_2048 = "SELECT DISTINCT id FROM person WHERE id < 16";

    /** */
    private boolean client;

    /** */
    private static String defaultQryQuota = "1024";


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlOffloadingEnabled(IgniteConfiguration.DFLT_SQL_QUERY_OFFLOADING_ENABLED);
        cfg.setSqlQueryMemoryQuota(defaultQryQuota);
        cfg.setSqlGlobalMemoryQuota(IgniteConfiguration.DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA);

        if (client)
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean startClient() {
        return false;
    }

    /**
     * Test for clients
     * Start server node with default configuration and QueryMemoryQuota set to 1024
     * Start client of type Client type with default configuration and QueryMemoryQuota set to 2048
     * Execute SQL that would take > 1024 and < 2048 bytes for result
     * Ensure that SqlException query quota exceeded is thrown
     * Start client of type Client type with default configuration and QueryMemoryQuota set to 512
     * Execute SQL that would take > 512 and < 1024 bytes for result
     * Ensure that SqlException query quota exceeded is thrown
     */
    @Test
    public void testClientQueryQuota() throws Exception {
        client = true;
        defaultQryQuota = "2048";

        // Start client.
        startGrid(1);

        // Check queries correctness.
        GridTestUtils.assertThrowsWithCause(() -> runQueryWithMemLimit(QUERY_512_TO_1024, 511),
            IgniteSQLException.class);
        runQueryWithMemLimit(QUERY_512_TO_1024, 1024);
        GridTestUtils.assertThrowsWithCause(() -> runQueryWithMemLimit(QUERY_1024_TO_2048, 1023),
            IgniteSQLException.class);
        runQueryWithMemLimit(QUERY_1024_TO_2048, 2048);

        GridTestUtils.assertThrows(log, () -> {
            runQueryFromClient(QUERY_1024_TO_2048, 1);
        }, CacheException.class, "SQL query run out of memory: Query quota exceeded.");

        defaultQryQuota = "512";

        // Start client.
        startGrid(2);

        GridTestUtils.assertThrows(log, () -> {
            runQueryFromClient(QUERY_512_TO_1024, 2);
        }, CacheException.class, "SQL query run out of memory: Query quota exceeded.");

    }

    /** */
    public void runQueryWithMemLimit(String sql, long qryQuota) {
        IgniteEx client = grid(1);

        client.cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQueryEx(sql, true)
                .setMaxMemory(qryQuota))
            .getAll();
    }

    /** */
    public void runQueryFromClient(String sql, int idx) {
        IgniteEx client = grid(idx);

        assertTrue(client.cluster().node().isClient());

        client.cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery(sql))
            .getAll();
    }
}
