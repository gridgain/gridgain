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
package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.oom.DiskSpillingAbstractTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Jdbc test for query quota configuration.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcQueryQuotaTest extends DiskSpillingAbstractTest {
    /** */
    private static final String QUERY_1024_TO_2048 = "SELECT DISTINCT id FROM person WHERE id < 8";

    /** */
    private static final String QUERY_2048_TO_4096 = "SELECT DISTINCT id FROM person WHERE id < 16";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlOffloadingEnabled(IgniteConfiguration.DFLT_SQL_QUERY_OFFLOADING_ENABLED);
        cfg.setSqlQueryMemoryQuota("1024");
        cfg.setSqlGlobalMemoryQuota(IgniteConfiguration.DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA);

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
        try (Connection conn512 = createConnection("jdbc-config-query-mem-limit-512.xml");
             Connection conn4096 = createConnection("jdbc-config-query-mem-limit-4096.xml")) {
            Statement stmt0 = conn4096.createStatement();

            // Expect no exception here.
            stmt0.execute(QUERY_1024_TO_2048);

            GridTestUtils.assertThrows(log, () -> {
                Statement stmt = conn4096.createStatement();

                stmt.execute(QUERY_2048_TO_4096);
            }, IgniteException.class, "SQL query run out of memory: Query quota exceeded.");

            GridTestUtils.assertThrows(log, () -> {
                Statement stmt = conn512.createStatement();

                stmt.execute(QUERY_1024_TO_2048);
            }, IgniteException.class, "SQL query run out of memory: Query quota exceeded.");
        }
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

    /**
     * Initialize SQL connection.
     *
     * @param xmlCfg Xml file with config.
     * @throws SQLException If failed.
     */
    protected Connection createConnection(String xmlCfg) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:cfg://" +
            "modules/clients/src/test/config/" + xmlCfg);

        conn.setSchema("\"PUBLIC\"");

        return conn;
    }
}
