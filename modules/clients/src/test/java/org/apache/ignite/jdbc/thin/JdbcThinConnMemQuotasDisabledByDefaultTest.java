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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA;

/** Tests for default behavior of memory quota per connection. */
public class JdbcThinConnMemQuotasDisabledByDefaultTest extends GridCommonAbstractTest {
    /** */
    private static final int TABLE_SIZE = 1000;

    /** */
    private static final long CONNECTION_MEMORY_QUOTA = 16; // 16 bytes, it's on purpose

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        createAndPopulateTable();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** */
    private void createAndPopulateTable() {
        GridQueryProcessor qryProc = grid(0).context().query();

        qryProc.querySqlFields(
            new SqlFieldsQuery("CREATE TABLE tbl (id INT PRIMARY KEY, indexed INT, grp INT, grp_indexed INT, name VARCHAR)"),
            false
        ).getAll();

        for (int i = 0; i < TABLE_SIZE; ++i)
            qryProc.querySqlFields(
                new SqlFieldsQuery("INSERT INTO tbl VALUES (?, ?, ?, ?, ?)")
                    .setArgs(i, i, i % 100, i % 100, UUID.randomUUID().toString()),
                false
            ).getAll();
    }

    /**
     * Ensure that connection quota will be applied if system property
     * {@link IgniteSystemProperties#IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA}
     * is set to {@code true}.
     *
     * Since connection quota is pretty low, it's expected that query
     * fails with OOM error and proper exception will be thrown.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    @WithSystemProperty(key = IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA, value = "true")
    public void testQuotaShouldBeAppliedIfSysPropSet() throws SQLException {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            GridTestUtils.assertThrows(
                log,
                () -> stmt.executeQuery("SELECT * FROM tbl"),
                SQLException.class,
                "SQL query run out of memory: Query quota exceeded."
            );
        }
    }

    /**
     * Ensure that connection quota will be ignored if system property
     * {@link IgniteSystemProperties#IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA}
     * is not set.
     *
     * Since default quota is sufficiently high, it's expected that query
     * finish without any errors.
     */
    @Test
    public void testQuotaShouldBeIgnoredByDefault() throws SQLException {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM tbl");

            while (rs.next()) {
                // NO-OP
            }
        }
    }

    /**
     * Initialize SQL connection.
     *
     * @throws SQLException If failed.
     */
    protected Connection createConnection() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800..10802?" +
            "queryMaxMemory=" + CONNECTION_MEMORY_QUOTA);

        conn.setSchema("\"PUBLIC\"");

        return conn;
    }
}