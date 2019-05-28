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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Query memory manager for local queries.
 */
public class JdbcQueryMemoryTrackerSelfTest extends QueryMemoryTrackerSelfTest {
    /** URL. */
    private String url = "jdbc:ignite:thin://127.0.0.1:10800..10802";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        if (conn != null) {
            conn.close();

            assert conn.isClosed();
        }
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> execQuery(String sql, boolean lazy) throws Exception {
        initConnection(lazy);

        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next())
                ;
        }

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    protected void checkQueryExpectOOM(String sql, boolean lazy) throws Exception {
        SQLException ex = (SQLException)GridTestUtils.assertThrows(log, () -> {
            execQuery(sql, lazy);

            return null;
        }, SQLException.class, "SQL query run out of memory");

        assertEquals(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY, ex.getErrorCode());
        assertEquals(IgniteQueryErrorCode.codeToSqlState(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY), ex.getSQLState());
    }

    /**
     * Initialize SQL connection.
     * @param lazy Lazy flag.
     *
     * @throws SQLException If failed.
     */
    private void initConnection(boolean lazy) throws SQLException {
        conn = DriverManager.getConnection(url + "?maxMemory=" + (maxMem)+"&lazy="+lazy);

        conn.setSchema("\"PUBLIC\"");

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }
}