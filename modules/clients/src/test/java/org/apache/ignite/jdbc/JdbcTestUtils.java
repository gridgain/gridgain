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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods for JDBC tests.
 */
public final class JdbcTestUtils {
    /** Jdbc thin url. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /**
     *
     */
    private JdbcTestUtils() {
        // No-op.
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    public static List<List<?>> sql(String sql, Object... params) throws Exception {
        return sql(URL, sql, Arrays.asList(params));
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    public static List<List<?>> sql(String url, String sql, List<Object> params) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); ++i)
                    pstmt.setObject(i + 1, params.get(i));

                pstmt.execute();

                return getAll(pstmt);
            }
        }
    }

    /**
     * @param stmt Statement.
     * @return Results.
     * @throws SQLException On error.
     */
    public static List<List<?>> getAll(Statement stmt) throws SQLException {
        int updCnt = stmt.getUpdateCount();

        if (updCnt == -1) {
            ResultSet rs = stmt.getResultSet();

            int cols = rs.getMetaData().getColumnCount();

            List<List<?>> res = new ArrayList<>();

            while (rs.next()) {
                List<Object> row = new ArrayList<>();

                for (int i = 0; i < cols; ++i)
                    row.add(rs.getObject(i + 1));

                res.add(row);
            }

            return res;
        }
        else
            return Collections.singletonList(Collections.singletonList((long)updCnt));
    }
}
