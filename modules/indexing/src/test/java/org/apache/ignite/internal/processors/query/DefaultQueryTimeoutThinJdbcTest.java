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

package org.apache.ignite.internal.processors.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.jdbc.thin.JdbcThinStatement;
import org.apache.ignite.internal.util.typedef.X;

public class DefaultQueryTimeoutThinJdbcTest extends DefaultQueryTimeoutTest {
    @Override protected void executeQuery(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            conn.createStatement().executeQuery(sql);
        }
    }

    @Override protected void executeQuery(String sql, long timeout) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            JdbcThinStatement stmt = (JdbcThinStatement)conn.createStatement();

            stmt.timeout((int)timeout);

            stmt.executeQuery(sql);
        }
    }

    @Override protected void assertQueryCancelled(Callable<?> c) {
        try {
            c.call();
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, "The query was cancelled while executing", SQLException.class));
        }
    }
}
