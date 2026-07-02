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

package org.apache.ignite.cache.store.jdbc;

import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.utility.ThrowingFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

/**
 * MariaDB Docker images matrix shared by the MariaDB cache-store integration tests.
 */
final class MariaDBImages {

    private MariaDBImages() {/* No-op. */}

    /**
     * Parameter rows for a {@code @Parameterized.Parameters} method.
     *
     * @return One single-element {@code Object[]} per MariaDB image tag.
     * @see <a href="https://mariadb.org/about/#mariadb-server-ga-release-cadence-2025-2031">MariaDB Server GA release cadence 2025-2031</a>
     */
    public static Collection<Object[]> lts() {
        return Arrays.asList(new Object[][]{
                {"mariadb:12.3"},
                {"mariadb:11.8"},
                {"mariadb:11.4"},
                {"mariadb:10.11"},
                {"mariadb:10.6"}
        });
    }

    /**
     * Executes the given sql against the live MariaDB container and hands the result set to the consumer.
     */
    static void executeQuery(MariaDBContainer container, String sql, ThrowingConsumer<ResultSet> consumer) throws Exception {
        try (Connection conn = container.createConnection("");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)
        ) {
            consumer.accept(rs);
        }
    }

    static <T> T executeQuery(MariaDBContainer container, String sql, ThrowingFunction<ResultSet, T> function) throws Exception {
        try (Connection conn = container.createConnection("");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)
        ) {
            return function.apply(rs);
        }
    }

    /**
     * Executes the given sql against the live MariaDB container. SQL statement may be an INSERT, UPDATE, DELETE, or DDL.
     */
    static void executeUpdate(MariaDBContainer container, String... sql) throws Exception {
        try (Connection conn = container.createConnection("");
             Statement stmt = conn.createStatement();
        ) {
            for (String query : sql) {
                stmt.executeUpdate(query);
            }
        }
    }

    interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }
}
