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

package org.apache.ignite.cache.store.jdbc.dialect;

/**
 * A dialect compatible with the MariaDB database. Inherits MERGE support
 * ({@code INSERT ... ON DUPLICATE KEY UPDATE}) and backtick identifier escaping from
 * {@link MySQLDialect}, and overrides the default fetch size to a JDBC-spec-compliant
 * non-negative value suitable for the MariaDB Connector/J driver.
 */
public class MariaDBDialect extends MySQLDialect {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a dialect with the recommended fetch size of {@code 1} for streaming result sets.
     * <p>
     * MariaDB Connector/J Before version 1.4.0, the only accepted value for fetch size
     * was {@code Statement.setFetchSize(Integer.MIN_VALUE)} (equivalent to {@code Statement.setFetchSize(1)}).
     * This value is still accepted for compatibility reasons, but rather use {@code Statement.setFetchSize(1)},
     * since according to JDBC the value must be >= 0.
     * <p>
     * See <a href="https://mariadb.com/docs/connectors/mariadb-connector-j/about-mariadb-connector-j#streaming-result-sets">MariaDB
     * Connector/J streaming result sets</a>.
     */
    public MariaDBDialect() {
        this(1);
    }

    /**
     * Creates a dialect with an explicit fetch size.
     *
     * @param fetchSize Fetch size passed to JDBC {@code Statement.setFetchSize} for load-cache range queries.
     */
    public MariaDBDialect(int fetchSize) {
        this.fetchSize = fetchSize;
    }
}
