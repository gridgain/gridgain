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

import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/** Tests for default behavior of memory quota per connection. */
public class JdbcConnMemQuotasDisabledByDefaultTest extends GridCommonAbstractTest {
    /** */
    private static final long MEMORY_QUOTA = 42 * MB;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * Ensure that property {@code queryMaxMemory} in connection string
     * has no effect since this it should be disabled by default.
     */
    @Test
    public void testDisabledByDefault() throws SQLException {
        try (JdbcConnection conn = createConnection()) {
            Assert.assertEquals(0L, conn.getQueryMaxMemory());
        }
    }

    /**
     * Ensure that property {@code queryMaxMemory} in connection string
     * has properly set to the connection.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA, value = "true")
    public void testEnabledWithProperty() throws SQLException {
        try (JdbcConnection conn = createConnection()) {
            Assert.assertEquals(MEMORY_QUOTA, conn.getQueryMaxMemory());
        }
    }

    /** */
    private JdbcConnection createConnection() throws SQLException {
        return (JdbcConnection)DriverManager.getConnection("jdbc:ignite:cfg://" +
            "queryMaxMemory=" + MEMORY_QUOTA + "@modules/clients/src/test/config/jdbc-config.xml"
        );
    }
}