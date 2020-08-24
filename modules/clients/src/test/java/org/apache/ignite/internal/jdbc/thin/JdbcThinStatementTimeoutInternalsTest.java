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

package org.apache.ignite.internal.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class JdbcThinStatementTimeoutInternalsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Check internal state of statement: explicitTimeout flag means that the query timeout is set bu user explicitly.
     * Steps:
     * - create JDBC connection without timeout property;
     * - check the flag explicitTimeout. It must be false;
     * - set query timeout by JDBC API;
     * - check the flag explicitTimeout. It must be true;
     * - create JDBC connection with timeout property;
     * - check the flag explicitTimeout. It must be true;
     * - set query timeout by JDBC API;
     * - check the flag explicitTimeout. It must be true.
     */
    @Test
    public void testUrlQueryTimeoutPropertyIsSetInternally() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            JdbcThinStatement stmt = (JdbcThinStatement)conn.createStatement();

            assertEquals(0, stmt.requestTimeout());
            assertFalse(stmt.explicitTimeout);

            stmt.setQueryTimeout(1);

            assertEquals(1000, stmt.requestTimeout());
            assertTrue(stmt.explicitTimeout);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?queryTimeout=1")) {
            JdbcThinStatement stmt = (JdbcThinStatement)conn.createStatement();

            assertEquals(1000, stmt.requestTimeout());
            assertTrue(stmt.explicitTimeout);

            stmt.setQueryTimeout(2);

            assertEquals(2000, stmt.requestTimeout());
            assertTrue(stmt.explicitTimeout);
        }
    }
}
