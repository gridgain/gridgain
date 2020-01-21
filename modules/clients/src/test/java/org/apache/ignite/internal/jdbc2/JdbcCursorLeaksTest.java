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
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Test for cursors leak on JDBC v2.
 */
public class JdbcCursorLeaksTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void test() throws Exception {
        checkQuery(url(false, false), "SELECT 1");
        checkQuery(url(true, false), "SELECT 1");
        checkQuery(url(false, true), "SELECT 1");
        checkQuery(url(true, true), "SELECT 1");
        checkQuery(url(false, true), "SELECT 1; SELECT 2");
        checkQuery(url(true, true), "SELECT 1; SELECT 2");
        checkQuery(url(false, true), "SELECT 1; SELECT 2; SELECT 3");
        checkQuery(url(true, true), "SELECT 1; SELECT 2; SELECT 3");
    }

    /**
     * @param url Connection string;
     * @param sql Query string.
     * @throws Exception Orn error.
     */
    private void checkQuery(String url, String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            for (int i = 0; i < 10; i++) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(sql);
                }
            }

            checkThereAreNoRunningQueries(1000);
        }
    }

    /**
     * @param timeout Timeout to finish running queries.
     */
    private void checkThereAreNoRunningQueries(int timeout) {
        for (Ignite ign : G.allGrids())
            checkThereAreNoRunningQueries((IgniteEx)ign, timeout);
    }

    /**
     * @param ign Noe to check running queries.
     * @param timeout Timeout to finish running queries.
     */
    private void checkThereAreNoRunningQueries(IgniteEx ign, int timeout) {
        long t0 = U.currentTimeMillis();

        while (true) {
            List<List<?>> res = ign.context().query().querySqlFields(
                new SqlFieldsQuery("SELECT * FROM sys.LOCAL_SQL_RUNNING_QUERIES"), false).getAll();

            if (res.size() == 1)
                return;

            if (U.currentTimeMillis() - t0 > timeout)
                fail ("Timeout. There are unexpected running queries [node=" + ign.name() + ", queries= " + res + ']');
        }
    }

    /**
     * @param remote If true, the nodeId to remote query execution is specified at the connection string.
     * @param multipleStatement Allow multiple statements.
     * @return JDBCv2 URL connection string.
     */
    private String url(boolean remote, boolean multipleStatement) {
        StringBuilder params = new StringBuilder();

        params.append("multipleStatementsAllowed=").append(multipleStatement);
        params.append(":");

        if  (remote)
            params.append("nodeId=").append(grid(0).cluster().localNode().id());

        return CFG_URL_PREFIX + params + "@modules/clients/src/test/config/jdbc-config.xml";
    }
}
