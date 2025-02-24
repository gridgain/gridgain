/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.gridgain.internal.h2.api.Trigger;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Tests for unsupported SQL statements on parser level.
 *
 * The test appears as result of require to remove part of original H2 parser and check that
 * the functionality fully disabled.
 */
public class SqlDisabledFeaturesSelfTest extends GridCommonAbstractTest {
    /**
     * Local.
     */
    private boolean local;

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        startGrid();
        startGrid(1);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test for unsupported SQL triggers.
     */
    @Test
    public void testUnsupportedTrigers() {
        execSql(
                "CREATE TABLE test ( " +
                        "id integer PRIMARY KEY, " +
                        "val varchar DEFAULT 'test_val')");

        assertCantBeParsed(
                "CREATE TRIGGER trig_0 BEFORE INSERT ON TEST FOR EACH ROW CALL \""
                        + TestTrigger.class.getName() + "\"",
                "Failed to parse query. Syntax error in SQL statement \"CREATE TRIGGER[*]");

        assertCantBeParsed(
                "DROP TRIGGER trig_0",
                "Failed to parse query. Syntax error in SQL statement \"DROP TRIGGER[*]");
    }

    /**
     * @param ignite Ignite.
     * @param sql    Sql.
     * @param args   Args.
     * @return Results.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setLocal(local);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return ((IgniteEx) ignite).context().query().querySqlFields(qry, false).getAll();
    }

    /**
     * @param sql  Sql.
     * @param args Args.
     * @return Query results.
     */
    private List<List<?>> execSql(String sql, Object... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param sql Sql.
     * @param msg Error message to check.
     */
    private void assertCantBeParsed(final String sql, String msg) {
        try {
            local = false;
            assertCantBeParsed0(sql, msg);

            local = true;
            assertCantBeParsed0(sql, msg);
        } finally {
            local = false;
        }
    }

    /**
     * @param sql Sql.
     * @param msg Error message match
     */
    private void assertCantBeParsed0(final String sql, String msg) {
        Throwable t = GridTestUtils.assertThrowsWithCause((Callable<Void>) () -> {
            execSql(sql);

            return null;
        }, IgniteSQLException.class);

        IgniteSQLException sqlE = X.cause(t, IgniteSQLException.class);

        assert sqlE != null;

        if (IgniteQueryErrorCode.PARSING != sqlE.statusCode() || !sqlE.getMessage().contains(msg)) {
            log.error("Unexpected exception", t);

            fail("Unexpected exception. See above");
        }
    }

    private static class TestTrigger implements Trigger {

        @Override public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) throws SQLException {

        }

        @Override public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {

        }

        @Override public void close() throws SQLException {

        }

        @Override public void remove() throws SQLException {

        }
    }
}
