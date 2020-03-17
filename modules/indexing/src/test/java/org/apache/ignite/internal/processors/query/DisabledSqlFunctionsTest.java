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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.SqlInitialConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link SqlInitialConfiguration#setDisabledSqlFunctions(String[])}.
 */
public class DisabledSqlFunctionsTest extends AbstractIndexingCommonTest {
    /** Local mode . */
    @Parameterized.Parameter
    private boolean local;

    /** Executes query on client node. */
    @Parameterized.Parameter
    private boolean client;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "local={0}, client={1}")
    public static Collection parameters() {
        Set<Object[]> paramsSet = new LinkedHashSet<>();

        for (int i = 0; i < 3; ++i) {
            Object[] params = new Object[2];

            params[0] = (i & 1) == 0;
            params[0] = (i & 2) == 0;

            paramsSet.add(params);
        }

        return paramsSet;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid("srv");
        startGrid("cli");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test local query execution.
     */
    @Test
    public void test() {

        checkSqlWithDisabledFunction("SELECT FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("SELECT FILE_READ('pom.xml')");
        checkSqlWithDisabledFunction("SELECT * FROM CSVREAD('test.dat')");
        checkSqlWithDisabledFunction("SELECT CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("SELECT MEMORY_FREE()");
        checkSqlWithDisabledFunction("SELECT MEMORY_USED()");
        checkSqlWithDisabledFunction("SELECT LOCK_MODE()");
        checkSqlWithDisabledFunction("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("SELECT SESSION_ID()");
        checkSqlWithDisabledFunction("SELECT CANCEL_SESSION(1)");
    }

    /**
     */
    private void checkSqlWithDisabledFunction(String sql) {
        GridTestUtils.assertThrows(log, () -> sql(sql).getAll(), IgniteSQLException.class, "qqq");
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object ... args) {
        IgniteEx ign = client ? grid("cli") :grid("srv");

        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLocal(local)
            .setArgs(args), false);
    }
}
