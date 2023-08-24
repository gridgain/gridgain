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

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.gridgain.internal.h2.engine.Constants;
import org.gridgain.internal.h2.value.Value;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

/**
 * Tests for custom sql functions.
 */
public class IgniteSqlCustomFunctionTest extends AbstractIndexingCommonTest {

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    @Test
    public void testFunctionAliasSupportsSubquery() throws Exception {
        for (int i = 0; i < 2; i++) {
            IgniteH2Indexing indexing = (IgniteH2Indexing) grid(i).context().query().getIndexing();
            H2Utils.registerSqlFunctions(log, indexing.connections(), Constants.SCHEMA_MAIN, new Class[]{CountSubqueryFunction.class});
        }

        // does not throw 'Scalar subquery contains more than one row'
        List<List<?>> rows = sql("SELECT COUNT_SUBQUERY(SELECT * FROM (SELECT * FROM VALUES (1), (2), (3)) as t limit 2)")
                .getAll();

        assertEquals(1, rows.size());
        assertEquals(2, rows.get(0).get(0));
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
                .setArgs(args), false);
    }

    /**
     * Test custom sql function.
     */
    public static class CountSubqueryFunction {

        /**
         * Custom sql function that supports subquery vararg.
         */
        @QuerySqlFunction(alias = "COUNT_SUBQUERY", deterministic = true)
        public static int countSubquery(Value... values) throws SQLException {
            return values.length;
        }
    }
}
