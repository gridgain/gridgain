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

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Tests for SQL LIMIT OFFSET keywords.
 */
public class SqlLimitOffsetTest extends AbstractIndexingCommonTest {
    /** Rows count. */
    private static final int ROWS_CNT = 100;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    /** */
    @Test
    public void test() {
        sql("CREATE TABLE TEST (ID INTEGER PRIMARY KEY, VAL VARCHAR(100))");

        for (int i = 0; i < ROWS_CNT; ++i)
            sql("INSERT INTO TEST VALUES (?, ?)", i, "val" + i);

        List<List<?>> res0 = sql("SELECT ID FROM TEST ORDER BY ID LIMIT 10 OFFSET 10 ").getAll();
        List<List<?>> res1 = sql("SELECT ID FROM TEST ORDER BY ID OFFSET 10 LIMIT 10").getAll();
        List<List<?>> res2 = sql("SELECT ID FROM TEST ORDER BY ID LIMIT 10, 10 ").getAll();

        assertEquals(10, res1.size());

        for (int i = 0; i < 10; ++i )
            assertEquals(10 + i, res0.get(i).get(0));

        assertEqualsCollections(res0, res1);
        assertEqualsCollections(res0, res2);

        assertEquals(10, sql("SELECT ID FROM TEST ORDER BY ID LIMIT 10").getAll().size());
        assertEquals(ROWS_CNT - 10, sql("SELECT ID FROM TEST ORDER BY ID OFFSET 10").getAll().size());
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
}
