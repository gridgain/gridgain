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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.junit.Test;

/**
 */
public class SqlInsertMergeImplicitColumnsTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cache : grid(0).cacheNames())
            grid(0).cache(cache).destroy();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsert() throws Exception {
        sql("CREATE TABLE test3 (id int primary key, val1 varchar, val2 varchar)");

        checkDml("INSERT INTO test3 values (1,'Kenny', null)", 1);
        checkDml("INSERT INTO test3 set id=2, val1='Bobby'", 1);

        GridTestUtils.assertThrows(log, () -> sql("INSERT INTO test3 set id=1, val2='Kennedy'"),
            TransactionDuplicateKeyException.class, "Duplicate key during INSERT [key=1]");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMerge() throws Exception {
        sql("CREATE TABLE test3 (id int primary key, val1 varchar, val2 varchar)");

        checkDml("INSERT INTO test3 values (1,'Kenny', null)", 1);
        checkDml("MERGE INTO test3 values (1, 'Cartman', 'Rodrigez'), (2, 'Hardik','kaushik')", 2);
    }

    /**
     * @param sql MERGE query.
     */
    private void checkDml(String sql, long expectedUpdateCounts) throws Exception {
        List<List<?>> resMrg = sql(sql);

        assertEquals(1, resMrg.size());
        assertEquals(1, resMrg.get(0).size());
        assertEquals(expectedUpdateCounts, resMrg.get(0).get(0));
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    protected List<List<?>> sql(String sql) throws Exception {
        GridQueryProcessor qryProc = grid(0).context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        return qryProc.querySqlFields(qry, true).getAll();
    }
}
