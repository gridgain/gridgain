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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
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
            grid(0).destroyCache(cache);

        super.afterTest();
    }

    /**
     */
    @Test
    public void testInsert() {
        sql("CREATE TABLE test3 (id int primary key, val1 varchar, val2 varchar)");

        checkDml("INSERT INTO test3 values (1,'Kenny', null)", 1);
        checkDml("INSERT INTO test3 set id=2, val1='Bobby'", 1);

        GridTestUtils.assertThrows(log, () -> sql("INSERT INTO test3 set id=1, val2='Kennedy'"),
            TransactionDuplicateKeyException.class, "Duplicate key during INSERT [key=1]");
    }

    /**
     */
    @Test
    public void testMerge() {
        sql("CREATE TABLE test3 (id int primary key, val1 varchar, val2 varchar)");

        checkDml("MERGE INTO test3 values (1,'Kenny', null)", 1);
        checkDml("MERGE INTO test3 values (1, 'Cartman', 'Rodrigez'), (2, 'Hardik','kaushik')", 2);
    }

    /**
     */
    @Test
    public void testKeyValWithUnspecifiedNames() {
        IgniteCache cache = grid(0).createCache(new CacheConfiguration<>()
            .setName("test")
            .setSqlSchema("TEST")
            .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, Integer.class))));

        checkDml("MERGE INTO test.integer values (1, 1)", 1);
        checkDml("MERGE INTO test.integer values (2, 3), (4, 5)", 2);
    }

    /**
     * @param sql DML query.
     * @param expUpdateCounts expected update count of the DML query.
     */
    private void checkDml(String sql, long expUpdateCounts) {
        List<List<?>> res = sql(sql);

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(expUpdateCounts, res.get(0).get(0));
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    protected List<List<?>> sql(String sql) {
        GridQueryProcessor qryProc = grid(0).context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        return qryProc.querySqlFields(qry, true).getAll();
    }
}
