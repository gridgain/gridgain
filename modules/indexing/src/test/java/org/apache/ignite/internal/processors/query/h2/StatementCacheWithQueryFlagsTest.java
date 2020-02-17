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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 *
 */
public class StatementCacheWithQueryFlagsTest extends AbstractIndexingCommonTest {
    /** */
    private static final int SIZE = 1000;

    /** Enforce join order. */
    private boolean enforceJoinOrder;

    /** Enforce join order. */
    private boolean local;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(1);

        sql("CREATE TABLE TBL0 (ID INT PRIMARY KEY, JID INT, VAL INT)");
        sql("CREATE TABLE TBL1 (ID INT PRIMARY KEY, JID INT, VAL INT)");

        sql("CREATE INDEX IDX_TBL1_JID ON TBL1(JID)");

        sql("INSERT INTO TBL0 VALUES (1, 1, 1)");

        for (int i = 0; i < SIZE; ++i)
            sql("INSERT INTO TBL1 VALUES (?, ?, ?)", i, i, i);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        local = true;

        sql("EXPLAIN SELECT TBL1.ID FROM TBL1, TBL0 WHERE TBL0.JID=TBL1.JID").getAll();

        enforceJoinOrder = true;

        String plan = (String)sql("EXPLAIN SELECT TBL1.ID FROM TBL1, TBL0 WHERE TBL0.JID=TBL1.JID").getAll().get(0).get(0);

        assertTrue("Invalid join order: " + plan ,
            Pattern.compile("\"PUBLIC\".\"TBL1\"[\\n\\w\\W]+\"PUBLIC\".\"TBL0\"", Pattern.MULTILINE)
                .matcher(plan).find());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return sql(grid(0), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLocal(local)
            .setEnforceJoinOrder(enforceJoinOrder)
            .setArgs(args), false);
    }
}
