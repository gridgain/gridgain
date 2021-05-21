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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Tests for DISTINCT & UNION operation for different types.
 */
public class DistinctResultTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
   }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cache : grid(0).cacheNames())
            grid(0).cache(cache).destroy();

        super.afterTest();
    }

    /**
     */
    @Test
    public void distinctDifferentNumericTypes() {
        sql("CREATE TABLE table1 (id INT PRIMARY KEY, vint INT, vstr VARCHAR)");
        sql("CREATE TABLE table2 (id INT PRIMARY KEY, vdec DECIMAL, vstr VARCHAR)");
        sql("CREATE TABLE table3 (id INT PRIMARY KEY, vlong LONG, vstr VARCHAR)");

        for (int i = 0; i < KEY_CNT; ++i) {
            sql("INSERT INTO table1 VALUES (?, ?, ?)", i, i, "val " + i);
            sql("INSERT INTO table2 VALUES (?, ?, ?)", i, new BigDecimal("" + i + ".00"), "val " + i);
            sql("INSERT INTO table3 VALUES (?, ?, ?)", i, (long)i, "val " + i);
        }

        List<List<?>> res = sql(
            "SELECT vint FROM table1 " +
            "UNION " +
            "SELECT vdec FROM table2 "
            + "UNION " +
            "SELECT vlong FROM table3"
        ).getAll();

        assertEquals("Invalid results:\n" + res, KEY_CNT, res.size());
    }

    /**
     */
    @Test
    public void distinctDifferentDateTimeTypes() {
        sql("CREATE TABLE table1 (id INT PRIMARY KEY, vdate DATE, vstr VARCHAR)");
        sql("CREATE TABLE table2 (id INT PRIMARY KEY, vtime TIME, vstr VARCHAR)");
        sql("CREATE TABLE table3 (id INT PRIMARY KEY, vts TIMESTAMP, vstr VARCHAR)");

        for (int i = 0; i < KEY_CNT; ++i) {
            sql("INSERT INTO table1 VALUES (?, ?, ?)", i, Date.valueOf("1970-01-01"), "val " + i);
            sql("INSERT INTO table2 VALUES (?, ?, ?)", i, Time.valueOf("00:00:00"), "val " + i);
            sql("INSERT INTO table3 VALUES (?, ?, ?)", i, Timestamp.valueOf("1970-01-01 00:00:00"), "val " + i);
        }

        List<List<?>> res = sql(
            "SELECT vdate FROM table1 " +
                "UNION " +
                "SELECT vtime FROM table2 "
                + "UNION " +
                "SELECT vts FROM table3"
        ).getAll();

        assertEquals("Invalid results:\n" + res, 1, res.size());
    }

    /**
     */
    @Test
    public void distinctDateVarchar() {
        sql("CREATE TABLE table1 (id INT PRIMARY KEY, vdate DATE)");
        sql("CREATE TABLE table2 (id INT PRIMARY KEY, vstr VARCHAR)");

        for (int i = 1; i < KEY_CNT; ++i) {
            sql("INSERT INTO table1 VALUES (?, ?)", i, Date.valueOf("2021-01-0" + i));
            sql("INSERT INTO table2 VALUES (?, ?)", i, "2021-01-0" + i);
        }

        List<List<?>> res = sql(
            "SELECT vdate FROM table1 " +
                "UNION " +
                "SELECT vstr FROM table2"
        ).getAll();

        assertEquals("Invalid results:\n" + res, KEY_CNT - 1, res.size());
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
            .setLazy(true)
            .setArgs(args), false);
    }
}
