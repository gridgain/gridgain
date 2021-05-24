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


    @Test
    public void dbg() {
        msql(grid(0),

            "CREATE TABLE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (\n" +
            "\tASSET_ID VARCHAR,\n" +
            "\tATTRIBUTE_ID VARCHAR,\n" +
            "\tLANGUAGE VARCHAR,\n" +
            "\tATTRIBUTE_VALUE VARCHAR,\n" +
            "\tATTRIBUTES_STRING VARCHAR,\n" +
            "\tIS_MULTIPLE_VALUE VARCHAR,\n" +
            "\tCREATE_TIME TIMESTAMP,\n" +
            "\tMODIFY_TIME TIMESTAMP,\n" +
            "\tSTATUS VARCHAR,\n" +
            "\tCONSTRAINT PK_PUBLIC_PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES PRIMARY KEY (ASSET_ID,ATTRIBUTE_ID,LANGUAGE,ATTRIBUTE_VALUE)\n" +
            ") WITH \"template=replicated, CACHE_NAME=ATGC16\";\n" +

            "CREATE TABLE PIM_ATG_PART_AND_ASSET (\n" +
            "\tPART_ID VARCHAR,\n" +
            "\tASSET_ID VARCHAR,\n" +
            "\tTYPE VARCHAR,\n" +
            "\tLANGUAGE VARCHAR,\n" +
            "\tMETA_VALUES VARCHAR,\n" +
            "\tCREATE_TIME TIMESTAMP,\n" +
            "\tMODIFY_TIME TIMESTAMP,\n" +
            "\tSTATUS VARCHAR,\n" +
            "\tCONSTRAINT PK_PUBLIC_PIM_ATG_PART_AND_ASSET PRIMARY KEY (PART_ID,ASSET_ID,TYPE,LANGUAGE)\n" +
            ") WITH \"template=partitioned,CACHE_NAME=ATGC12,AFFINITY_KEY=PART_ID\";\n" +

            "CREATE INDEX IF NOT EXISTS PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES_IDX_ASSET_ID ON PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (ASSET_ID);\n" +
            "CREATE INDEX IF NOT EXISTS PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES_IDX_LANGUAGE ON PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (LANGUAGE);\n" +
            "CREATE INDEX IF NOT EXISTS PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES_IDX_ASSET_ID_LANGUAGE ON PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (ASSET_ID, LANGUAGE);\n" +
            "CREATE INDEX IF NOT EXISTS PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES_IDX_LANGUAGE_ATTRIBUTE_ID ON PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (LANGUAGE, ATTRIBUTE_ID);\n" +


            "CREATE INDEX IF NOT EXISTS PIM_ATG_PART_AND_ASSET_IDX_PART_ID ON PIM_ATG_PART_AND_ASSET (PART_ID);\n" +
            "CREATE INDEX IF NOT EXISTS PIM_ATG_PART_AND_ASSET_IDX_LANGUAGE ON PIM_ATG_PART_AND_ASSET (LANGUAGE);\n" +
            "CREATE INDEX IF NOT EXISTS PIM_ATG_PART_AND_ASSET_IDX_LANGUAGE_PART_ID ON PIM_ATG_PART_AND_ASSET (LANGUAGE, PART_ID);");

        msql(grid(0), " ANALYZE PIM_ATG_PART_AND_ASSET (LANGUAGE) WITH \"DISTINCT=9,NULLS=0,TOTAL=538925,SIZE=2\"\n" +
            " ANALYZE PIM_ATG_PART_AND_ASSET (META_VALUES) WITH \"DISTINCT=30,NULLS=519586,TOTAL=538925,SIZE=7\"\n" +
            " ANALYZE PIM_ATG_PART_AND_ASSET (STATUS) WITH \"DISTINCT=1,NULLS=0,TOTAL=538925,SIZE=1\"\n" +
            " ANALYZE PIM_ATG_PART_AND_ASSET (CREATE_TIME) WITH \"DISTINCT=422992,NULLS=0,TOTAL=538925,SIZE=6\"\n" +
            " ANALYZE PIM_ATG_PART_AND_ASSET (ASSET_ID) WITH \"DISTINCT=44824,NULLS=0,TOTAL=538925,SIZE=10\"\n" +
            " ANALYZE PIM_ATG_PART_AND_ASSET (TYPE) WITH \"DISTINCT=4,NULLS=0,TOTAL=538925,SIZE=22\"\n" +
            " ANALYZE PIM_ATG_PART_AND_ASSET (MODIFY_TIME) WITH \"DISTINCT=422992,NULLS=0,TOTAL=538925,SIZE=6\"\n" +
            " ANALYZE PIM_ATG_PART_AND_ASSET (PART_ID) WITH \"DISTINCT=16426,NULLS=0,TOTAL=538925,SIZE=8\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (LANGUAGE) WITH \"DISTINCT=9,NULLS=0,TOTAL=878553,SIZE=2\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (STATUS) WITH \"DISTINCT=1,NULLS=0,TOTAL=878553,SIZE=1\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (ATTRIBUTE_ID) WITH \"DISTINCT=10,NULLS=0,TOTAL=878553,SIZE=12\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (ATTRIBUTE_VALUE) WITH \"DISTINCT=128355,NULLS=0,TOTAL=878553,SIZE=29\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (CREATE_TIME) WITH \"DISTINCT=529615,NULLS=0,TOTAL=878553,SIZE=6\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (ATTRIBUTES_STRING) WITH \"DISTINCT=1,NULLS=0,TOTAL=878553,SIZE=0\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (ASSET_ID) WITH \"DISTINCT=45835,NULLS=0,TOTAL=878553,SIZE=10\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (MODIFY_TIME) WITH \"DISTINCT=529649,NULLS=0,TOTAL=878553,SIZE=6\"\n" +
            " ANALYZE PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES (IS_MULTIPLE_VALUE) WITH \"DISTINCT=1,NULLS=0,TOTAL=878553,SIZE=1\"\n");

        sql(
            "select t1.PART_ID, t2.ATTRIBUTE_VALUE from PIM_ATG_PART_AND_ASSET t1, PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES t2 where \n" +
                "t1.PART_ID IN ('121-1012', '121-1012E')\n" +
                "and t1.ASSET_ID=t2.ASSET_ID and t1.LANGUAGE='en' and t2.ATTRIBUTE_ID='ExternalAssetURL' and t2.LANGUAGE='en' and t1.type='PartNumberImage'").getAll();

        List<List<?>> res = sql(
            "explain select t1.PART_ID, t2.ATTRIBUTE_VALUE from PIM_ATG_PART_AND_ASSET t1, " +
                "PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES t2  " +
                "where \n" +
                "t1.PART_ID IN ('121-1012', '121-1012E')\n" +
                "and t1.ASSET_ID=t2.ASSET_ID and t1.LANGUAGE='en' and t2.ATTRIBUTE_ID='ExternalAssetURL' and t2.LANGUAGE='en' and t1.type='PartNumberImage'").getAll();

        System.out.println("+++ " + res);
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
            .setLocal(true)
            .setArgs(args), false);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private List<FieldsQueryCursor<List<?>>> msql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setArgs(args), false, false);
    }
}
