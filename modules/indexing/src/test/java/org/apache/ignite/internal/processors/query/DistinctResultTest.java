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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.internal.U;
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
    public void dbg() throws IgniteInterruptedCheckedException {
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

        U.sleep(3000);

        sql(
            "select t1.PART_ID, t2.ATTRIBUTE_VALUE from PIM_ATG_PART_AND_ASSET t1, PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES t2 where \n" +
                "t1.PART_ID IN ('121-1012', '121-1012E')\n" +
                "and t1.ASSET_ID=t2.ASSET_ID and t1.LANGUAGE='en' and t2.ATTRIBUTE_ID='ExternalAssetURL' and t2.LANGUAGE='en' and t1.type='PartNumberImage'").getAll();

        List<List<?>> res = sql(
            "explain select t1.PART_ID, t2.ATTRIBUTE_VALUE from PIM_ATG_PART_AND_ASSET t1, " +
                "PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES t2  " +
                "where \n" +
                "t1.PART_ID IN ('121-1012', '121-1012E', '121-1012LTM', '121-1013', '121-1013LTM', '121-101A', '121-101ALTM', '121-1022', '121-1022E', '121-1023', '121-1023LTM', '121-1043', '121-1043E', '122-1011', '122-1012', '122-1012E', '122-1012LTM', '122-1013', '122-1013E', '122-1022', '122-1022E', '122-1031', '122-1031E', '122-1032', '122-1032-KEY', '122-1032E', '122-1032LTM', '122-1033', '122-1033E', '122-1033LTM', '122-103E', '122-1052', '122-1061', '122-1062', '122-1062E', '122-1063', '122-1063E', '122-106E', '122-10AE', '122-10G3', '123-100A-INT', '123-100ALTM', '123-1011', '123-1012', '123-1012LTM', '123-1013', '123-1014', '123-1015', '123-1015E', '123-1015LTM', '123-1022', '123-1026', '123-1027', '123-102F', '123-1031', '123-1032', '123-1032E', '123-1032LTM', '123-1033', '123-1033-INT', '123-1033E', '123-1033LTM', '123-1034', '123-1034E', '123-1035', '123-1035E', '123-1035LTM', '123-103B', '123-103BLTM', '123-103E', '123-103ELTM', '123-1052', '123-1055', '123-1056', '123-105C', '123-105F', '123-1061', '123-1062', '123-1062E', '123-1063', '123-1063E', '123-1064', '123-1065', '123-1065E', '123-106B', '123-106BE', '123-106E', '123-106G', '124-1032', '124-1034', '125-1002', '125-1002E', '125-1005', '125-1005LTM', '125-100B', '125-1011', '125-1011E', '125-1012', '125-1012E', '125-1012LTM', '125-1014', '125-1015', '125-1015E', '125-1017', '125-101J', '125-101K', '125-1025', '125-102J', '125-1032', '125-1032E', '125-1032LTM', '125-1034', '125-1034E', '125-1034LTM', '125-1035', '125-1035E', '125-1035LTM', '125-1037', '125-1039', '125-103B', '125-103J', '125-103JE', '125-103JLTM', '125-103K', '125-103KE', '125-1055', '125-1055E', '125-1062', '125-1062E', '125-1064', '125-1065', '125-1065E', '125-106J', '125-106JE', '125-10B5', '125-10H5', '125-10HB', '125-10HBE', '126-1012', '126-1013', '127-100A', '127-100ALTM', '127-1012', '127-1012E', '127-1013', '127-1013E', '127-1013LTM', '127-1022', '127-1022E', '127-1023', '127-1023E', '127-1043', '127-1046', '127-1046E', '128-1012', '128-1022', '128-1034', '128-1052', '12A-1015', '222-1032LTM', 'G3838AA', 'G3900-63002', 'G3903-61004')\n" +
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
