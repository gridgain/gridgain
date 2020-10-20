/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.Ignite;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Tests to check statistics collection by different types and distributions.
 */
public class StatisticCollectionTest extends TableStatisticsAbstractTest {

    private static final String TYPES[] = new String[]{"BOOLEAN", "INT", "TINYINT", "SMALLINT","BIGINT",
            "DECIMAL", "DOUBLE","REAL","TIME","DATE","TIMESTAMP","VARCHAR","CHAR","UUID","BINARY","GEOMETRY"};

    /** */
    private static final String START_DATE = "1970.01.01 12:00:00 UTC";

    /** */
    private static final long TIMESTART;

    /** */
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("HH:mm:ss");

    /** */
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    /** */
    private static final SimpleDateFormat TIMESTAMP_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        SimpleDateFormat SDF = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");
        SDF.setTimeZone(tz);
        Calendar cal = Calendar.getInstance();
        try {
            cal.setTime(SDF.parse(START_DATE));
        } catch (ParseException e) {
            // No-op.
        }
        TIMESTART = cal.getTimeInMillis();

        TIME_FORMATTER.setTimeZone(tz);
        DATE_FORMATTER.setTimeZone(tz);
        TIMESTAMP_FORMATTER.setTimeZone(tz);
    }

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(1);

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        runSql("DROP TABLE IF EXISTS dtypes");

        StringBuilder create = new StringBuilder("CREATE TABLE dtypes (ID INT PRIMARY KEY, col_index int, col_no_index int");
        for (String type : TYPES)
            create.append(", col_").append(type).append(" ").append(type);

        create.append(")");

        runSql(create.toString());

        runSql("CREATE INDEX dtypes_col_index ON dtypes(col_index)");
        for (String type : TYPES)
            runSql(String.format("CREATE INDEX dtypes_%s ON dtypes(col_%s)", type, type));

        for (int i = 1; i < SMALL_SIZE; i++)
            runSql(insert(i));

        for (int i = 0; i > -SMALL_SIZE / 2; i--)
            runSql(insertNulls(i));

        updateStatistics("dtypes");
    }

    private String insertNulls(long counter) {
        return String.format("INSERT INTO dtypes(id) values (%d)", counter);
    }

    private String insert(long counter) {
        StringBuilder insert = new StringBuilder("INSERT INTO dtypes(id, col_index, col_no_index");

        for (int i = 0; i < TYPES.length; i++)
            insert.append(", col_").append(TYPES[i]);

        insert.append(") VALUES (")
                .append(counter).append(", ")
                .append(counter).append(", ")
                .append(counter);

        for (int i = 0; i < TYPES.length; i++)
            insert.append(", ").append(getVal(TYPES[i], counter));

        insert.append(")");
        return insert.toString();
    }

    private String getVal(String type, long counter) {

        switch (type) {
            case "BOOLEAN":
                return ((counter & 1) == 0) ? "False" : "True";

            case "INT":
                return String.valueOf(counter % 2147483648L);

            case "TINYINT":
                return String.valueOf(counter % 128);

            case "SMALLINT":
                return String.valueOf(counter % 32768);

            case "BIGINT":
                return String.valueOf(counter);

            case "DECIMAL":
            case "DOUBLE":
            case "REAL":
                return String.valueOf((double)counter / 100);

            case "TIME":
                Calendar timeCalendar = Calendar.getInstance();
                timeCalendar.setTimeInMillis(TIMESTART);
                timeCalendar.add(Calendar.SECOND, (int)counter);
                return "'" + TIME_FORMATTER.format(timeCalendar.getTime()) + "'";

            case "DATE":
                Calendar dateCalendar = Calendar.getInstance();
                dateCalendar.setTimeInMillis(TIMESTART);
                dateCalendar.add(Calendar.DATE, (int)counter);
                return "'" + DATE_FORMATTER.format(dateCalendar.getTime()) + "'";

            case "TIMESTAMP":
                Calendar timestampCalendar = Calendar.getInstance();
                timestampCalendar.setTimeInMillis(TIMESTART);
                timestampCalendar.add(Calendar.SECOND, (int)counter);
                return "'" + TIMESTAMP_FORMATTER.format(timestampCalendar.getTime()) + "'";

            case "VARCHAR":
                return "'varchar" + counter + "'";

            case "CHAR":
                return "'" + (char)((int)'A' + counter % 32) + "'";

            case "UUID":
                return "'" + new UUID(0L, counter) + "'";

            case "BINARY":
                return String.valueOf(counter);

            case "GEOMETRY":
                return "null";

            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Run set of tests to select from specified column specified value.
     *
     * @param name type name
     * @param comparator >, <, =, is...
     * @param value value
     */
    private void doColumnTests(String name, String comparator, String value) {
        String[][] noHints = new String[1][];

        String[][] hints = new String[1][];
        hints[0] = new String[]{"DTYPES_" + name};

        String[][] wrongHints = new String[1][];
        wrongHints[0] = new String[]{"DTYPES_COL_INDEX"};

        String isNullSql = String.format("select * from dtypes i1 where col_%s is null", name);;

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, isNullSql, noHints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, isNullSql, hints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, isNullSql, wrongHints);

        // TODO implement is not null check when optimizer will able to properly handle such condition

        String sql = String.format("select * from dtypes i1 where col_%s %s %s", name, comparator, value);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, sql, noHints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, sql, hints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sql, wrongHints);

        String sqlMoreCond = sql + " and col_no_index = 213";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, sqlMoreCond, noHints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, sqlMoreCond, hints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sqlMoreCond, wrongHints);

        String descSql = sql + String.format(" order by col_%s desc", name);
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, descSql, noHints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, descSql, hints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, descSql, wrongHints);

        String descNoIndexSql = sql + " order by col_no_index desc";

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, descNoIndexSql, noHints);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DTYPES_" + name}, descNoIndexSql, hints);
    }

    /**
     * Test that optimizer will use boolean column index.
     */
    @Test
    public void compareSelectWithBooleanConditions() {
        doColumnTests("BOOLEAN", "=", "true");
    }

    /**
     * Test that optimizer will use int column index.
     */
    @Test
    public void compareSelectWithIntConditions() {
        doColumnTests("INT", "<", "-10");
        doColumnTests("INT", "<", "10");
        doColumnTests("INT", "<=", "2");
        doColumnTests("INT", "=", "1");
        doColumnTests("INT", ">=", "100");
        doColumnTests("INT", ">", "90");
        doColumnTests("INT", ">", "190");
    }

    /**
     * Test that optimizer will use tinyint column index.
     */
    @Test
    public void compareSelectWithTinyintConditions() {
        doColumnTests("TINYINT", "<", "-10");
        doColumnTests("TINYINT", "<", "10");
        doColumnTests("TINYINT", "<=", "10");
        doColumnTests("TINYINT", "=", "1");
        doColumnTests("TINYINT", ">=", "100");
        doColumnTests("TINYINT", ">", "99");
        doColumnTests("TINYINT", ">", "110");
    }

    /**
     * Test that optimizer will use small integer column index.
     */
    @Test
    public void compareSelectWithSmallintConditions() {
        doColumnTests("SMALLINT", "<", "-10");
        doColumnTests("SMALLINT", "<", "10");
        doColumnTests("SMALLINT", "<=", "10");
        doColumnTests("SMALLINT", "=", "1");
        doColumnTests("SMALLINT", ">=", "100");
        doColumnTests("SMALLINT", ">", "90");
        doColumnTests("SMALLINT", ">", "190");
    }

    /**
     * Test that optimizer will use big integer column index.
     */
    @Test
    public void compareSelectWithBigintConditions() {
        doColumnTests("BIGINT", "<", "-10");
        doColumnTests("BIGINT", "<", "10");
        doColumnTests("BIGINT", "<=", "10");
        doColumnTests("BIGINT", "=", "1");
        doColumnTests("BIGINT", ">=", "100");
        doColumnTests("BIGINT", ">", "99");
        doColumnTests("BIGINT", ">", "199");
    }

    /**
     * Test that optimizer will use decimal column index.
     */
    @Test
    public void compareSelectWithDecimalConditions() {
        doColumnTests("DECIMAL", "<", "-10");
        doColumnTests("DECIMAL", "<", "0.2");
        doColumnTests("DECIMAL", "<=", "0.1");
        doColumnTests("DECIMAL", "=", "1");
        doColumnTests("DECIMAL", ">=", "0.8");
        doColumnTests("DECIMAL", ">=", "100");
    }

    /**
     * Test that optimizer will use double column index.
     */
    @Test
    public void compareSelectWithDoubleConditions() {
        doColumnTests("DOUBLE", "<", "-10");
        doColumnTests("DOUBLE", "<", "0.2");
        doColumnTests("DOUBLE", "<=", "0.2");
        doColumnTests("DOUBLE", "=", "1");
        doColumnTests("DOUBLE", ">=", "0.8");
        doColumnTests("DOUBLE", ">", "0.9");
        doColumnTests("DOUBLE", ">=", "100");
    }

    /**
     * Test that optimizer will use real column index.
     */
    @Test
    public void compareSelectWithRealConditions() {
        doColumnTests("REAL", "<", "-10");
        doColumnTests("REAL", "<", "0.2");
        doColumnTests("REAL", "<=", "0.22");
        doColumnTests("REAL", "=", "1");
        doColumnTests("REAL", ">=", "0.8");
        doColumnTests("REAL", ">", "0.9");
        doColumnTests("REAL", ">=", "100");
    }

    /**
     * Test that optimizer will use time column index.
     */
    @Test
    public void compareSelectWithTimeConditions() {
        doColumnTests("TIME", "<", "'11:00:02'");
        doColumnTests("TIME", "<", "'12:00:02'");
        doColumnTests("TIME", "<=", "'12:00:02'");
        doColumnTests("TIME", "=", "'12:00:00'");
        doColumnTests("TIME", ">=", "'12:01:00'");
        doColumnTests("TIME", ">=", "'13:00:00'");
    }

    /**
     * Test that optimizer will use date column index.
     */
    @Test
    public void compareSelectWithDateConditions() {
        doColumnTests("DATE", "<", "'1969-01-03'");
        doColumnTests("DATE", "<", "'1970-01-03'");
        doColumnTests("DATE", "<=", "'1970-01-02'");
        doColumnTests("DATE", "=", "'1970-01-02'");
        doColumnTests("DATE", ">=", "'1970-03-03'");
        doColumnTests("DATE", ">=", "'1970-09-03'");
    }

    /**
     * Test that optimizer will use timestamp column index.
     */
    @Test
    public void compareSelectWithTimestampConditions() {
        doColumnTests("TIMESTAMP", "<", "'1970-01-01 11:00:09'");
        doColumnTests("TIMESTAMP", "<", "'1970-01-01 12:00:09'");
        doColumnTests("TIMESTAMP", "<=", "'1970-01-01 12:00:02'");
        doColumnTests("TIMESTAMP", "=", "'1970-01-01 12:00:59'");
        doColumnTests("TIMESTAMP", ">=", "'1970-01-01 12:01:23'");
        doColumnTests("TIMESTAMP", ">=", "'1970-01-01 12:08:23'");
    }

    /**
     * Test that optimizer will use varchar column index.
     */
    @Test
    public void compareSelectWithVarcharConditions() {
        doColumnTests("VARCHAR", "<", "'a'");
        doColumnTests("VARCHAR", "<", "'varchar2'");
        doColumnTests("VARCHAR", "<", "'varchar1'");
        doColumnTests("VARCHAR", "=", "'test+string'");
        doColumnTests("VARCHAR", ">=", "'varchar99'");
        doColumnTests("VARCHAR", ">", "'varchar99'");
        doColumnTests("VARCHAR", ">", "'varchar199'");
        doColumnTests("VARCHAR", ">", "'varchar1'");
    }

    /**
     * Test that optimizer will use char column index.
     */
    @Test
    public void compareSelectWithCharConditions() {
        doColumnTests("CHAR", "<", "'8'");
        doColumnTests("CHAR", "<", "'B'");
        doColumnTests("CHAR", "<=", "'C'");
        doColumnTests("CHAR", "=", "'D'");
        doColumnTests("CHAR", ">=", "'W'");
        doColumnTests("CHAR", ">", "'Z'");
        doColumnTests("CHAR", ">", "'z'");
    }

    /**
     * Test that optimizer will use uuid column index.
     */
    @Test
    public void compareSelectWithUuidConditions() {
        doColumnTests("UUID", "<=", "'00000000-0000-0000-0000-000000000003'");
        doColumnTests("UUID", "<=", "'00000000-0000-0000-0000-000000000001'");
        doColumnTests("UUID", "=", "'00000000-0000-0000-0000-000000000002'");
        doColumnTests("UUID", ">=", "'00000000-0000-0000-0000-000000000089'");
        doColumnTests("UUID", ">=", "'00000000-0000-0000-0000-000000000099'");
        doColumnTests("UUID", ">", "'c1707d92-f1ad-11ea-adc1-0242ac120002'");
    }

    /**
     * Test that optimizer will use binary column index.
     */
    @Test
    public void compareSelectWithBinaryConditions() {
        doColumnTests("BINARY", "<", "12");
        doColumnTests("BINARY", "<=", "13");
        doColumnTests("BINARY", "=", "13");
        doColumnTests("BINARY", ">=", "85");
        doColumnTests("BINARY", ">", "95");
        doColumnTests("BINARY", ">=", "1242452143213");
    }
}