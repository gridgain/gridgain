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
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.Ignite;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEX_COST_FUNCTION;

/**
 * Tests to check statistics collection by different types and distributions.
 * //TODO run all tests after rebalance.
 */
public class StatisticCollectionTest extends TableStatisticsAbstractTest {

    private static final String TYPES[] = new String[]{"BOOLEAN", "INT", "TINYINT", "SMALLINT","BIGINT",
            "DECIMAL", "DOUBLE","REAL","TIME","DATE","TIMESTAMP","VARCHAR","CHAR","UUID","BINARY","GEOMETRY"};

    /** */
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("HH:mm:ss");

    /** */
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    /** */
    private static final SimpleDateFormat TIMESTAMP_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(1); // TBD 1!!!!!

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        runSql("DROP TABLE IF EXISTS dtypes");

        StringBuilder create = new StringBuilder("CREATE TABLE dtypes (ID INT PRIMARY KEY, col_index int, col_no_index int");
        for (String type : TYPES) {
            create.append(", col_").append(type).append(" ").append(type);
        }
        create.append(")");

        runSql(create.toString());

        runSql("CREATE INDEX dtypes_col_index ON dtypes(col_index)");
        for (String type : TYPES) {
            runSql(String.format("CREATE INDEX dtypes_%s ON dtypes(col_%s)", type, type));
        }
        for (int i = 0; i < SMALL_SIZE; i++) {
            runSql(insert(i));
        }

        updateStatistics("dtypes");
    }

    private String insert(long counter) {
        StringBuilder insert = new StringBuilder("INSERT INTO dtypes(id, col_index, col_no_index");
        for (int i = 0; i < TYPES.length; i++) {
            insert.append(", col_").append(TYPES[i]);
        }
        insert.append(") VALUES (").append(counter).append(", ").append(counter).append(", ").append(counter);

        for (int i = 0; i < TYPES.length; i++) {
            insert.append(", ").append(getVal(TYPES[i], counter));
        }
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
                timeCalendar.setTimeInMillis(counter * 1000);
                return "'" + TIME_FORMATTER.format(timeCalendar.getTime()) + "'";

            case "DATE":
                Calendar dateCalendar = Calendar.getInstance();
                dateCalendar.setTimeInMillis(counter * 91200000);
                return "'" + DATE_FORMATTER.format(dateCalendar.getTime()) + "'";

            case "TIMESTAMP":
                Calendar timestampCalendar = Calendar.getInstance();
                timestampCalendar.setTimeInMillis(counter * 1000);
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

        String sql = String.format("select * from dtypes i1 where col_%s = %s", name, value);

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
        doColumnTests("INT", "=", "1");
        doColumnTests("INT", "<", "10");
        doColumnTests("INT", ">=", "100");
    }

    /**
     * Test that optimizer will use tinyint column index.
     */
    @Test
    public void compareSelectWithTinyintConditions() {
        doColumnTests("TINYINT", "=", "1");
        doColumnTests("TINYINT", "<", "10");
        doColumnTests("TINYINT", ">=", "100");
    }

    /**
     * Test that optimizer will use small integer column index.
     */
    @Test
    public void compareSelectWithSmallintConditions() {
        doColumnTests("SMALLINT", "=", "1");
        doColumnTests("SMALLINT", "<", "10");
        doColumnTests("SMALLINT", ">=", "100");
    }

    /**
     * Test that optimizer will use big integer column index.
     */
    @Test
    public void compareSelectWithBigintConditions() {
        doColumnTests("BIGINT", "=", "1");
        doColumnTests("BIGINT", "<", "10");
        doColumnTests("BIGINT", ">=", "100");
    }

    /**
     * Test that optimizer will use decimal column index.
     */
    @Test
    public void compareSelectWithDecimalConditions() {
        doColumnTests("DECIMAL", "=", "1");
        doColumnTests("DECIMAL", "<", "10");
        doColumnTests("DECIMAL", ">=", "100");
    }

    /**
     * Test that optimizer will use double column index.
     */
    @Test
    public void compareSelectWithDoubleConditions() {
        doColumnTests("DECIMAL", "=", "1");
        doColumnTests("DECIMAL", "<", "10");
        doColumnTests("DECIMAL", ">=", "100");
    }

    /**
     * Test that optimizer will use real column index.
     */
    @WithSystemProperty(key = IGNITE_INDEX_COST_FUNCTION, value = "COMPATIBLE_8_7_28")
    @Test
    public void compareSelectWithRealConditions() {
        doColumnTests("REAL", "=", "1");
        doColumnTests("REAL", "<", "10");
        doColumnTests("REAL", ">=", "100");
    }

    /**
     * Test that optimizer will use time column index.
     */
    @Test
    public void compareSelectWithTimeConditions() {
        doColumnTests("TIME", "=", "'12:00:00'");
        doColumnTests("TIME", "<", "'12:00:00'");
        doColumnTests("TIME", ">=", "'12:00:00'");
    }

    /**
     * Test that optimizer will use date column index.
     */
    @Test
    public void compareSelectWithDateConditions() {
        doColumnTests("DATE", "=", "'1970-02-03'");
        doColumnTests("DATE", "<", "'1970-02-03'");
        doColumnTests("DATE", ">=", "'1970-02-03'");
    }

    /**
     * Test that optimizer will use timestamp column index.
     */
    @Test
    public void compareSelectWithTimestampConditions() {
        doColumnTests("TIMESTAMP", "=", "'1972-02-02 11:59:59'");
        doColumnTests("TIMESTAMP", "<", "'1972-02-02 11:59:59'");
        doColumnTests("TIMESTAMP", ">=", "'1972-02-02 11:59:59'");
    }

    /**
     * Test that optimizer will use varchar column index.
     */
    @Test
    public void compareSelectWithVarcharConditions() {
        doColumnTests("VARCHAR", "=", "'test+string'");
    }

    /**
     * Test that optimizer will use char column index.
     */
    @Test
    public void compareSelectWithCharConditions() {
        doColumnTests("CHAR", "=", "'A'");
        doColumnTests("CHAR", "<", "'B'");
        doColumnTests("CHAR", ">=", "'Z'");
    }

    /**
     * Test that optimizer will use uuid column index.
     */
    @Test
    public void compareSelectWithUuidConditions() {
        doColumnTests("UUID", "=", "'c1707d92-f1ad-11ea-adc1-0242ac120002'");
        doColumnTests("UUID", "<", "'c1707d92-f1ad-11ea-adc1-0242ac120002'");
        doColumnTests("UUID", ">=", "'c1707d92-f1ad-11ea-adc1-0242ac120002'");
    }

    /**
     * Test that optimizer will use binary column index.
     */
    @Test
    public void compareSelectWithBinaryConditions() {
        doColumnTests("BINARY", "=", "1242452143213");
        doColumnTests("BINARY", "<", "1242452143213");
        doColumnTests("BINARY", ">=", "1242452143213");
    }
}