/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Integration tests for statistics collection.
 */
public class SqlStatisticsCommandTests extends StatisticsAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        sql("DROP TABLE IF EXISTS TEST");
        sql("DROP TABLE IF EXISTS TEST2");

        clearStat();

        testStatistics(SCHEMA, "TEST", true);
        testStatistics(SCHEMA, "TEST2", true);

        sql("CREATE TABLE TEST(id int primary key, name varchar)");
        sql("CREATE TABLE TEST2(id int primary key, name varchar)");

        sql("CREATE INDEX TEXT_NAME ON TEST(NAME);");
    }

    /**
     * 1) Analyze two test table one by one and test statistics collected
     * 2) Clear collected statistics
     * 3) Analyze it in single batch
     */
    @Test
    public void testAnalyze() throws IgniteCheckedException {
        sql("ANALYZE TEST");

        //U.sleep(1000);
        testStatistics(SCHEMA, "TEST", false);

        sql("ANALYZE PUBLIC.TEST2(name)");

        //U.sleep(1000);
        testStatistics(SCHEMA, "TEST2", false);

        clearStat();

        testStatistics(SCHEMA, "TEST", true);
        testStatistics(SCHEMA, "TEST2", true);

        sql("ANALYZE PUBLIC.TEST, test2");

        testStatistics(SCHEMA, "TEST", false);
        testStatistics(SCHEMA, "TEST2", false);
    }

    /**
     * 0) Ensure that there are no statistics before test (old id after schema implementation).
     * 1) Refresh statistics in batch.
     * 2) Test that there are statistics collected (new after schema implementation).
     * 3) Clear statistics and refresh one again.
     * 4) Test that now only one statistics exists.
     * @throws IgniteCheckedException
     */
    @Test
    public void testRefreshStatistics() throws IgniteCheckedException {
        // TODO after GG-32420 test schema
        testStatistics(SCHEMA, "TEST", true);
        testStatistics(SCHEMA, "TEST2", true);

        sql("ANALYZE PUBLIC.TEST, test2");

        testStatistics(SCHEMA, "TEST", false);
        testStatistics(SCHEMA, "TEST2", false);

        long testVer = sumStatisticsVersion(SCHEMA, "TEST");
        long test2Ver = sumStatisticsVersion(SCHEMA, "TEST2");

        sql("REFRESH STATISTICS PUBLIC.TEST, test2");

        testStatisticsVersion(SCHEMA, "TEST", testVer);
        testStatisticsVersion(SCHEMA, "TEST2", test2Ver);
    }

    /**
     * Test drop statistics command:
     * 1) Collect and test that statistics exists.
     * 2) Drop statistics by single column.
     * 3) Test statistics exists for the rest columns.
     * 4) Drop statistics by the rest column.
     * 5) Test statistics not exists
     */
    @Test
    public void testDropStatistics() throws IgniteInterruptedCheckedException {
        // TODO after GG-32420 test schema
        sql("ANALYZE PUBLIC.TEST, test2");

        testStatistics(SCHEMA, "TEST", false);
        testStatistics(SCHEMA, "TEST2", false);

        sql("DROP STATISTICS PUBLIC.TEST(name);");

        testStatistics(SCHEMA, "TEST", false);
        testStatistics(SCHEMA, "TEST2", false);

        U.sleep(TIMEOUT);

        sql("DROP STATISTICS PUBLIC.TEST;");

        testStatistics(SCHEMA, "TEST", true);
        testStatistics(SCHEMA, "TEST2", false);

        sql("ANALYZE PUBLIC.TEST, test2");

        testStatistics(SCHEMA, "TEST", false);
        testStatistics(SCHEMA, "TEST2", false);

        sql("DROP STATISTICS PUBLIC.TEST, test2");

        testStatistics(SCHEMA, "TEST", true);
        testStatistics(SCHEMA, "TEST2", true);
    }

    /**
     * Test ability to create table, index and statistics on table named STATISTICS:
     *
     * 1) Create table STATISTICS with column STATISTICS.
     * 2) Create index STATISTICS_STATISTICS on STATISTICS(STATISTICS).
     * 3) Analyze STATISTICS and check that statistics collected.
     * 4) Refresh STATISTICS.
     * 5) Drop statistics for table STATISTICS.
     */
    @Test
    public void statisticsLexemaTest() throws IgniteInterruptedCheckedException {
        sql("CREATE TABLE STATISTICS(id int primary key, statistics varchar)");
        sql("CREATE INDEX STATISTICS_STATISTICS ON STATISTICS(STATISTICS);");

        testStatistics(SCHEMA, "STATISTICS", true);

        sql("ANALYZE PUBLIC.STATISTICS(STATISTICS)");

        testStatistics(SCHEMA, "STATISTICS", false);

        sql("REFRESH STATISTICS PUBLIC.STATISTICS(STATISTICS)");

        testStatistics(SCHEMA, "STATISTICS", false);

        U.sleep(TIMEOUT);

        sql("DROP STATISTICS PUBLIC.STATISTICS(STATISTICS)");

        testStatistics(SCHEMA, "STATISTICS", true);
    }

    /**
     * Clear statistics on two test tables;
     *
     * @throws IgniteCheckedException In case of errors.
     */
    private void clearStat() throws IgniteCheckedException {
        IgniteStatisticsManager statMgr = grid(0).context().query().getIndexing().statsManager();
        statMgr.dropStatistics(new StatisticsTarget(SCHEMA, "TEST"),
            new StatisticsTarget(SCHEMA, "TEST2"));
    }

    /**
     * Test statistics existence on all nodes.
     *
     * @param schema Schema name.
     * @param obj Object name.
     */
    private void testStatistics(String schema, String obj, boolean isNull) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (Ignite node : G.allGrids()) {
                IgniteStatisticsManager nodeStatMgr = ((IgniteEx) node).context().query().getIndexing().statsManager();
                ObjectStatistics localStat = nodeStatMgr.getLocalStatistics(new StatisticsKey(schema, obj));

                if (!(isNull == (localStat == null)))
                    return false;
            }
            return true;
        }, TIMEOUT));
    }

    /**
     * Test statistics existence on all nodes.
     *
     * @param schema Schema name.
     * @param obj Object name.
     */
    private void testStatisticsVersion(String schema, String obj, long ver) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (Ignite node : G.allGrids()) {
                IgniteStatisticsManager nodeStatMgr = ((IgniteEx) node).context().query().getIndexing().statsManager();

                ObjectStatisticsImpl localStat = (ObjectStatisticsImpl)nodeStatMgr.getLocalStatistics(
                    new StatisticsKey(schema, obj)
                );

                long sumVer = localStat.columnsStatistics().values().stream()
                    .mapToLong(ColumnStatistics::version)
                    .sum();

                if (sumVer <= ver)
                    return false;
            }

            return true;
        }, TIMEOUT));
    }

    /**
     * Get average version of the column statistics for specified DB object.
     */
    long sumStatisticsVersion(String schema, String obj) {
        IgniteStatisticsManager nodeStatMgr = ((IgniteEx)F.first(G.allGrids())).context().query().getIndexing().statsManager();
        ObjectStatisticsImpl localStat = (ObjectStatisticsImpl)nodeStatMgr.getLocalStatistics(new StatisticsKey(schema, obj));

        if (localStat == null)
            return -1;

        return localStat.columnsStatistics().values().stream()
            .mapToLong(ColumnStatistics::version)
            .sum();
    }
}
