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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base test for table statistics.
 */
public abstract class StatisticsAbstractTest extends GridCommonAbstractTest {
    /** Counter to avoid query caching. */
    private static final AtomicInteger queryRandomizer = new AtomicInteger(0);

    /** Big table size. */
    static final int BIG_SIZE = 1000;

    /** Medium table size. */
    static final int MED_SIZE = 500;

    /** Small table size. */
    static final int SMALL_SIZE = 100;

    static {
        assertTrue(SMALL_SIZE < MED_SIZE && MED_SIZE < BIG_SIZE);
    }

    /**
     * Compare different index used for the given query.
     *
     * @param grid Grid to run queries on.
     * @param optimal Array of optimal indexes.
     * @param sql Query with placeholders to hint indexes (i1, i2, ...).
     * @param indexes Arrays of indexes to put into placeholders.
     */
    protected void checkOptimalPlanChosenForDifferentIndexes(IgniteEx grid, String[] optimal, String sql, String[][] indexes) {
        int size = -1;
        for (String[] idxs : indexes) {
            if (size == -1)
                size = (idxs == null) ? 0 : idxs.length;

            assert idxs == null || idxs.length == size;
        }
        sql = replaceIndexHintPlaceholders(sql, indexes);

        int spaces = queryRandomizer.incrementAndGet();
        StringBuilder spaceBuilder = new StringBuilder(spaces);
        for (int i = 0; i < spaces; i++)
            spaceBuilder.append(' ');

        sql = sql.replaceFirst(" ", spaceBuilder.toString());

        String actual[] = runLocalExplainIdx(grid, sql);

        assertTrue(String.format("got %s, expected %s in query %s", Arrays.asList(actual), Arrays.asList(optimal), sql),
                Arrays.equals(actual, optimal));
    }

    /**
     * Compares different orders of joins for the given query.
     *
     * @param grid Grid to run queries on.
     * @param sql Query text with placeholder (t0, t1, ...) instead of table names.
     * @param tbls Table names.
     */
    protected void checkOptimalPlanChosenForDifferentJoinOrders(Ignite grid, String sql, String... tbls) {
        String directOrder = replaceTablePlaceholders(sql, tbls);

        if (log.isDebugEnabled())
            log.debug("Direct join order=" + directOrder);

        ensureOptimalPlanChosen(grid, directOrder);

        // Reverse tables order.
        List<String> dirOrdTbls = Arrays.asList(tbls);

        Collections.reverse(dirOrdTbls);

        String reversedOrder = replaceTablePlaceholders(sql, dirOrdTbls.toArray(new String[dirOrdTbls.size()]));

        if (log.isDebugEnabled())
            log.debug("Reversed join order=" + reversedOrder);

        ensureOptimalPlanChosen(grid, reversedOrder);
    }

    /**
     * Compares join orders by actually scanned rows. Join command is run twice:
     * with {@code enforceJoinOrder = true} and without. The latest allows join order optimization
     * based or table row count.
     *
     * Actual scan row count is obtained from the EXPLAIN ANALYZE command result.
     */
    private void ensureOptimalPlanChosen(Ignite grid, String sql, String... tbls) {
        int cntNoStats = runLocalExplainAnalyze(grid, true, sql);

        int cntStats = runLocalExplainAnalyze(grid, false, sql);

        String res = "Scanned rows count [noStats=" + cntNoStats + ", withStats=" + cntStats +
                ", diff=" + (cntNoStats - cntStats) + ']';

        if (log.isInfoEnabled())
            log.info(res);

        assertTrue(res, cntStats <= cntNoStats);
    }

    /**
     * Run specified query with EXPLAIN and return array of used indexes.
     *
     * @param grid Grid where query should be executed.
     * @param sql Query to explain.
     * @return Array of selected indexes.
     */
    protected String[] runLocalExplainIdx(Ignite grid, String sql) {
        List<List<?>> res = grid.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("EXPLAIN " + sql).setLocal(true))
                .getAll();
        String explainRes = (String)res.get(0).get(0);

        // Extract scan count from EXPLAIN ANALYZE with regex: return all numbers after "scanCount: ".
        Matcher m = Pattern.compile(".*\\/\\*.+?\\.(\\w+):.*\\R*.*\\R*.*\\R*.*\\R*\\*\\/.*").matcher(explainRes);
        List<String> result = new ArrayList<>();
        while (m.find())
            result.add(m.group(1).trim());

        return result.toArray(new String[result.size()]);
    }

    /**
     * Runs local join sql in EXPLAIN ANALYZE mode and extracts actual scanned row count from the result.
     *
     * @param enfJoinOrder Enforce join order flag.
     * @param sql Sql string.
     * @return Actual scanned rows count.
     */
    protected int runLocalExplainAnalyze(Ignite grid, boolean enfJoinOrder, String sql) {
        List<List<?>> res = grid.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQueryEx("EXPLAIN ANALYZE " + sql, null)
                        .setEnforceJoinOrder(enfJoinOrder)
                        .setLocal(true))
                .getAll();

        if (log.isDebugEnabled())
            log.debug("ExplainAnalyze enfJoinOrder=" + enfJoinOrder + ", res=" + res);

        return extractScanCountFromExplain(res);
    }

    /**
     * Extracts actual scanned rows count from EXPLAIN ANALYZE result.
     *
     * @param res EXPLAIN ANALYZE result.
     * @return Actual scanned rows count.
     */
    private int extractScanCountFromExplain(List<List<?>> res) {
        String explainRes = (String)res.get(0).get(0);

        // Extract scan count from EXPLAIN ANALYZE with regex: return all numbers after "scanCount: ".
        Matcher m = Pattern.compile("scanCount: (?=(\\d+))").matcher(explainRes);

        int scanCnt = 0;

        while (m.find())
            scanCnt += Integer.parseInt(m.group(1));

        return scanCnt;
    }

    /**
     * Run specified SQL on grid0.
     *
     * @param sql Statement to execute.
     */
    protected void runSql(String sql) {
        grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql)).getAll();
    }

    /**
     * Replaces index hint placeholder like "i1", "i2" with specified index names in the ISQL query.
     *
     * @param sql SQL to replace index placeholders.
     * @param idxs Index names array.
     * @return SQL with actual index names.
     */
    private static String replaceIndexHintPlaceholders(String sql, String[][] idxs) {
        assert !sql.contains("i0");

        int i = 0;

        for (String idx[] : idxs) {
            String idxPlaceHolder = "i" + (++i);

            assert sql.contains(idxPlaceHolder);

            if (!F.isEmpty(idx)) {
                String idxStr = "USE INDEX (" + String.join(",", idx) + ")";
                sql = sql.replaceAll(idxPlaceHolder, idxStr);
            }
            else
                sql = sql.replaceAll(idxPlaceHolder, "");

        }

        assert !sql.contains("i" + (i + 1));

        return sql;
    }

    /**
     * Replaces table placeholders like "t1", "t2" and others with actual table names in the SQL query.
     *
     * @param sql SQL query.
     * @param tbls Actual table names.
     * @return SQL with place holders replaced by the actual names.
     */
    private static String replaceTablePlaceholders(String sql, String... tbls) {
        assert !sql.contains("t0");

        int i = 0;

        for (String tbl : tbls) {
            String tblPlaceHolder = "t" + (++i);

            assert sql.contains(tblPlaceHolder);

            sql = sql.replace(tblPlaceHolder, tbl);
        }

        assert !sql.contains("t" + (i + 1));

        return sql;
    }

    /**
     * Update statistics on specified objects in PUBLIC schema.
     *
     * @param table Table where to update statistics, just to require al least one name.
     * @param tables Tables where to update statistics.
     */
    protected void updateStatistics(String table, String... tables) {
        List<String> allTbls = new ArrayList<>();
        allTbls.add(table);
        if (null != tables)
            allTbls.addAll(Arrays.asList(tables));

        try {
            for (String tbl : allTbls) {
                for (Ignite node : G.allGrids()) {
                    IgniteStatisticsManager statsMgr = ((IgniteEx)node).context().query().getIndexing().statsManager();
                    statsMgr.collectObjectStatistics("PUBLIC", tbl.toUpperCase());
                }
            }
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }
    }

    /**
     * Get object statistics.
     *
     * @param rowsCnt Rows count.
     * @return Object statistics.
     */
    protected ObjectStatisticsImpl getStatistics(long rowsCnt) {
        ColumnStatistics colStatistics = new ColumnStatistics(null, null, 100, 0, 100,
                0, new byte[0]);
        return new ObjectStatisticsImpl(rowsCnt, Collections.singletonMap("col1", colStatistics));
    }

    /**
     * Get object partition statistics.
     *
     * @param partId Partition id.
     * @return Object partition statistics with specified partition id.
     */
    protected ObjectPartitionStatisticsImpl getPartitionStatistics(int partId) {
        ColumnStatistics colStatistics = new ColumnStatistics(null, null, 100, 0,
                100, 0, new byte[0]);
        return new ObjectPartitionStatisticsImpl(partId, true, 0, 0,
                Collections.singletonMap("col1", colStatistics));
    }
}
