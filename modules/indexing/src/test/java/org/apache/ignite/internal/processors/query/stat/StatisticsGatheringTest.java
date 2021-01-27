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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Test cluster wide gathering.
 */
public class StatisticsGatheringTest extends StatisticsRestartAbstractTest {
    /** {@inheritDoc} */
    @Override public int nodes() {
        return 3;
    }

    /**
     * Test statistics gathering:
     * 1) Get local statistics from each nodes and check: not null, equals columns length, not null numbers
     * 2) Get global statistics (with delay) and check its equality in all nodes.
     */
    @Test
    public void testGathering() throws Exception {
        ObjectStatisticsImpl stats[] = getStats("SMALL", StatisticsType.LOCAL);

        testCond(Objects::nonNull, stats);

        testCond(stat -> stat.columnsStatistics().size() == stats[0].columnsStatistics().size(), stats);

        testCond(this::checkStat, stats);

        GridTestUtils.waitForCondition(() -> {
            ObjectStatisticsImpl globalStats[] = getStats("SMALL", StatisticsType.GLOBAL);
            try {
                testCond(stat -> stat.equals(globalStats[0]), globalStats);
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }, 1000);
    }

    /**
     * Collect statistics for group of object at once and check it collected in each node.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testGroupGathering() throws Exception {
        StatisticsTarget t100 = createSmallTable(100);
        StatisticsTarget t101 = createSmallTable(101);
        StatisticsTarget tWrong = new StatisticsTarget(t101.schema(), t101.obj() + "wrong");

        StatisticsGatheringFuture<Map<StatisticsTarget, ObjectStatistics>>[] futures = grid(0).context().query()
            .getIndexing().statsManager().gatherObjectStatisticsAsync(t100, t101, tWrong);

        try {
            for (StatisticsGatheringFuture<Map<StatisticsTarget, ObjectStatistics>> future : futures)
                future.get();

            fail("Got result of wrong target.");
        } catch (IgniteCheckedException e) {
            assertTrue(e.getMessage().contains("1 target not found"));
        }

        ObjectStatisticsImpl[] stats100 = getStats(t100.obj(), StatisticsType.LOCAL);
        ObjectStatisticsImpl[] stats101 = getStats(t101.obj(), StatisticsType.LOCAL);

        testCond(this::checkStat, stats100);
        testCond(this::checkStat, stats101);
    }

    /**
     * Test statistics gathering on new node after join to cluster:
     *
     * 0) check statistics exists.
     * 1) clear statistics and check last node lack it.
     * 2) stop last node and gather statistics again.
     * 3) start last node and check last node lack statistics.
     * 4) gather statistics and check last node have it.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testNoStatNewNode() throws Exception {
        ObjectStatisticsImpl stat0local, stat0global, statNlocal;

        IgniteStatisticsManager statMgr = grid(0).context().query().getIndexing().statsManager();

        // 0) check statistics exists.
        assertNotNull(getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.LOCAL));
        assertNotNull(getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.GLOBAL));

        // 1) clear statistics and check last node lack it.
        statMgr.clearObjectStatistics(SMALL_TARGET);

        GridTestUtils.waitForCondition(() ->
            null == getStatsFromNode(0, "SMALL", StatisticsType.LOCAL), TIMEOUT);
        assertNull(getStatsFromNode(0, "SMALL", StatisticsType.GLOBAL));

        GridTestUtils.waitForCondition(() ->
            null == getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.LOCAL), TIMEOUT);
        assertNull(getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.GLOBAL));

        // 2) stop last node and gather statistics again.
        stopGrid(nodes() - 1);

        statMgr.gatherObjectStatistics(SMALL_TARGET);

        assertNotNull(getStatsFromNode(0, "SMALL", StatisticsType.LOCAL));
        assertNotNull(getStatsFromNode(0, "SMALL", StatisticsType.GLOBAL));

        // 3) start last node and check last node lack statistics.
        startGrid(nodes() - 1);

        assertNull(getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.LOCAL));
        assertNull(getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.GLOBAL));

        // 4) gather statistics and check last node have it.
        statMgr.gatherObjectStatistics(SMALL_TARGET);

        assertNotNull(stat0local = getStatsFromNode(0, "SMALL", StatisticsType.LOCAL));
        GridTestUtils.waitForCondition(() ->
            null != getStatsFromNode(0, "SMALL", StatisticsType.GLOBAL), TIMEOUT);

        assertNotNull(statNlocal = getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.LOCAL));
        GridTestUtils.waitForCondition(() ->
            null != getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.GLOBAL), TIMEOUT);

        assertTrue(stat0local.columnsStatistics().size() == statNlocal.columnsStatistics().size());
        assertEquals(stat0global = getStatsFromNode(0, "SMALL", StatisticsType.GLOBAL),
            getStatsFromNode(nodes() - 1, "SMALL", StatisticsType.GLOBAL));

        checkStat(stat0global);
        checkStat(statNlocal);
    }

    /**
     * Gather object statistics from a client node.
     *
     * @throws Exception In case of errors.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-18638")
    @Test
    public void testGatheringFromClientNode() throws Exception {
        IgniteEx client = startClientGrid(nodes());
        IgniteStatisticsManager statMgr = client.context().query().getIndexing().statsManager();

        assertNull(statMgr.getGlobalStatistics(SCHEMA, "SMALL"));

        statMgr.gatherObjectStatistics(SMALL_TARGET);

        assertNotNull(statMgr.getGlobalStatistics(SCHEMA, "SMALL"));
    }

    /**
     * Check if specified SMALL table stats is OK (have not null values).
     *
     * @param stat Object statistics to check.
     * @return {@code true} if statistics OK, otherwise - throws exception.
     */
    private boolean checkStat(ObjectStatisticsImpl stat) {
        assertTrue(stat.columnStatistics("A").total() > 0);
        assertTrue(stat.columnStatistics("B").cardinality() > 0);
        ColumnStatistics statC = stat.columnStatistics("C");
        assertTrue(statC.min() != null);
        assertTrue(statC.max() != null);
        assertTrue(statC.size() > 0);
        assertTrue(statC.nulls() == 0);
        assertTrue(statC.raw().length > 0);
        assertTrue(statC.total() == stat.rowCount());

        return true;
    }

    /**
     * Get local object statistics from all server nodes.
     *
     * @param tblName Object name to get statistics by.
     * @param type Desired statistics type.
     * @return Array of local statistics from nodes.
     */
    private ObjectStatisticsImpl[] getStats(String tblName, StatisticsType type) {
        ObjectStatisticsImpl res[] = new ObjectStatisticsImpl[nodes()];

        for (int i = 0; i < nodes(); i++)
            res[i] = getStatsFromNode(i, tblName, type);

        return res;
    }

    /**
     * Test specified predicate on each object statistics.
     *
     * @param cond Predicate to test.
     * @param stats Statistics to test on.
     */
    private void testCond(Function<ObjectStatisticsImpl, Boolean> cond, ObjectStatisticsImpl... stats) {
        assertFalse(F.isEmpty(stats));

        for (ObjectStatisticsImpl stat : stats)
            assertTrue(cond.apply(stat));
    }

    /**
     * Get local table statistics by specified node.
     *
     * @param nodeIdx Node index to get statistics from.
     * @param tblName Table name.
     * @param type Desired statistics type.
     * @return Local table statistics or {@code null} if there are no such statistics in specified node.
     */
    private ObjectStatisticsImpl getStatsFromNode(int nodeIdx, String tblName, StatisticsType type) {
        IgniteStatisticsManager statMgr = grid(nodeIdx).context().query().getIndexing().statsManager();
        try {
            switch (type) {
                case LOCAL:
                    return (ObjectStatisticsImpl) statMgr.getLocalStatistics(SCHEMA, tblName);
                case GLOBAL:
                    return (ObjectStatisticsImpl) statMgr.getGlobalStatistics(SCHEMA, tblName);
                default:
                    throw new UnsupportedOperationException();
            }
        }
        catch (IgniteCheckedException e) {
            fail(e.getMessage());
        }
        return null;
    }
}
