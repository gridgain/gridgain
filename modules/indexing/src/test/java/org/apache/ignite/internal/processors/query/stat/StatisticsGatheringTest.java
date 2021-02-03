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

import java.util.Objects;
import java.util.function.Function;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

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
        StatisticsTarget t100 = createStatisticTarget(100);
        StatisticsTarget t101 = createStatisticTarget(101);
        StatisticsTarget tWrong = new StatisticsTarget(t101.schema(), t101.obj() + "wrong");

        grid(0).context().query().getIndexing().statsManager().updateStatistics(t100, t101, tWrong);
        awaitStatistics(TIMEOUT * 5);

        ObjectStatisticsImpl[] stats100 = getStats(t100.obj(), StatisticsType.LOCAL);
        ObjectStatisticsImpl[] stats101 = getStats(t101.obj(), StatisticsType.LOCAL);

        testCond(this::checkStat, stats100);
        testCond(this::checkStat, stats101);
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
        switch (type) {
            case LOCAL:
                return (ObjectStatisticsImpl) statMgr.getLocalStatistics(SCHEMA, tblName);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
