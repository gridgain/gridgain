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

import org.apache.ignite.internal.processors.query.IgniteSQLException;
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

        GridTestUtils.assertThrows(
            log,
            () -> grid(0).context().query().getIndexing().statsManager().collectStatistics(t100, t101, tWrong),
            IgniteSQLException.class,
            "Table doesn't exist [schema=PUBLIC, table=SMALL101wrong]"
        );

        updateStatistics(t100, t101);

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
}
