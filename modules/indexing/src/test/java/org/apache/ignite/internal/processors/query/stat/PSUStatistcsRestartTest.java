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

import org.apache.ignite.cluster.ClusterState;
import org.junit.Test;

/**
 * Test that statistics still used by planner after restart.
 */
public class PSUStatistcsRestartTest extends StatisticsRestartAbstractTest {
    /**
     * Use select with two conditions which shows statistics presence.
     * 1) Check that select use correct index with statistics
     * 2) Restart grid
     * 3) Check that select use correct index with statistics
     * 4) Clear statistics
     * 5) Check that select use correct index WITHOUT statistics (i.e. other one)
     * @throws Exception In case of errors.
     */
    @Test
    public void testRestart() throws Exception {
        String isNullSql = "select * from SMALL i1 where b < 2 and c = 1";
        String[][] noHints = new String[1][];
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SMALL_B"}, isNullSql, noHints);

        stopGrid(0);

        startGrid(0);

        grid(0).cluster().state(ClusterState.ACTIVE);
        Thread.sleep(100);

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SMALL_B"}, isNullSql, noHints);

        grid(0).context().query().getIndexing().statsManager().clearObjectStatistics("PUBLIC", "SMALL");

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SMALL_C"}, isNullSql, noHints);
    }
}
