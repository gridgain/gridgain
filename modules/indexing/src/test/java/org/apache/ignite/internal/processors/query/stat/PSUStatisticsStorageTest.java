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

import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Planner statistics usage test.
 */
public class PSUStatisticsStorageTest extends StatisticsStorageAbstractTest {
    /**
     * Check that statistics will be used correctly after partial removing and partial collection.
     *
     * 1) check that statistics used and optimal plan generated
     * 2) partially remove statistics for one extra column and check chat the rest statistics still can be used
     * 3) partially remove necessarily for the query statistics and check that query plan will be changed
     * 4) partially collect statistics for extra column and check that query plan still unable to get all statistics
     *      it wants
     * 5) partially collect statistics for the necessarily column and check that the query plan will restore to optimal
     */
    @Test
    public void testPartialDeletionCollection() throws Exception {
        String lessSql = "select * from SMALL i1 where b < 2 and c < 2";
        String[][] noHints = new String[1][];
        IgniteEx grid0 = grid(0);

        IgniteStatisticsManager statsMgr = grid0.context().query().getIndexing().statsManager();

        // 1) check that statistics used and optimal plan generated
        checkOptimalPlanChosenForDifferentIndexes(grid0, new String[]{"SMALL_B"}, lessSql, noHints);

        // 2) partially remove statistics for one extra column and check chat the rest statistics still can be used
        statsMgr.clearObjectStatistics("PUBLIC", "SMALL", "A");

        checkOptimalPlanChosenForDifferentIndexes(grid0, new String[]{"SMALL_B"}, lessSql, noHints);

        // 3) partially remove necessarily for the query statistics and check that query plan will be changed
        statsMgr.clearObjectStatistics("PUBLIC", "SMALL", "B");

        checkOptimalPlanChosenForDifferentIndexes(grid0, new String[]{"SMALL_C"}, lessSql, noHints);

        // 4) partially collect statistics for extra column and check that query plan still unable to get all statistics
        //      it wants

        statsMgr.collectObjectStatistics("PUBLIC", "SMALL", "A");

        checkOptimalPlanChosenForDifferentIndexes(grid0, new String[]{"SMALL_C"}, lessSql, noHints);

        // 5) partially collect statistics for the necessarily column and check that the query plan will restore to optimal

        statsMgr.collectObjectStatistics("PUBLIC", "SMALL", "B");

        checkOptimalPlanChosenForDifferentIndexes(grid0, new String[]{"SMALL_B"}, lessSql, noHints);
    }
}
