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
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Planner statistics usage test.
 */
public class PSUStatisticsStorageTest extends StatisticsStorageAbstractTest {
    /** */
    private static final String SQL = "select * from SMALL i1 where b < 2 and c < 2";

    /** */
    private static final String[][] NO_HINTS = new String[1][];

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
        updateStatistics(SMALL_TARGET);

        IgniteEx ign = grid(0);

        IgniteStatisticsManager statsMgr = ign.context().query().getIndexing().statsManager();

        // 1) check that statistics used and optimal plan generated
        checkOptimalPlanChosenForDifferentIndexes(ign, new String[]{"SMALL_B"}, SQL, NO_HINTS);

        // 2) partially remove statistics for one extra column and check chat the rest statistics still can be used
        statsMgr.dropStatistics(new StatisticsTarget("PUBLIC", "SMALL", "A"));

        assertTrue(GridTestUtils.waitForCondition(
            () ->
                ((ObjectStatisticsImpl)statsMgr.getLocalStatistics(SMALL_KEY)).columnStatistics("A") == null,
                    TIMEOUT));

        checkOptimalPlanChosenForDifferentIndexes(ign, new String[]{"SMALL_B"}, SQL, NO_HINTS);

        // 3) partially remove necessarily for the query statistics and check that query plan will be changed
        statsMgr.dropStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));

        assertTrue(GridTestUtils.waitForCondition(
            () ->
                ((ObjectStatisticsImpl)statsMgr.getLocalStatistics(SMALL_KEY))
                    .columnStatistics("B") == null,
            TIMEOUT));

        assertTrue(GridTestUtils.waitForCondition(() -> {
            try {
                checkOptimalPlanChosenForDifferentIndexes(ign, new String[]{"SMALL_C"}, SQL, NO_HINTS);
                return true;
            }
            catch (AssertionError e) {
                return false;
            }
        }, TIMEOUT));

        // 4) partially collect statistics for extra column and check that query plan still unable to get all statistics
        // it wants

        updateStatistics(new StatisticsTarget("PUBLIC", "SMALL", "A"));

        checkOptimalPlanChosenForDifferentIndexes(ign, new String[]{"SMALL_C"}, SQL, NO_HINTS);

        // 5) partially collect statistics for the necessarily column
        // and check that the query plan will restore to optimal
        updateStatistics(new StatisticsTarget("PUBLIC", "SMALL", "B"));

        checkOptimalPlanChosenForDifferentIndexes(ign, new String[]{"SMALL_B"}, SQL, NO_HINTS);
    }
}
