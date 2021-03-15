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

import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper.buildDefaultConfigurations;

/**
 * Test for statistics obsolescence.
 */
public class StatisticsObsolescenceTest extends StatisticsAbstractTest {
    /**
     * Test statistics refreshing after significant changes of base table:
     * 1) Create and populate small table
     * 2) Analyze it and get local statistics
     * 3) Insert same number of rows into small table
     * 4) Check that statistics refreshed and its values changed.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testObsolescence() throws Exception {
        startGridsMultiThreaded(1);

        createSmallTable(null);

        statisticsMgr(0).collectStatistics(buildDefaultConfigurations(SMALL_TARGET));

        assertTrue(GridTestUtils.waitForCondition(() -> statisticsMgr(0).getLocalStatistics(SMALL_KEY) != null, TIMEOUT));

        ObjectStatisticsImpl stat1 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(stat1);

        for (int i = SMALL_SIZE; i < 2 * SMALL_SIZE; i++)
            sql(String.format("INSERT INTO small(a, b, c) VALUES(%d, %d, %d)", i, i, i % 10));

        statisticsMgr(0).processObsolescence();

        assertTrue(GridTestUtils.waitForCondition(() -> {
            ObjectStatisticsImpl stat2 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            return stat2.rowCount() > stat1.rowCount();
        }, 7000));
    }
}
