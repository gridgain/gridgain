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
import org.apache.ignite.cache.CacheMode;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test cases to ensure that proper index is chosen by H2 optimizer when value distribution statistics is collected.
 */
@RunWith(Parameterized.class)
public class ValueDistributionTableStatisticsUsageTest extends TableStatisticsAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
                { REPLICATED },
                // { PARTITIONED }, // TBD!!!
        });
    }

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(1); // TBD 1!!!!!

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        runSql("DROP TABLE IF EXISTS sized");

        runSql("CREATE TABLE sized (ID INT PRIMARY KEY, small VARCHAR, small_nulls VARCHAR," +
                " big VARCHAR, big_nulls VARCHAR) WITH \"TEMPLATE=" + cacheMode + "\"");

        runSql("CREATE INDEX sized_small ON sized(small)");
        runSql("CREATE INDEX sized_small_nulls ON sized(small_nulls)");
        runSql("CREATE INDEX sized_big ON sized(big)");
        runSql("CREATE INDEX sized_big_nulls ON sized(big_nulls)");

        String bigValue = "someBigLongValueWithTheSameTextAtFirst";
        String smallNulls, bigNulls;
        int valAdd;
        for (int i = 0; i < BIG_SIZE; i++) {
            if ((i & 1) == 0) {
                smallNulls = "null";
                bigNulls = null;
                valAdd = 0;
            } else {
                smallNulls = String.format("'small%d'", i);
                bigNulls = String.format("'%s%d'", bigValue, i);
                valAdd = 1;
            }
            String sql = String.format("INSERT INTO sized(id, small, small_nulls, big, big_nulls)" +
                    " VALUES(%d,'small%d', %s, '%s%d', %s)", i, i + valAdd, smallNulls, bigValue, i + valAdd, bigNulls);
            runSql(sql);
        }
        runSql("INSERT INTO sized(id, small, big) VALUES(" + BIG_SIZE + ", null, null)");
        updateStatistics("sized");
    }

    @Test
    public void selectWithManyCond() {
        String sql = "SELECT COUNT(*) FROM sized i1 USE INDEX (sized_small) where big like '3%' and small like '3%'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);
    }

    @Test
    public void selectNullCond() {
        String sql = "select count(*) from sized i1 where small is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);
    }

    @Test
    @Ignore
    public void selectNotNullCond() {
        String sql = "select count(*) from sized i1 where small is not null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);
    }

    @Test
    public void selectWithNullsDistributionCond() {
        String sql = "select * from sized i1 where small is null and small_nulls is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);
    }

    @Test
    public void selectWithValueNullsDistributionCond() {
        String sql = "select * from sized i1 where small = '1'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);

        String sql2 = "select * from sized i1 where small = '1' and small_nulls = '1'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL_NULLS"}, sql2, new String[1][]);
    }

    @Ignore
    @Test
    public void selectWithValueSizeCond() {
        String sql = "select * from sized i1 where big = '1' and small = '1'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);

        String sql2 = "select * from sized i1 where big_nulls = '1' and small_nulls = '1'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL_NULLS"}, sql2, new String[1][]);
    }
}