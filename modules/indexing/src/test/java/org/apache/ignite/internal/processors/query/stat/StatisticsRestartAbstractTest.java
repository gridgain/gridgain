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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Test about statistics reload after restart.
 */
public class StatisticsRestartAbstractTest extends StatisticsAbstractTest {
    /** Target for test table SMALL. */
    protected StatisticsTarget SMALL_TARGET = new StatisticsTarget(SCHEMA, "SMALL");

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        startGridsMultiThreaded(nodes());

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        createSmallTable(null);
    }

    /**
     * Create SQL table with the given index.
     *
     * @param idx Table idx, if {@code null} - name "SMALL" without index will be used.
     * @return Target to created table.
     */
    protected StatisticsTarget createSmallTable(Integer idx) {
        String strIdx = (idx == null) ? "" : String.valueOf(idx);

        runSql("DROP TABLE IF EXISTS small" + strIdx);

        runSql(String.format("CREATE TABLE small%s (a INT PRIMARY KEY, b INT, c INT) with \"BACKUPS=1\"", strIdx));

        runSql(String.format("CREATE INDEX small%s_b ON small%s(b)", strIdx, strIdx));

        runSql(String.format("CREATE INDEX small%s_c ON small%s(c)", strIdx, strIdx));

        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < SMALL_SIZE; i++)
            runSql(String.format("INSERT INTO small%s(a, b, c) VALUES(%d, %d, %d)", strIdx, i, i, i % 10));

        return new StatisticsTarget(SCHEMA, "SMALL" + strIdx);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws IgniteCheckedException {
        grid(0).context().query().getIndexing().statsManager().gatherObjectStatistics(SMALL_TARGET);
    }

    /**
     * @return Number of nodes to start.
     */
    public int nodes() {
        return 1;
    }
}
