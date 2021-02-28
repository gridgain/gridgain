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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for statistics storage.
 */
public abstract class StatisticsStorageTest extends StatisticsStorageAbstractTest {
    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        updateStatistics(
            new StatisticsTarget("PUBLIC", "SMALL"));
    }

    /**
     * Test that statistics manager will return local statistics after cleaning of statistics repository.
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void clearAllTest() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();
        IgniteStatisticsRepository statsRepo = ((IgniteStatisticsManagerImpl) statsMgr).statisticsRepository();
        IgniteStatisticsStore statsStore = statsRepo.statisticsStore();

        statsStore.clearAllStatistics();

        ObjectStatistics locStat = statsMgr.getLocalStatistics(new StatisticsKey("PUBLIC", "SMALL"));

        assertNotNull(locStat);
    }

    /**
     * Collect statistics by whole table twice and compare collected statistics:
     * 1) collect statistics by table and get it.
     * 2) collect same statistics by table again and get it.
     * 3) compare obtained statistics.
     */
    @Test
    public void testRecollection() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();

        updateStatistics(new StatisticsTarget("PUBLIC", "SMALL"));

        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(new StatisticsKey("PUBLIC", "SMALL"));

        updateStatistics(new StatisticsTarget("PUBLIC", "SMALL"));

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(new StatisticsKey("PUBLIC", "SMALL"));

        // Reset version to compare statistic.
        for (ColumnStatistics c : locStat2.columnsStatistics().values())
            GridTestUtils.setFieldValue(c, "ver", 0);

        // Reset version to compare statistic.
        for (ColumnStatistics c : locStat.columnsStatistics().values())
            GridTestUtils.setFieldValue(c, "ver", 0);

        assertEquals(locStat, locStat2);
    }

    /**
     * Collect statistics partially twice and compare collected statistics:
     * 1) collect statistics by column and get it.
     * 2) collect same statistics by column again and get it.
     * 3) compare obtained statistics.
     *
     */
    @Test
    public void testPartialRecollection() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();

        updateStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));
        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL"));

        updateStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));
        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL"));

        // Reset version to compare statistic.
        for (ColumnStatistics c : locStat2.columnsStatistics().values())
            GridTestUtils.setFieldValue(c, "ver", 0);

        // Reset version to compare statistic.
        for (ColumnStatistics c : locStat.columnsStatistics().values())
            GridTestUtils.setFieldValue(c, "ver", 0);

        assertEquals(locStat, locStat2);
    }

    /**
     * 1) Check that statistics available and usage state is ON
     * 2) Switch usage state to NO_UPDATE and check that statistics still availabel
     * 3) Disable statistics usage (OFF) and check it became unavailable.
     * 4) Turn ON again and check statistics availability
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testDisableGet() throws Exception {
        IgniteStatisticsManager statsMgr0 = grid(0).context().query().getIndexing().statsManager();
        IgniteStatisticsManager statsMgr1 = grid(1).context().query().getIndexing().statsManager();

        assertNotNull(statsMgr0.getLocalStatistics(SCHEMA, "SMALL"));
        assertNotNull(statsMgr1.getLocalStatistics(SCHEMA, "SMALL"));

        statsMgr0.usageState(StatisticsUsageState.NO_UPDATE);

        assertNotNull(statsMgr0.getLocalStatistics(SCHEMA, "SMALL"));
        assertNotNull(statsMgr1.getLocalStatistics(SCHEMA, "SMALL"));

        statsMgr0.usageState(StatisticsUsageState.OFF);

        assertNull(statsMgr0.getLocalStatistics(SCHEMA, "SMALL"));
        assertNull(statsMgr1.getLocalStatistics(SCHEMA, "SMALL"));

        statsMgr0.usageState(StatisticsUsageState.ON);

        assertNotNull(statsMgr0.getLocalStatistics(SCHEMA, "SMALL"));
        assertNotNull(statsMgr1.getLocalStatistics(SCHEMA, "SMALL"));
    }

    /**
     * Clear statistics twice and check that .
     */
    @Test
    public void testDoubleDeletion() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();

        statsMgr.dropStatistics(new StatisticsTarget(SCHEMA, "SMALL"));

        assertTrue(GridTestUtils.waitForCondition(() ->
            null == statsMgr
                .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL")), TIMEOUT));

        GridTestUtils.assertThrows(
            log,
            () -> statsMgr.dropStatistics(new StatisticsTarget(SCHEMA, "SMALL")),
            IgniteSQLException.class,
            "Statistic doesn't exist for [schema=PUBLIC, obj=SMALL]"
        );

        Thread.sleep(TIMEOUT);

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL"));

        assertNull(locStat2);
    }

    /**
     * Clear statistics partially twice.
     */
    @Test
    public void testDoublePartialDeletion() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();

        statsMgr.dropStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));

        assertTrue(GridTestUtils.waitForCondition(() -> null == ((ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL"))).columnsStatistics().get("B"), TIMEOUT));

        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL"));

        assertNotNull(locStat);
        assertNotNull(locStat.columnsStatistics().get("A"));

        GridTestUtils.assertThrows(
            log,
            () -> statsMgr.dropStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B")),
            IgniteSQLException.class,
            "Statistic doesn't exist for [schema=PUBLIC, obj=SMALL, col=B]"
        );

        Thread.sleep(TIMEOUT);

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL"));

        assertNotNull(locStat2);
        assertNotNull(locStat.columnsStatistics().get("A"));
        assertNull(locStat.columnsStatistics().get("B"));
    }
}
