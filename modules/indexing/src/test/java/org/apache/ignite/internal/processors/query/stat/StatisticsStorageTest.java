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
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for statistics storage.
 */
public abstract class StatisticsStorageTest extends StatisticsStorageAbstractTest {
    /** {@inheritDoc} */
    @Override public void beforeTest() throws IgniteCheckedException {
        grid(0).context().query().getIndexing().statsManager().gatherObjectStatistics(
            new StatisticsTarget("PUBLIC", "SMALL"));
    }

    /**
     * Test that statistics manager will return local statistics after cleaning of statistics repository.
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void clearAllTest() throws IgniteCheckedException {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();
        IgniteStatisticsRepositoryImpl statsRepo = (IgniteStatisticsRepositoryImpl)
            ((IgniteStatisticsManagerImpl) statsMgr).statisticsRepository();
        IgniteStatisticsStore statsStore = statsRepo.statisticsStore();

        statsStore.clearAllStatistics();

        ObjectStatistics locStat = statsMgr.getLocalStatistics("PUBLIC", "SMALL");

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
        statsMgr.gatherObjectStatistics(new StatisticsTarget("PUBLIC", "SMALL"));
        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics("PUBLIC", "SMALL");

        statsMgr.gatherObjectStatistics(new StatisticsTarget("PUBLIC", "SMALL"));

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics("PUBLIC", "SMALL");

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
        statsMgr.gatherObjectStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));
        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr.getLocalStatistics(SCHEMA, "SMALL");
        statsMgr.gatherObjectStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));
        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr.getLocalStatistics(SCHEMA, "SMALL");

        assertEquals(locStat, locStat2);
    }

    /**
     * Clear statistics twice and check that .
     */
    @Test
    public void testDoubleDeletion() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();

        statsMgr.clearObjectStatistics(new StatisticsTarget(SCHEMA, "SMALL"));

        GridTestUtils.waitForCondition(() ->
            null == (ObjectStatisticsImpl) statsMgr.getLocalStatistics(SCHEMA, "SMALL"), TIMEOUT);

        statsMgr.clearObjectStatistics(new StatisticsTarget(SCHEMA, "SMALL"));

        Thread.sleep(TIMEOUT);

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(SCHEMA, "SMALL");

        assertNull(locStat2);
    }

    /**
     * Clear statistics partially twice.
     */
    @Test
    public void testDoublePartialDeletion() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();

        statsMgr.clearObjectStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));

        GridTestUtils.waitForCondition(() -> null == ((ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(SCHEMA, "SMALL")).columnsStatistics().get("B"), TIMEOUT);

        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
            .getLocalStatistics(SCHEMA, "SMALL");

        assertNotNull(locStat);
        assertNotNull(locStat.columnsStatistics().get("A"));

        statsMgr.clearObjectStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));

        Thread.sleep(TIMEOUT);

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr.getLocalStatistics(SCHEMA, "SMALL");
        assertNotNull(locStat2);
        assertNotNull(locStat.columnsStatistics().get("A"));
        assertNull(locStat.columnsStatistics().get("B"));
    }
}
