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
import org.junit.Test;

/**
 * Tests for statistics storage.
 */
public abstract class StatisticsStorageTest extends StatisticsStorageAbstractTest {
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

        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");
        assertNotNull(locStat);

        statsMgr.collectObjectStatistics("PUBLIC", "SMALL");
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
        statsMgr.collectObjectStatistics("PUBLIC", "SMALL");
        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");
        statsMgr.collectObjectStatistics("PUBLIC", "SMALL");
        ObjectStatisticsImpl localStat2 = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");

        assertEquals(locStat, localStat2);
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
        statsMgr.collectObjectStatistics("PUBLIC", "SMALL", "B");
        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");
        statsMgr.collectObjectStatistics("PUBLIC", "SMALL", "B");
        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");

        assertEquals(locStat, locStat2);
    }

    /**
     * Clear statistics twice and check that .
     */
    @Test
    public void testDoubleDeletion() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();

        statsMgr.clearObjectStatistics("PUBLIC", "SMALL");
        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");
        assertNull(locStat);

        statsMgr.clearObjectStatistics("PUBLIC", "SMALL");
        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");
        assertNull(locStat2);

        statsMgr.collectObjectStatistics("PUBLIC", "SMALL");
    }

    /**
     * Clear statistics partially twice.
     */
    @Test
    public void testDoublePartialDeletion() throws Exception {
        IgniteStatisticsManager statsMgr = grid(0).context().query().getIndexing().statsManager();

        statsMgr.clearObjectStatistics("PUBLIC", "SMALL", "B");

        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");
        assertNotNull(locStat);
        assertNotNull(locStat.columnsStatistics().get("A"));
        assertNull(locStat.columnsStatistics().get("B"));

        statsMgr.clearObjectStatistics("PUBLIC", "SMALL", "B");

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statsMgr
                .getLocalStatistics("PUBLIC", "SMALL");
        assertNotNull(locStat2);
        assertNotNull(locStat.columnsStatistics().get("A"));
        assertNull(locStat.columnsStatistics().get("B"));

        statsMgr.collectObjectStatistics("PUBLIC", "SMALL");
    }
}
