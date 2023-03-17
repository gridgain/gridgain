/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.stat;

import java.util.Map;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsType;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;
import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.apache.ignite.internal.util.IgniteUtils.checkpointBufferSize;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for IO statistic manager.
 */
public class IoStatisticsManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int RECORD_COUNT = 5000;

    /**
     * Value is chosen so that DataPage are evicted from data region, when adding an entry to the cache, we again
     * loaded (read) the DataPage from disk in order to add an entry to it.
     */
    private static final long MAX_REGION_SIZE = 25 * MB;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test existing zero statistics for not touched caches.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testEmptyIOStat() throws Exception {
        IgniteEx ign = prepareIgnite(true);

        IoStatisticsManager ioStatMgr = ign.context().ioStats();

        Map<IoStatisticsHolderKey, IoStatisticsHolder> stat = ioStatMgr.statistics(IoStatisticsType.CACHE_GROUP);

        checkEmptyStat(stat, DEFAULT_CACHE_NAME, null);

        stat = ioStatMgr.statistics(IoStatisticsType.HASH_INDEX);

        checkEmptyStat(stat, DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME);
    }

    /**
     * @param stat Statistics map.
     * @param name Name of statistics.
     * @param subName Subname of statistics.
     */
    private void checkEmptyStat(Map<IoStatisticsHolderKey, IoStatisticsHolder> stat, String name, String subName) {
        assertEquals(1, stat.size());

        IoStatisticsHolder cacheIoStatHolder = stat.get(new IoStatisticsHolderKey(name, subName));

        assertNotNull(cacheIoStatHolder);

        assertEquals(0, cacheIoStatHolder.logicalReads());

        assertEquals(0, cacheIoStatHolder.physicalReads());
    }

    /**
     * Test LOCAL_NODE statistics tracking for persistent cache.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testNotPersistentIOGlobalStat() throws Exception {
        ioStatGlobalPageTrackTest(false);
    }

    /**
     * Test LOCAL_NODE statistics tracking for not persistent cache.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testPersistentIOGlobalStat() throws Exception {
        ioStatGlobalPageTrackTest(true);
    }

    /**
     * Check LOCAL_NODE statistics tracking.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @throws Exception In case of failure.
     */
    private void ioStatGlobalPageTrackTest(boolean isPersistent) throws Exception {
        IoStatisticsManager ioStatMgr = prepareData(isPersistent);

        long physicalReadsCnt = ioStatMgr.physicalReads(IoStatisticsType.CACHE_GROUP, DEFAULT_CACHE_NAME, null);

        if (isPersistent)
            Assert.assertTrue(physicalReadsCnt > 0);
        else
            Assert.assertEquals(0, physicalReadsCnt);

        Long logicalReads = ioStatMgr.logicalReads(IoStatisticsType.HASH_INDEX, DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME);

        Assert.assertNotNull(logicalReads);

        Assert.assertEquals(RECORD_COUNT, logicalReads.longValue());

        // We expect pages to be rotated with disk.
        for (int i = 0; i < RECORD_COUNT; i++)
            ignite(0).cache(DEFAULT_CACHE_NAME).get("KEY-" + i);

        if (isPersistent) {
            // Check that physical reads grows, but not infinitely.
            assertTrue(physicalReadsCnt < ioStatMgr.physicalReads(IoStatisticsType.CACHE_GROUP, DEFAULT_CACHE_NAME, null));

            // There should be no more than 3 page rotations per read (data page, index level 1 page and index level 2 page).
            assertTrue(physicalReadsCnt + 3 * RECORD_COUNT > ioStatMgr.physicalReads(IoStatisticsType.CACHE_GROUP, DEFAULT_CACHE_NAME, null));
        }
        else
            assertEquals(0, (long)ioStatMgr.physicalReads(IoStatisticsType.CACHE_GROUP, DEFAULT_CACHE_NAME, null));

        assertTrue(logicalReads < (long)ioStatMgr.logicalReads(IoStatisticsType.HASH_INDEX, DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertTrue(logicalReads + 3 * RECORD_COUNT > ioStatMgr.logicalReads(IoStatisticsType.HASH_INDEX, DEFAULT_CACHE_NAME, HASH_PK_IDX_NAME));
    }

    /**
     * Prepare Ignite instance and fill cache.
     *
     * @param isPersistent {@code true} in case persistence should be enable.
     * @return IO statistic manager.
     * @throws Exception In case of failure.
     */
    private IoStatisticsManager prepareData(boolean isPersistent) throws Exception {
        IgniteEx ign = prepareIgnite(isPersistent);

        IoStatisticsManager ioStatMgr = ign.context().ioStats();

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        ioStatMgr.reset();

        for (int i = 0; i < RECORD_COUNT; i++)
            cache.put("KEY-" + i, "VALUE-" + i);

        return ioStatMgr;
    }

    /**
     * Create Ignite configuration.
     *
     * @param isPersist {@code true} in case persistence should be enable.
     * @return Ignite configuration.
     * @throws Exception In case of failure.
     */
    private IgniteConfiguration getConfiguration(boolean isPersist) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        if (isPersist) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setConcurrencyLevel(3)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(MAX_REGION_SIZE)
                        .setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /**
     * Start Ignite grid and create cache.
     *
     * @param isPersist {@code true} in case Ignate should use persistente storage.
     * @return Started Ignite instance.
     * @throws Exception In case of failure.
     */
    private IgniteEx prepareIgnite(boolean isPersist) throws Exception {
        IgniteEx ignite = startGrid(getConfiguration(isPersist));

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.createCache(new CacheConfiguration<String, String>(DEFAULT_CACHE_NAME));

        if (isPersist) {
            DataRegion dataRegion = ignite.context().cache().cache(DEFAULT_CACHE_NAME).context().dataRegion();

            PageMemoryImpl memory = ((PageMemoryImpl)dataRegion.pageMemory());

            assertThat(
                LongStream.of(getFieldValue(memory, "sizes")).sum() - checkpointBufferSize(dataRegion.config()),
                lessThanOrEqualTo(MAX_REGION_SIZE)
            );
        }

        return ignite;
    }
}
