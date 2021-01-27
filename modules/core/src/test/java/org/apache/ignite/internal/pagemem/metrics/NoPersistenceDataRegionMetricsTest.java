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

package org.apache.ignite.internal.pagemem.metrics;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link PagesMetric} without persistence data region.
 */
public class NoPersistenceDataRegionMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final String REGION = "default-region";

    /** */
    private static final String CACHE = "test-cache";

    private PagesMetric metric;
    private DataRegionMetricsImpl oldMetrics;
    private final List<StatisticData> statistic = new ArrayList<>();

    /** */
    public static class StatisticData {

        long dataPages;

        long indexPages;

        long freePages;

        long freelistPages;

        long metaPages;

        public long getAll() {
            return dataPages + indexPages + freePages + freelistPages + metaPages;
        }

        public long getUsedAndInFreeListPages() {
            return dataPages + indexPages + freelistPages + metaPages;
        }

        public long getUsedPages() {
            return dataPages + indexPages + metaPages;
        }
    }

    /** */
    public static class Data {

        String data;

        @QuerySqlField(index = true)
        long indexedValue;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(persistenceRegion())
                    .setName(REGION)
                    .setMetricsEnabled(true)
                    .setMaxSize(1024 * 1024 * 1024)
                    .setInitialSize(1024 * 1024 * 1024)));
        return cfg;
    }

    protected boolean persistenceRegion() {
        return false;
    }

    @Test
    public void allocateDataPagesTest() throws Exception {
        IgniteEx ig = prepareGrid();

        applyStatistic();

        Assert.assertEquals(oldMetrics.getPhysicalMemoryPages(), statistic.get(0).getAll());

        IgniteCache<Integer, Data> cache = ig
            .getOrCreateCache(getCacheCfg());

        applyStatistic();
        Assert.assertEquals(oldMetrics.getPhysicalMemoryPages(), statistic.get(1).getUsedAndInFreeListPages());

        for (int i = 0; i < 100_000; i++) {
            Data data = new Data();
            data.data = "ASDkjsahdfjashdgfkhgHJFJHGHjhkjvhdgsaKJASHGDHSAGFDkhasgdSHJGSAD" + i;
            data.indexedValue = i % 5;
            cache.put(i, data);
        }

        applyStatistic();
        Assert.assertEquals(oldMetrics.getPhysicalMemoryPages(), statistic.get(2).getUsedPages());
        // some pages were allocated for PK()
        if (persistenceRegion()) {
            Assert.assertEquals(statistic.get(1).indexPages, statistic.get(2).indexPages);
        } else {
            Assert.assertTrue(statistic.get(1).indexPages < statistic.get(2).indexPages);
        }
        Assert.assertEquals(statistic.get(1).metaPages, statistic.get(2).metaPages);
        // check data pages
        Assert.assertTrue(statistic.get(1).dataPages < statistic.get(2).dataPages);
        // logically data pages should glow faster
        Assert.assertTrue(statistic.get(2).dataPages - statistic.get(1).dataPages
            > statistic.get(2).indexPages - statistic.get(1).indexPages);
    }

    private CacheConfiguration<Integer, Data> getCacheCfg() {
        return new CacheConfiguration<Integer, Data>()
            .setName(CACHE)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1))
            .setOnheapCacheEnabled(false);
    }

    @Test
    public void createDropCacheTest() throws Exception {
        IgniteEx ig = prepareGrid();

        applyStatistic();

        Assert.assertEquals(oldMetrics.getPhysicalMemoryPages(), statistic.get(0).getAll());

        IgniteCache<Integer, Data> cache = ig
            .getOrCreateCache(getCacheCfg());

        applyStatistic();

        ig.destroyCache(CACHE);

        applyStatistic();
        Assert.assertEquals(oldMetrics.getPhysicalMemoryPages(), statistic.get(2).getUsedAndInFreeListPages());
        // One INDEX page is not released?
        Assert.assertEquals(oldMetrics.getTotalUsedPages(), statistic.get(2).getUsedPages());
        Assert.assertEquals(statistic.get(0).dataPages, statistic.get(2).dataPages);
        Assert.assertEquals(statistic.get(0).metaPages, statistic.get(2).metaPages);
    }

    @Test
    public void dropCacheTest() throws Exception {
        allocateDataPagesTest();
        IgniteEx ig = grid(0);
        ig.destroyCache(CACHE);

        applyStatistic();
        Assert.assertEquals(oldMetrics.getTotalAllocatedPages(), statistic.get(3).getUsedAndInFreeListPages());
        Assert.assertEquals(oldMetrics.getTotalUsedPages(), statistic.get(3).getUsedPages());
        Assert.assertEquals(statistic.get(0).dataPages, statistic.get(3).dataPages);
        Assert.assertEquals(statistic.get(0).metaPages, statistic.get(3).metaPages);
    }

    private void applyStatistic() {
        StatisticData data = new StatisticData();
        data.dataPages = metric.physicalMemoryDataPagesSize();
        data.freelistPages = metric.physicalMemoryFreelistPagesSize();
        data.freePages = metric.physicalMemoryFreePagesSize();
        data.indexPages = metric.physicalMemoryIndexPagesSize();
        data.metaPages = metric.physicalMemoryMetaPagesSize();

        statistic.add(data);
    }

    @NotNull private IgniteEx prepareGrid() throws Exception {
        IgniteEx ig = startGrid(0);
        ig.cluster().active(true);
        GridCacheSharedContext sharedCtx = ig.context().cache().context();
        metric = sharedCtx.database().dataRegion(REGION).pageMemory().getPageMetric();
        oldMetrics = sharedCtx.database().dataRegion(REGION).memoryMetrics();
        return ig;
    }
}
