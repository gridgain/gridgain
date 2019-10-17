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

package org.apache.ignite.internal.stat;

import java.lang.management.ManagementFactory;
import java.time.format.DateTimeFormatter;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IoStatisticsMetricsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;

/**
 * Test of local node IO statistics MX bean.
 */
public class IoStatisticsMetricsLocalMXBeanImplSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String MEMORY_CACHE_NAME = "inmemory";

    /** */
    public static final String PERSISTENT_CACHE_NAME = "persistent";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(256 * 1024L * 1024L).setName("default"));

        dsCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setPersistenceEnabled(true).setMaxSize(256 * 1024 * 1024).setName("persistent"));

        cfg.setDataStorageConfiguration(dsCfg);

        final CacheConfiguration cCfg1 = new CacheConfiguration()
            .setName(MEMORY_CACHE_NAME)
            .setDataRegionName("default")
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1));

        final CacheConfiguration cCfg2 = new CacheConfiguration()
            .setName(PERSISTENT_CACHE_NAME)
            .setDataRegionName("persistent")
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1));

        cfg.setCacheConfiguration(cCfg1, cCfg2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /**
     * Simple test JMX bean for indexes IO stats.
     *
     * @throws Exception In case of failure.
     */
    @Test
    public void testExistingCachesMetrics() throws Exception {
        IoStatisticsMetricsMXBean bean = ioStatMXBean();

        IoStatisticsManager ioStatMgr = ignite(0).context().ioStats();

        Assert.assertEquals(ioStatMgr.startTime().toEpochSecond(), bean.getStartTime());

        Assert.assertEquals(ioStatMgr.startTime().format(DateTimeFormatter.ISO_DATE_TIME), bean.getStartTimeLocal());

        bean.reset();

        Assert.assertEquals(ioStatMgr.startTime().toEpochSecond(), bean.getStartTime());

        Assert.assertEquals(ioStatMgr.startTime().format(DateTimeFormatter.ISO_DATE_TIME), bean.getStartTimeLocal());

        // Check that in initial state all metrics are zero.
        assertEquals(0, (long)bean.getIndexLeafLogicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexLeafPhysicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexInnerLogicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexInnerPhysicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexLogicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexPhysicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getCacheGroupLogicalReads(MEMORY_CACHE_NAME));
        assertEquals(0, (long)bean.getCacheGroupPhysicalReads(MEMORY_CACHE_NAME));

        assertEquals(0, (long)bean.getIndexLeafLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexLeafPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexInnerLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexInnerPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getCacheGroupLogicalReads(PERSISTENT_CACHE_NAME));
        assertEquals(0, (long)bean.getCacheGroupPhysicalReads(PERSISTENT_CACHE_NAME));

        int cnt = 500;

        populateCaches(0, cnt);

        bean.reset();

        readCaches(0, cnt);

        // 1 of the reads got resolved from the inner page.
        assertEquals(cnt - 1, (long)bean.getIndexLeafLogicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));

        assertEquals(0,   (long)bean.getIndexLeafPhysicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(cnt, (long)bean.getIndexInnerLogicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0,   (long)bean.getIndexInnerPhysicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(2 * cnt - 1, (long)bean.getIndexLogicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0,   (long)bean.getIndexPhysicalReads(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));
        // Each data page is touched twice - one during index traversal and second
        assertEquals(2 * cnt, (long)bean.getCacheGroupLogicalReads(MEMORY_CACHE_NAME));
        assertEquals(0,   (long)bean.getCacheGroupPhysicalReads(MEMORY_CACHE_NAME));

        // 1 of the reads got resolved from the inner page.
        assertEquals(cnt - 1, (long)bean.getIndexLeafLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0,   (long)bean.getIndexLeafPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(cnt, (long)bean.getIndexInnerLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0,   (long)bean.getIndexInnerPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(2 * cnt - 1, (long)bean.getIndexLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0,   (long)bean.getIndexPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(2 * cnt, (long)bean.getCacheGroupLogicalReads(PERSISTENT_CACHE_NAME));
        assertEquals(0,   (long)bean.getCacheGroupPhysicalReads(PERSISTENT_CACHE_NAME));

        Assert.assertEquals("HASH_INDEX inmemory.HASH_PK [LOGICAL_READS_LEAF=499, LOGICAL_READS_INNER=500, " +
            "PHYSICAL_READS_INNER=0, PHYSICAL_READS_LEAF=0]",
            bean.getIndexStatistics(MEMORY_CACHE_NAME, HASH_PK_IDX_NAME));

        Assert.assertEquals("HASH_INDEX persistent.HASH_PK [LOGICAL_READS_LEAF=499, LOGICAL_READS_INNER=500, " +
            "PHYSICAL_READS_INNER=0, PHYSICAL_READS_LEAF=0]",
            bean.getIndexStatistics(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));

        // Force physical reads
        ignite(0).cluster().active(false);
        ignite(0).cluster().active(true);

        bean.reset();

        assertEquals(0, (long)bean.getIndexLeafLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexLeafPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexInnerLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexInnerPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getIndexPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(0, (long)bean.getCacheGroupLogicalReads(PERSISTENT_CACHE_NAME));
        assertEquals(0, (long)bean.getCacheGroupPhysicalReads(PERSISTENT_CACHE_NAME));

        readCaches(0, cnt);

        assertEquals(cnt - 1, (long)bean.getIndexLeafLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        // We had a split, so now we have 2 leaf pages...
        assertEquals(2,   (long)bean.getIndexLeafPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(cnt, (long)bean.getIndexInnerLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        // And 1 inner page
        assertEquals(1,   (long)bean.getIndexInnerPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(2 * cnt - 1, (long)bean.getIndexLogicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(3,   (long)bean.getIndexPhysicalReads(PERSISTENT_CACHE_NAME, HASH_PK_IDX_NAME));
        assertEquals(2 * cnt, (long)bean.getCacheGroupLogicalReads(PERSISTENT_CACHE_NAME));
        // For sure should overflow 2 data pages.
        assertTrue(bean.getCacheGroupPhysicalReads(PERSISTENT_CACHE_NAME) > 2);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNonExistingCachesMetrics() throws Exception {
        IoStatisticsMetricsMXBean bean = ioStatMXBean();

        String nonExistingCache = "non-existing-cache";
        String nonExistingIdx = "non-existing-index";

        assertNull(bean.getIndexLeafLogicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexLeafPhysicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexInnerLogicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexInnerPhysicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexLogicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexPhysicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getCacheGroupLogicalReads(nonExistingCache));
        assertNull(bean.getCacheGroupPhysicalReads(nonExistingCache));

        assertNull(bean.getIndexLeafLogicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexLeafPhysicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexInnerLogicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexInnerPhysicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexLogicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexPhysicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
    }

    /**
     * @param cnt Number of inserting elements.
     */
    private void populateCaches(int start, int cnt) {
        for (int i = start; i < cnt; i++) {
            ignite(0).cache(MEMORY_CACHE_NAME).put(i, i);

            ignite(0).cache(PERSISTENT_CACHE_NAME).put(i, i);
        }
    }

    /**
     * @param cnt Number of inserting elements.
     */
    private void readCaches(int start, int cnt) {
        for (int i = start; i < cnt; i++) {
            ignite(0).cache(MEMORY_CACHE_NAME).get(i);

            ignite(0).cache(PERSISTENT_CACHE_NAME).get(i);
        }
    }

    /**
     * @return IO statistics MX bean for node with given index.
     * @throws Exception In case of failure.
     */
    private IoStatisticsMetricsMXBean ioStatMXBean() throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(0), "IOMetrics",
            IoStatisticsMetricsLocalMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, IoStatisticsMetricsMXBean.class, false);
    }
}
