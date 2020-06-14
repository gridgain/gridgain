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

package org.apache.ignite.internal.metric;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.StreamSupport;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.LOGICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.PHYSICAL_READS_INNER;
import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.PHYSICAL_READS_LEAF;
import static org.apache.ignite.internal.metric.IoStatisticsHolderQuery.LOGICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsHolderQuery.PHYSICAL_READS;
import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;
import static org.apache.ignite.internal.metric.IoStatisticsType.HASH_INDEX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Test of local node IO statistics MX bean.
 */
@RunWith(Parameterized.class)
public class IoStatisticsMetricsLocalMXBeanImplSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_1_NAME = "cache1";

    /** */
    public static final String CACHE_2_NAME = "cache2";

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicity1;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicity2;

    /** */
    @Parameterized.Parameters(name = "Cache 1 = {0}, Cache 2 = {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CacheAtomicityMode.TRANSACTIONAL},
            new Object[] {CacheAtomicityMode.ATOMIC, CacheAtomicityMode.ATOMIC},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CacheAtomicityMode.ATOMIC},
            new Object[] {CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL}
        );
    }

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
            .setName(CACHE_1_NAME)
            .setDataRegionName("default")
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1));

        final CacheConfiguration cCfg2 = new CacheConfiguration()
            .setName(CACHE_2_NAME)
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
     */
    @Test
    public void testExistingCachesMetrics() {
        IgniteEx ignite = ignite(0);

        MetricRegistry pkCache1 = ignite.context().metric()
            .registry(metricName(HASH_INDEX.metricGroupName(), CACHE_1_NAME, HASH_PK_IDX_NAME));
        MetricRegistry pkCache2 = ignite.context().metric()
            .registry(metricName(HASH_INDEX.metricGroupName(), CACHE_2_NAME, HASH_PK_IDX_NAME));
        MetricRegistry cache1 = ignite.context().metric()
            .registry(metricName(CACHE_GROUP.metricGroupName(), CACHE_1_NAME));
        MetricRegistry cache2 = ignite.context().metric()
            .registry(metricName(CACHE_GROUP.metricGroupName(), CACHE_2_NAME));

        resetAllIoMetrics(ignite);

        // Check that in initial state all metrics are zero.
        assertEquals(0, pkCache1.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
        assertEquals(0, pkCache1.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
        assertEquals(0, pkCache1.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
        assertEquals(0, pkCache1.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
        assertEquals(0, cache1.<LongMetric>findMetric(LOGICAL_READS).value());
        assertEquals(0, cache1.<LongMetric>findMetric(PHYSICAL_READS).value());

        assertEquals(0, pkCache2.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
        assertEquals(0, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
        assertEquals(0, pkCache2.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
        assertEquals(0, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
        assertEquals(0, cache2.<LongMetric>findMetric(LOGICAL_READS).value());
        assertEquals(0, cache2.<LongMetric>findMetric(PHYSICAL_READS).value());

        int cnt = 500;

        populateCaches(0, cnt);

        resetAllIoMetrics(ignite);

        readCaches(0, cnt);

        // 1 of the reads got resolved from the inner page.
        // Each data page is touched twice - one during index traversal and second
        assertEquals(cnt - 1, pkCache1.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
        assertEquals(0, pkCache1.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
        assertEquals(cnt, pkCache1.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
        assertEquals(0, pkCache1.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
        assertEquals(2 * cnt, cache1.<LongMetric>findMetric(LOGICAL_READS).value());
        assertEquals(0, cache1.<LongMetric>findMetric(PHYSICAL_READS).value());

        // 1 of the reads got resolved from the inner page.
        assertEquals(cnt - 1, pkCache2.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
        assertEquals(0, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
        assertEquals(cnt, pkCache2.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
        assertEquals(0, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
        assertEquals(2 * cnt, cache2.<LongMetric>findMetric(LOGICAL_READS).value());
        assertEquals(0, cache2.<LongMetric>findMetric(PHYSICAL_READS).value());

        // Force physical reads
        ignite.cluster().active(false);
        ignite.cluster().active(true);

        resetAllIoMetrics(ignite);

        assertEquals(0, pkCache2.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
        assertEquals(0, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
        assertEquals(0, pkCache2.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
        assertEquals(0, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
        assertEquals(0, cache2.<LongMetric>findMetric(LOGICAL_READS).value());
        assertEquals(0, cache2.<LongMetric>findMetric(PHYSICAL_READS).value());

        readCaches(0, cnt);

        // We had a split, so now we have 2 leaf pages and 1 inner page read from disk.
        // For sure should overflow 2 data pages.
        assertEquals(cnt - 1, pkCache2.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
        assertEquals(2, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
        assertEquals(cnt, pkCache2.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
        assertEquals(1, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
        assertEquals(2 * cnt, cache2.<LongMetric>findMetric(LOGICAL_READS).value());

        long physReads = cache2.<LongMetric>findMetric(PHYSICAL_READS).value();
        assertTrue(physReads > 2);

        // Check that metrics are further increasing for logical reads and stay the same for physical reads.
        readCaches(0, cnt);

        assertEquals(2 * (cnt - 1), pkCache2.<LongMetric>findMetric(LOGICAL_READS_LEAF).value());
        assertEquals(2, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_LEAF).value());
        assertEquals(2 * cnt, pkCache2.<LongMetric>findMetric(LOGICAL_READS_INNER).value());
        assertEquals(1, pkCache2.<LongMetric>findMetric(PHYSICAL_READS_INNER).value());
        assertEquals(2 * 2 * cnt, cache2.<LongMetric>findMetric(LOGICAL_READS).value());
        assertEquals(physReads, cache2.<LongMetric>findMetric(PHYSICAL_READS).value());
    }

    /**
     * @param cnt Number of inserting elements.
     */
    private void populateCaches(int start, int cnt) {
        for (int i = start; i < cnt; i++) {
            ignite(0).cache(CACHE_1_NAME).put(i, i);

            ignite(0).cache(CACHE_2_NAME).put(i, i);
        }
    }

    /**
     * @param cnt Number of inserting elements.
     */
    private void readCaches(int start, int cnt) {
        for (int i = start; i < cnt; i++) {
            ignite(0).cache(CACHE_1_NAME).get(i);

            ignite(0).cache(CACHE_2_NAME).get(i);
        }
    }

    /**
     * Resets all io statistics.
     *
     * @param ignite Ignite.
     */
    public static void resetAllIoMetrics(IgniteEx ignite) {
        GridMetricManager mmgr = ignite.context().metric();

        StreamSupport.stream(mmgr.spliterator(), false)
            .map(MetricRegistry::name)
            .filter(name -> {
                for (IoStatisticsType type : IoStatisticsType.values()) {
                    if (name.startsWith(type.metricGroupName()))
                        return true;
                }

                return false;
            })
            .forEach(grpName -> resetMetric(ignite, grpName));

    }

    /**
     * Resets all metrics for a given prefix.
     *
     * @param grpName Group name to reset metrics.
     */
    public static void resetMetric(IgniteEx ignite, String grpName) {
        try {
            ObjectName mbeanName = U.makeMBeanName(ignite.name(), "Kernal",
                IgniteKernal.class.getSimpleName());

            MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

            if (!mbeanSrv.isRegistered(mbeanName))
                fail("MBean is not registered: " + mbeanName.getCanonicalName());

            IgniteMXBean bean = MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, IgniteMXBean.class, false);

            bean.resetMetrics(grpName);
        } catch (MalformedObjectNameException e) {
            throw new IgniteException(e);
        }
    }
}
