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

import java.util.Optional;
import java.util.Set;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.junit.Test;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.spi.metric.jmx.MetricRegistryMBean.searchHistogram;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class JmxMetricExporterSpiTest extends AbstractExporterSpiTest {
    /** */
    private static IgniteEx ignite;

    /** */
    private static final String REGISTRY_NAME = "test_registry";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        JmxMetricExporterSpi jmxSpi = new JmxMetricExporterSpi();

        jmxSpi.setExportFilter(mgrp -> !mgrp.name().startsWith(FILTERED_PREFIX));

        cfg.setMetricExporterSpi(jmxSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testSysJmxMetrics() throws Exception {
        DynamicMBean sysMBean = metricRegistry(ignite.name(), null, SYS_METRICS);

        Set<String> res = stream(sysMBean.getMBeanInfo().getAttributes())
            .map(MBeanFeatureInfo::getName)
            .collect(toSet());

        assertTrue(res.contains(CPU_LOAD));
        assertTrue(res.contains(GC_CPU_LOAD));
        assertTrue(res.contains(metricName("memory", "heap", "init")));
        assertTrue(res.contains(metricName("memory", "heap", "used")));
        assertTrue(res.contains(metricName("memory", "nonheap", "committed")));
        assertTrue(res.contains(metricName("memory", "nonheap", "max")));

        Optional<MBeanAttributeInfo> cpuLoad = stream(sysMBean.getMBeanInfo().getAttributes())
            .filter(a -> a.getName().equals(CPU_LOAD))
            .findFirst();

        assertTrue(cpuLoad.isPresent());
        assertEquals(CPU_LOAD_DESCRIPTION, cpuLoad.get().getDescription());

        Optional<MBeanAttributeInfo> gcCpuLoad = stream(sysMBean.getMBeanInfo().getAttributes())
            .filter(a -> a.getName().equals(GC_CPU_LOAD))
            .findFirst();

        assertTrue(gcCpuLoad.isPresent());
        assertEquals(GC_CPU_LOAD_DESCRIPTION, gcCpuLoad.get().getDescription());
    }

    /** */
    @Test
    public void testDataRegionJmxMetrics() throws Exception {
        DynamicMBean dataRegionMBean = metricRegistry(ignite.name(), "io", "dataregion.default");

        Set<String> res = stream(dataRegionMBean.getMBeanInfo().getAttributes())
            .map(MBeanFeatureInfo::getName)
            .collect(toSet());

        assertTrue(res.containsAll(EXPECTED_ATTRIBUTES));

        for (String metricName : res)
            assertNotNull(metricName, dataRegionMBean.getAttribute(metricName));
    }

    /** */
    @Test
    public void testFilterAndExport() throws Exception {
        createAdditionalMetrics(ignite);

        assertThrowsWithCause(new RunnableX() {
            @Override public void runx() throws Exception {
                metricRegistry(ignite.name(), "filtered", "metric");
            }
        }, IgniteException.class);

        DynamicMBean bean1 = metricRegistry(ignite.name(), "other", "prefix");

        assertEquals(42L, bean1.getAttribute("test"));
        assertEquals(43L, bean1.getAttribute("test2"));

        DynamicMBean bean2 = metricRegistry(ignite.name(), "other", "prefix2");

        assertEquals(44L, bean2.getAttribute("test3"));
    }

    /** */
    @Test
    public void testHistogramSearchByName() throws Exception {
        MetricRegistry mreg = new MetricRegistry("test", "test", null);

        createTestHistogram(mreg);

        assertEquals(Long.valueOf(1), searchHistogram("histogram_0_50", mreg));
        assertEquals(Long.valueOf(2), searchHistogram("histogram_50_500", mreg));
        assertEquals(Long.valueOf(3), searchHistogram("histogram_500_inf", mreg));

        assertEquals(Long.valueOf(1), searchHistogram("histogram_with_underscore_0_50", mreg));
        assertEquals(Long.valueOf(2), searchHistogram("histogram_with_underscore_50_500", mreg));
        assertEquals(Long.valueOf(3), searchHistogram("histogram_with_underscore_500_inf", mreg));

        assertNull(searchHistogram("unknown", mreg));
        assertNull(searchHistogram("unknown_0", mreg));
        assertNull(searchHistogram("unknown_0_50", mreg));
        assertNull(searchHistogram("unknown_test", mreg));
        assertNull(searchHistogram("unknown_test_test", mreg));
        assertNull(searchHistogram("unknown_0_inf", mreg));

        assertNull(searchHistogram("histogram", mreg));
        assertNull(searchHistogram("histogram_0", mreg));
        assertNull(searchHistogram("histogram_0_100", mreg));
        assertNull(searchHistogram("histogram_0_inf", mreg));
        assertNull(searchHistogram("histogram_0_500", mreg));

        assertNull(searchHistogram("histogram_with_underscore", mreg));
        assertNull(searchHistogram("histogram_with_underscore_0", mreg));
        assertNull(searchHistogram("histogram_with_underscore_0_100", mreg));
        assertNull(searchHistogram("histogram_with_underscore_0_inf", mreg));
        assertNull(searchHistogram("histogram_with_underscore_0_500", mreg));
    }

    /** */
    @Test
    public void testHistogramExport() throws Exception {
        MetricRegistry mreg = ignite.context().metric().registry("histogramTest");

        createTestHistogram(mreg);

        DynamicMBean bean = metricRegistry(ignite.name(), null, "histogramTest");

        MBeanAttributeInfo[] attrs = bean.getMBeanInfo().getAttributes();

        assertEquals(6, attrs.length);

        assertEquals(1L, bean.getAttribute("histogram_0_50"));
        assertEquals(2L, bean.getAttribute("histogram_50_500"));
        assertEquals(3L, bean.getAttribute("histogram_500_inf"));

        assertEquals(1L, bean.getAttribute("histogram_with_underscore_0_50"));
        assertEquals(2L, bean.getAttribute("histogram_with_underscore_50_500"));
        assertEquals(3L, bean.getAttribute("histogram_with_underscore_500_inf"));
    }

    /** */
    @Test
    public void testJmxHistogramNamesExport() throws Exception {
        MetricRegistry reg = ignite.context().metric().registry(REGISTRY_NAME);

        String simpleName = "testhist";
        String nameWithUnderscore = "test_hist";

        reg.histogram(simpleName, new long[] {10, 100}, null);
        reg.histogram(nameWithUnderscore, new long[] {10, 100}, null);

        DynamicMBean mbn = metricRegistry(ignite.name(), null, REGISTRY_NAME);

        assertNotNull(mbn.getAttribute(simpleName + '_' + 0 + '_' + 10));
        assertEquals(0L, mbn.getAttribute(simpleName + '_' + 0 + '_' + 10));
        assertNotNull(mbn.getAttribute(simpleName + '_' + 10 + '_' + 100));
        assertEquals(0L, mbn.getAttribute(simpleName + '_' + 10 + '_' + 100));
        assertNotNull(mbn.getAttribute(nameWithUnderscore + '_' + 10 + '_' + 100));
        assertEquals(0L, mbn.getAttribute(nameWithUnderscore + '_' + 10 + '_' + 100));
        assertNotNull(mbn.getAttribute(simpleName + '_' + 100 + "_inf"));
        assertEquals(0L, mbn.getAttribute(simpleName + '_' + 100 + "_inf"));
    }

    /** */
    private void createTestHistogram(MetricRegistry mreg) {
        long[] bounds = new long[] {50, 500};

        HistogramMetric histogram = mreg.histogram("histogram", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);

        histogram = mreg.histogram("histogram_with_underscore", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);
    }
}
