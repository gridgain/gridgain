package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class CacheMetricsImplTest extends GridCommonAbstractTest {
    private CacheMetricsImpl metrics;
    private GridCacheContext<?, ?> cctx;
    private GridKernalContext kctx;
    private GridMetricManager metricManager;
    private MetricRegistry metricRegistry;

    @Before
    public void setUp() throws Exception {
        cctx = mock(GridCacheContext.class);
        kctx = mock(GridKernalContext.class);
        metricManager = mock(GridMetricManager.class);
        metricRegistry = mock(MetricRegistry.class);
        CacheStoreManager mockStoreManager = mock(CacheStoreManager.class);

        when(cctx.store()).thenReturn(mockStoreManager);
        when(cctx.name()).thenReturn("testCache");
        when(cctx.kernalContext()).thenReturn(kctx);
        when(kctx.metric()).thenReturn(metricManager);
        when(metricManager.registry(MetricUtils.metricName("cache", "testCache"))).thenReturn(metricRegistry);

        // Mock and stub the metrics
        mockAndStubMetrics();

        metrics = new CacheMetricsImpl(cctx, false);
    }

    private void mockAndStubMetrics() {
        String[] metricNames = new String[] {
                "testCache.CacheGets",
                "testCache.EntryProcessorPuts",
                "rebalanceStartTime",
                "rebalanceDuration",
                "rebalanceKeys",
                "rebalanceBytes"
        };

        for (String metricName : metricNames) {
            AtomicLongMetric mockMetric = mockAtomicLongMetric();
            when(metricRegistry.longMetric(eq(metricName), anyString())).thenReturn(mockMetric);
        }
    }

    private AtomicLongMetric mockAtomicLongMetric() {
        AtomicLongMetric atomicLongMetric = mock(AtomicLongMetric.class);
        when(atomicLongMetric.value()).thenReturn(0L);
        return atomicLongMetric;
    }

    @Test
    public void testCacheTouchMetrics() {
        metrics.onCacheTouch();
        assertEquals(1, metrics.getCacheTouches());
        metrics.onCacheTouchHit();
        assertEquals(1, metrics.getCacheTouchHits());
        metrics.onCacheTouchMiss();
        assertEquals(1, metrics.getCacheTouchMisses());
    }

    @Test
    public void testCacheTouchHitPercentage() {
        metrics.onCacheTouch();
        metrics.onCacheTouchHit();
        assertEquals(100.0, metrics.getCacheTouchHitPercentage(), 0.01);
    }

    @Test
    public void testCacheTouchMissPercentage() {
        metrics.onCacheTouch();
        metrics.onCacheTouchMiss();
        assertEquals(100.0, metrics.getCacheTouchMissPercentage(), 0.01);
    }
}
