package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;

public class CacheMetricsIntegrationTest extends GridCommonAbstractTest {
    private Ignite ignite;
    private IgniteCache<Integer, String> cache;

    @Before
    public void setUp() {
        ignite = Ignition.start();
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>("testCache");
        cache = ignite.createCache(cfg);
    }

    @Test
    public void testCacheTouchMetrics() {
        cache.put(1, "value1");
        cache.touch(1);

        CacheMetrics metrics = cache.metrics();

        assertEquals(1, metrics.getCacheTouches());
        assertEquals(1, metrics.getCacheTouchHits());
        assertEquals(0, metrics.getCacheTouchMisses());
    }

    @Test
    public void testCacheTouchMetricsMiss() {
        cache.touch(2);

        CacheMetrics metrics = cache.metrics();

        assertEquals(1, metrics.getCacheTouches());
        assertEquals(0, metrics.getCacheTouchHits());
        assertEquals(1, metrics.getCacheTouchMisses());
    }
}
