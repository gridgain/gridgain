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
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.*;

/**
 * Test class for cache touch metrics.
 */

public class CacheMetricsTouchTest extends GridCommonAbstractTest {
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

        // Explicitly mock rebalanceStartTime metric
        AtomicLongMetric rebalanceStartTimeMetric = mock(AtomicLongMetric.class);
        when(rebalanceStartTimeMetric.value()).thenReturn(-1L);
        when(metricRegistry.longMetric(eq("RebalanceStartTime"), anyString())).thenReturn(rebalanceStartTimeMetric);

        // Mock and stub the metrics
        mockAndStubMetrics();

        metrics = new CacheMetricsImpl(cctx, false);
    }

    private void mockAndStubMetrics() {
        String[] metricNames = new String[] {
                "CacheTouches",
                "CacheTouchHits",
                "CacheTouchMisses",
                "reads",
                "entryProcessorPuts",
                "entryProcessorRemovals",
                "entryProcessorReadOnlyInvocations",
                "entryProcessorInvokeTimeNanos",
                "entryProcessorMinInvocationTime",
                "entryProcessorMaxInvocationTime",
                "entryProcessorHits",
                "entryProcessorMisses",
                "writes",
                "hits",
                "misses",
                "txCommits",
                "txRollbacks",
                "evictCnt",
                "rmCnt",
                "putTimeTotal",
                "getTimeTotal",
                "rmvTimeTotal",
                "commitTimeTotal",
                "rollbackTimeTotal",
                "offHeapGets",
                "offHeapPuts",
                "offHeapRemoves",
                "offHeapEvicts",
                "offHeapHits",
                "offHeapMisses",
                "rebalancedKeys",
                "totalRebalancedBytes",
                "rebalanceStartTime",
                "estimatedRebalancingKeys",
                "rebalancingKeysRate",
                "rebalancingBytesRate",
                "rebalanceClearingPartitions",
                "evictingPartitions",
                "getTime",
                "putTime",
                "rmvTime",
                "commitTime",
                "rollbackTime",
                "idxRebuildKeyProcessed",
                "offHeapEntriesCnt",
                "offHeapPrimaryEntriesCnt",
                "offHeapBackupEntriesCnt",
                "heapEntriesCnt",
                "cacheSize"
        };

        for (String metricName : metricNames) {
            AtomicLongMetric mockMetric = mockAtomicLongMetric();
            when(metricRegistry.longMetric(eq(metricName), anyString())).thenReturn(mockMetric);
        }
    }

    private AtomicLongMetric mockAtomicLongMetric() {
        AtomicLongMetric atomicLongMetric = mock(AtomicLongMetric.class);
        AtomicLong value = new AtomicLong(0L);  // Start with 0L to match expected behavior.

        // Simulate getting the current value.
        when(atomicLongMetric.value()).thenAnswer(invocation -> value.get());

        // Simulate incrementing the value using increment().
        doAnswer(invocation -> {
            value.incrementAndGet();
            return null;
        }).when(atomicLongMetric).increment();

        // Simulate setting a new value.
        doAnswer(invocation -> {
            long newValue = invocation.getArgument(0);
            value.set(newValue);
            return null;
        }).when(atomicLongMetric).value(anyLong());

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
