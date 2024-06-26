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

package org.apache.ignite.internal.processors.cache;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.DataPageEvictionMode.DISABLED;

/**
 * Tests behavior of IgniteCache when {@link IgniteOutOfMemoryException} is thrown.
 */
public class AbstractCacheIgniteOutOfMemoryExceptionTest extends GridCommonAbstractTest {
    /** Minimal region size. */
    protected static final long DATA_REGION_SIZE = 10L * U.MB;

    /** Huge data region. */
    protected static final long HUGE_DATA_REGION_SIZE = U.GB;

    /** Region name. */
    protected static final String HUGE_DATA_REGION_NAME = "hugeRegion";

    /** Region name. */
    protected static final String HUGE_ATOMIC_CACHE_NAME = "huge-atomic-cache";

    /** Page size. */
    protected static final long PAGE_SIZE = 4 * 1024;

    /** */
    protected static final int ATTEMPTS_NUM = 3;

    /** Node failure occurs. */
    protected static final AtomicBoolean failure = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setPageSize(4096)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(DATA_REGION_SIZE)
                    .setPageEvictionMode(DISABLED)
                    .setPersistenceEnabled(false)
                    .setMetricsEnabled(true))
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName(HUGE_DATA_REGION_NAME)
                    .setMaxSize(HUGE_DATA_REGION_SIZE)
                    .setPersistenceEnabled(false)
                    .setMetricsEnabled(true)));

        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

                // Do not invalidate a node context.
                return false;
            }
        });

        cfg.setCacheConfiguration(cacheConfiguration(ATOMIC), cacheConfiguration(TRANSACTIONAL));

        return cfg;
    }

    /**
     * Creates a new cache configuration with the given cache atomicity mode.
     *
     * @param mode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Object> cacheConfiguration(CacheAtomicityMode mode) {
        return cacheConfiguration(mode, mode.name(), null);
    }

    /**
     * Creates a new cache configuration with the given cache atomicity mode.
     *
     * @param mode Cache atomicity mode.
     * @param cacheName Cache name.
     * @param dataRegion Data region name.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, Object> cacheConfiguration(
        CacheAtomicityMode mode,
        String cacheName,
        String dataRegion) {
        return new CacheConfiguration<Integer, Object>(cacheName)
            .setAtomicityMode(mode)
            .setDataRegionName(dataRegion)
            .setAffinity(new RendezvousAffinityFunction(false, 32));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteCache<Object, Object> cache = grid(0).cache(HUGE_ATOMIC_CACHE_NAME);

        if (cache != null)
            cache.destroy();
    }

    /**
     * Creates a new cache with the given atomicity node and tries to load & clear it in a loop.
     * It is assumed that {@link IgniteOutOfMemoryException} is thrown during loading the cache,
     * however {@link IgniteCache#clear()} should return the cache to the operable state.
     *
     * @param mode Cache atomicity mode.
     * @param attempts Number of attempts to load and clear the cache.
     */
    protected void loadAndClearCache(CacheAtomicityMode mode, int attempts) {
        IgniteCache<Object, Object> cache = grid(0).cache(mode.name());

        for (int i = 0; i < attempts; ++i) {
            try {
                for (int key = 0; key < 500_000; ++key)
                    cache.put(key, new byte[4000]);

                fail("OutOfMemoryException hasn't been thrown");
            }
            catch (Exception e) {
                assertTrue(
                    "Exception has been thrown, but the exception type is unexpected [exc=" + e + ']',
                    X.hasCause(e, IgniteOutOfMemoryException.class));

                assertTrue("Failure handler should be called due to IOOM.", failure.get());
            }

            // Let's check that the cache can be cleared without any errors.
            failure.set(false);

            try {
                cache.clear();
            }
            catch (Exception e) {
                fail("Clearing the cache should not trigger any exception [exc=" + e + ']');
            }

            assertFalse("Failure handler should not be called during clearing the cache.", failure.get());
        }
    }

    public void testContainsFromBackup0(IgniteCache<Integer, Object> cache, int cnt) {
        testContains(cache, backupKeys(cache, 1, 0), true);
    }

    /**
     * Tests that contains operation does not require loading the whole entry.
     *
     * @param cache Cache.
     * @param keys Keys.
     * @param expectContains Expected result.
     */
    protected void testContains(IgniteCache<Integer, Object> cache, Collection<Integer> keys, boolean expectContains) {
        Runtime.getRuntime().gc();

        int blobSize = (int) (512 / keys.size() * U.MB);

        for (Integer k : keys)
            cache.put(k, new byte[blobSize]);

        // Let's occupy all free memory.
        List<Object> unused = new ArrayList<>();
        IgniteBiTuple<HotSpotDiagnosticMXBean, VMOption> mbean = disableHeapDumpOnOutOfMemoryError();
        try {
            while (true) {
                try {
                    unused.add(new byte[(int) (50 * U.MB)]);
                } catch (OutOfMemoryError e) {
                    // We don't have enough space to allocate a new continous block.
                    // Let's remove one blob in order to have enough memory to process the request.
                    unused.remove(unused.size() - 1);
                    break;
                }
            }
        }
        finally {
            restoreHeapDumpOnOutOfMemoryError(mbean);
        }

        // This code should not throw OutOfMemoryError.
        assertEquals(
            expectContains ? "The request key is not found, but it should be." : "The request key is found but should not be.",
            expectContains,
            keys.size() == 1 ? cache.containsKey(keys.iterator().next()) : cache.containsKeys(new HashSet<>(keys)));

        // To avoid JIT effects (removing unused variabales).
        assertFalse(unused.isEmpty());
    }

    /**
     * @return DataRegionMetrics for the default data region.
     */
    protected DataRegionMetrics getDefaultRegionMetrics() {
        return grid(0).dataRegionMetrics().stream().filter(d -> d.getName().equals("default")).findFirst().get();
    }

    protected void restoreHeapDumpOnOutOfMemoryError(IgniteBiTuple<HotSpotDiagnosticMXBean, VMOption> bean) {
        if (bean != null) {
            try {
                bean.get1().setVMOption("HeapDumpOnOutOfMemoryError", bean.get2().getValue());
            }
            catch (Exception e) {
                // No-op.
            }
        }
    }

    protected IgniteBiTuple<HotSpotDiagnosticMXBean, VMOption> disableHeapDumpOnOutOfMemoryError() {
        try {
            MBeanServer srv = ManagementFactory.getPlatformMBeanServer();

            String hotSpotBeanName = "com.sun.management:type=HotSpotDiagnostic";

            HotSpotDiagnosticMXBean hotSpotDiagnosticMXBean = ManagementFactory.newPlatformMXBeanProxy(
                srv,
                hotSpotBeanName,
                HotSpotDiagnosticMXBean.class);

            VMOption option = hotSpotDiagnosticMXBean.getVMOption("HeapDumpOnOutOfMemoryError");

            hotSpotDiagnosticMXBean.setVMOption("HeapDumpOnOutOfMemoryError", "false");

            return new IgniteBiTuple<>(hotSpotDiagnosticMXBean, option);
        }
        catch (Exception e) {
            return null;
        }
    }
}
