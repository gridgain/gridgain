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

package org.apache.ignite.internal.processors.database;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.persistence.DataStructure;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Base class for memory leaks tests.
 */
public abstract class IgniteDbMemoryLeakAbstractTest extends IgniteDbAbstractTest {
    /** */
    private static final int CONCURRENCY_LEVEL = 16;

    /** */
    private static final int MIN_PAGE_CACHE_SIZE = 1048576 * CONCURRENCY_LEVEL;

    /** */
    private volatile Exception ex;

    /** */
    private long endTime;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        DataStructure.rnd = null;

        endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(duration());
    }

    /** {@inheritDoc} */
    @Override protected void configure(IgniteConfiguration cfg) {
        cfg.setMetricsLogFrequency(5000);
    }

    /** {@inheritDoc} */
    @Override protected void configure(DataStorageConfiguration mCfg) {
        mCfg.setConcurrencyLevel(CONCURRENCY_LEVEL);

        long size = (1024 * (isLargePage() ? 16 : 4) + 24) * pagesMax();

        mCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(Math.max(size, MIN_PAGE_CACHE_SIZE)).setName("default"));
    }

    /**
     * @return Test duration in seconds.
     */
    protected int duration() {
        return getInteger("IGNITE_MEMORY_LEAKS_TEST_DURATION", 30);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected boolean indexingEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return (duration() + 10) * 1000; // Extra seconds to stop all threads.
    }

    /**
     * @param ig Ignite instance.
     * @return IgniteCache.
     */
    protected abstract IgniteCache<Object, Object> cache(IgniteEx ig);

    /**
     * @return Cache key to perform an operation.
     */
    protected abstract Object key();

    /**
     * @param key Cache key to perform an operation.
     * @return Cache value to perform an operation.
     */
    protected abstract Object value(Object key);

    /**
     * @param cache IgniteCache.
     */
    protected void operation(IgniteCache<Object, Object> cache) {
        Object key = key();
        Object val = value(key);

        switch (nextInt(3)) {
            case 0:
                cache.getAndPut(key, val);

                break;

            case 1:
                cache.get(key);

                break;

            case 2:
                cache.getAndRemove(key);
        }
    }

    /**
     * @param bound Upper bound (exclusive). Must be positive.
     * @return Random int value.
     */
    protected static int nextInt(int bound) {
        return ThreadLocalRandom.current().nextInt(bound);
    }

    /**
     * @return Random int value.
     */
    protected static int nextInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryLeak() throws Exception {
        final IgniteEx ignite = grid(0);

        final IgniteCache<Object, Object> cache = cache(ignite);

        Runnable target = new Runnable() {
            @Override public void run() {
                while (ex == null && System.nanoTime() < endTime) {
                    try {
                        operation(cache);
                    }
                    catch (Exception e) {
                        ex = e;

                        break;
                    }
                }
            }
        };

        Thread[] threads = new Thread[CONCURRENCY_LEVEL];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(target);
            threads[i].start();
        }

        while (ex == null && System.nanoTime() < endTime) {
            try {
                check(cache);
            }
            catch (Exception e) {
                ex = e;

                break;
            }

            Thread.sleep(100L);
        }

        if (ex != null)
            throw ex;
    }

    /**
     * Callback to check the current state.
     *
     * @param cache Cache instance.
     * @throws Exception If failed.
     */
    protected final void check(IgniteCache cache) throws Exception {
        long pagesActual = ((IgniteCacheProxy)cache).context().dataRegion().pageMemory().loadedPages();

        long pagesAllowed = pagesMax();

        assertTrue("Allocated pages count is more than expected [allowed=" + pagesAllowed + ", actual=" + pagesActual + "]", pagesActual < pagesAllowed);
    }

    /**
     * @return Maximal allowed pages number.
     */
    protected abstract long pagesMax();

}
