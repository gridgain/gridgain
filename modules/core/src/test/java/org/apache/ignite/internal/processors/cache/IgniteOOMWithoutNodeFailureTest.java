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
package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests how eviction behaves when {@link DataRegionConfiguration#getEmptyPagesPoolSize()} is not enough for large
 * values.
 */
public class IgniteOOMWithoutNodeFailureTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        memCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setMaxSize(30L * 1024 * 1024)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU)
        );

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Object, Object> baseCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        baseCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        baseCfg.setCacheMode(CacheMode.PARTITIONED);
        baseCfg.setBackups(0);
        baseCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        cfg.setMetricsLogFrequency(1_000);
        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler(false, 0));
        cfg.setCacheConfiguration(baseCfg);

        return cfg;
    }

    /**
     * Tests how eviction behaves when {@link DataRegionConfiguration#getEmptyPagesPoolSize()} is not enough for large
     * values.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteOOMWithoutNodeFailure() throws Exception {
        IgniteEx sn = startGrids(1);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = sn.cache(DEFAULT_CACHE_NAME);

        List<IgniteInternalFuture<Object>> futures = new ArrayList<>();

        Function<ThreadLocalRandom, byte[]> function = random -> new byte[random.nextInt(3) * 100 * 1024];

        AtomicInteger exceptionsCnt = new AtomicInteger();

        AtomicInteger successfulOpCountAfterException = new AtomicInteger();

        final int exceptionsLimit = 2;

        IgniteInClosure<IgniteInClosure<ThreadLocalRandom>> task = op -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            while (true) {
                if (exceptionsCnt.get() >= exceptionsLimit)
                    break;

                try {
                    op.apply(random);

                    if (exceptionsCnt.get() > 0)
                        successfulOpCountAfterException.incrementAndGet();
                }
                catch (CachePartialUpdateException e) {
                    if (X.hasCause(e, IgniteOutOfMemoryException.class)) {
                        exceptionsCnt.incrementAndGet();

                        break;
                    }
                }
            }
        };

        for (int i = 0; i < 3; i++) {
            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
                task.apply(random -> cache.put(random.nextInt(), function.apply(random)));
            });

            futures.add(fut);
        }

        for (int i = 0; i < 3; i++) {
            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
                task.apply(random -> cache.replace(random.nextInt(), function.apply(random)));
            });

            futures.add(fut);
        }

        for (IgniteInternalFuture<Object> future : futures)
            future.get(2, TimeUnit.MINUTES);

        assertTrue(exceptionsCnt.get() >= exceptionsLimit);
        assertTrue(successfulOpCountAfterException.get() > 0);

        log.info("Successful operations after exception: " + successfulOpCountAfterException.get());
    }
}
