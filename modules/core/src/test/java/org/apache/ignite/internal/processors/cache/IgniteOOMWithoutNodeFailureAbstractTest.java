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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
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
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests how eviction behaves when {@link DataRegionConfiguration#getEmptyPagesPoolSize()} is not enough for large
 * values.
 */
public class IgniteOOMWithoutNodeFailureAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final String TX_CACHE_NAME = "tx-cache";

    /** */
    protected IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        memCfg.setDataRegionConfigurations(
            new DataRegionConfiguration()
                .setName("atomic")
                .setMaxSize(30L * 1024 * 1024)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU),
            new DataRegionConfiguration()
                .setName("tx")
                .setMaxSize(30L * 1024 * 1024)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU)
        );

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Object, Object> baseCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setWriteThrough(true)
            .setCacheWriterFactory(new CacheWriterFactory())
            .setDataRegionName("atomic");

        CacheConfiguration<Object, Object> txCacheCfg = new CacheConfiguration<>(TX_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setDataRegionName("tx");

        cfg.setMetricsLogFrequency(1_000);
        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler(false, 0));
        cfg.setCacheConfiguration(baseCfg, txCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite = startGrids(1);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests how eviction behaves when {@link DataRegionConfiguration#getEmptyPagesPoolSize()} is not enough for large
     * values.
     *
     * @throws Exception If failed.
     */
    protected void testIgniteOOMWithoutNodeFailure(IgniteCache<Object, Object> cache, Consumer<Runnable> cacheOpWrapper) throws Exception {
        List<IgniteInternalFuture<Object>> futures = new ArrayList<>();

        Function<ThreadLocalRandom, byte[]> function = random -> new byte[random.nextInt(3) * 100 * 1024];

        AtomicReference<Exception> exceptionsRef = new AtomicReference<>();

        AtomicBoolean running = new AtomicBoolean();

        IgniteInClosure<IgniteInClosure<ThreadLocalRandom>> task = op -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            while (running.get()) {
                try {
                    cacheOpWrapper.accept(() -> op.apply(random));
                }
                catch (CachePartialUpdateException e) {
                    exceptionsRef.compareAndSet(null, e);

                    running.set(false);
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

        for (int i = 0; i < 3; i++) {
            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> task.apply(random -> {
                Map<Integer, Object> map = new HashMap<>();

                map.put(random.nextInt(), function.apply(random));
                map.put(random.nextInt(), function.apply(random));

                cache.putAll(map);
            }));

            futures.add(fut);
        }

        doSleep(10_000);

        running.set(false);

        for (IgniteInternalFuture<Object> future : futures)
            future.get(5, TimeUnit.SECONDS);

        assertNull(exceptionsRef.get());
    }

    /**
     *
     */
    private static class CacheWriterImpl implements CacheWriter<Object, Object>, Serializable {
        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> collection) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object o) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> collection) throws CacheWriterException {
            // No-op.
        }
    }

    /**
     *
     */
    private static class CacheWriterFactory implements Factory<CacheWriter<Object, Object>>, Serializable {
        /** {@inheritDoc} */
        @Override public CacheWriter<Object, Object> create() {
            return new CacheWriterImpl();
        }
    }
}
