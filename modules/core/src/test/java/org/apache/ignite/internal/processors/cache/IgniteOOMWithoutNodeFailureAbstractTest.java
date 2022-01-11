/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionHeuristicException;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

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
        cfg.setCacheConfiguration(baseCfg, txCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

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
     * @param cache Cache.
     * @param cacheOpWrapper Wrapper for cache operations.
     * @throws Exception If failed.
     */
    protected void testIgniteOOMWithoutNodeFailure(
        IgniteCache<Object, Object> cache,
        Consumer<Runnable> cacheOpWrapper
    ) throws Exception {
        List<IgniteInternalFuture<Void>> futures = new ArrayList<>();

        Function<ThreadLocalRandom, byte[]> randomVal = random -> new byte[random.nextInt(3) * 100 * 1024];
        Function<ThreadLocalRandom, byte[]> randomSmallVal = random -> new byte[random.nextInt(3) * 50 * 1024];

        AtomicBoolean running = new AtomicBoolean(true);

        DataRegion dataRegion = ignite.context().cache().cache(cache.getName()).context().dataRegion();

        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());

        int emptyPagesPoolSizeInitial = dataRegion.emptyPagesPoolSize();

        IgniteInClosure<IgniteInClosure<ThreadLocalRandom>> task = op -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            while (running.get()) {
                try {
                    cacheOpWrapper.accept(() -> op.apply(random));
                }
                catch (CachePartialUpdateException | TransactionHeuristicException e) {
                    exceptions.add(e);
                }
            }
        };

        for (int i = 0; i < 3; i++) {
            IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(() -> {
                task.apply(random -> cache.put(random.nextInt(), randomVal.apply(random)));
            });

            futures.add(fut);
        }

        for (int i = 0; i < 3; i++) {
            IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(() -> {
                task.apply(random -> cache.replace(random.nextInt(), randomVal.apply(random)));
            });

            futures.add(fut);
        }

        for (int i = 0; i < 3; i++) {
            IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(() -> task.apply(random -> {
                Map<Integer, Object> map = new HashMap<>();

                map.put(random.nextInt(), randomSmallVal.apply(random));
                map.put(random.nextInt(), randomSmallVal.apply(random));

                cache.putAll(map);
            }));

            futures.add(fut);
        }

        waitForCondition(() -> dataRegion.emptyPagesPoolSize() >= emptyPagesPoolSizeInitial * 4, 60_000, 10);

        int emptyPagesPoolSizeAfterWarmup = dataRegion.emptyPagesPoolSize();

        log.info("Test - empty pages pool size after test warmup: " + emptyPagesPoolSizeAfterWarmup);

        exceptions.clear();

        doSleep(20_000);

        running.set(false);

        for (IgniteInternalFuture<Void> future : futures)
            future.get(5, TimeUnit.SECONDS);

        assertTrue(dataRegion.emptyPagesPoolSize() > emptyPagesPoolSizeInitial);

        // Exceptions count is 0 in most cases. But we can't guarantee this, as eviction doesn't happen
        // atomically with put/replace.
        log.info("Test - exceptions count: " + exceptions.size());
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
