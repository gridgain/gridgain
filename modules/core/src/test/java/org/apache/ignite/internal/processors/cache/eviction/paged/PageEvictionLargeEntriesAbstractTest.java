/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.EntryCompressionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public abstract class PageEvictionLargeEntriesAbstractTest extends GridCommonAbstractTest {
    /** Page size. */
    private static final int PAGE_SIZE = (int)(2 * IgniteUtils.KB);

    /** Size of a large entry. A single entry should cover a very large number of pages. */
    private static final int LARGE_ENTRY_SIZE = PAGE_SIZE * 10_000;

    /** Empty pages pool size, matches default. */
    private static final int EMPTY_PAGES_POOL_SIZE = 100;

    /** Eviction threshold, matches default. */
    private static final double EVICTION_THRESHOLD = 0.9;

    /** Default region name. */
    private static final String DEFAULT_REGION_NAME = "defaultRegion";

    /**
     * Off-heap size for the data region. The formula states that "by the time we exhaust the eviction threshold, we
     * have space to fit 10 more entries". The value "10" is arbitrary. We also align the size to the nearest MB.
     */
    private static final int SIZE = (int)(
        IgniteUtils.MB * (long)Math.ceil(10 * LARGE_ENTRY_SIZE / (1 - EVICTION_THRESHOLD) / IgniteUtils.MB)
    );

    /** Number of entries. At least twice the size of the data region. */
    private static final int ENTRIES = SIZE / LARGE_ENTRY_SIZE * 2;

    /**
     * @param mode Eviction mode.
     * @param configuration Configuration.
     * @return Configuration with given eviction mode set.
     */
    static IgniteConfiguration setEvictionMode(DataPageEvictionMode mode, IgniteConfiguration configuration) {
        DataRegionConfiguration[] policies = configuration.getDataStorageConfiguration().getDataRegionConfigurations();

        if (policies != null) {
            for (DataRegionConfiguration plcCfg : policies)
                plcCfg.setPageEvictionMode(mode);
        }

        configuration.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPageEvictionMode(mode);

        return configuration;
    }

    /**
     * @return Compression configuration or {@code null} if none is needed.
     */
    protected EntryCompressionConfiguration entryCompressionConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();

        // This will test additional segment allocation.
        dataRegionCfg.setInitialSize(SIZE / 2);
        dataRegionCfg.setMaxSize(SIZE);
        dataRegionCfg.setEmptyPagesPoolSize(EMPTY_PAGES_POOL_SIZE);
        dataRegionCfg.setEvictionThreshold(EVICTION_THRESHOLD);
        dataRegionCfg.setName(DEFAULT_REGION_NAME);

        dbCfg.setDefaultDataRegionConfiguration(dataRegionCfg);
        dbCfg.setPageSize(PAGE_SIZE);

        cfg.setDataStorageConfiguration(dbCfg);

        if (gridName.startsWith("client"))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @param name Name.
     * @param memoryPlcName Memory policy name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param writeSynchronizationMode Write synchronization mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfig(
        @NotNull String name,
        String memoryPlcName,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        CacheWriteSynchronizationMode writeSynchronizationMode
    ) {
        CacheConfiguration<Object, Object> cacheConfiguration = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setName(name)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setDataRegionName(memoryPlcName)
            .setWriteSynchronizationMode(writeSynchronizationMode)
            .setEntryCompressionConfiguration(entryCompressionConfiguration());

        if (cacheMode == CacheMode.PARTITIONED)
            cacheConfiguration.setBackups(1);

        return cacheConfiguration;
    }

    /** Cache modes. */
    private static final CacheMode[] CACHE_MODES = {CacheMode.PARTITIONED, CacheMode.REPLICATED};

    /** Atomicity modes. */
    private static final CacheAtomicityMode[] ATOMICITY_MODES = {
        CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

    /** Write modes. */
    private static final CacheWriteSynchronizationMode[] WRITE_MODES = {CacheWriteSynchronizationMode.PRIMARY_SYNC,
        CacheWriteSynchronizationMode.FULL_SYNC, CacheWriteSynchronizationMode.FULL_ASYNC};

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(4, false);

        startGrid("client");
    }

    /**
     * @return Client grid.
     */
    Ignite clientGrid() {
        return grid("client");
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }

    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageEviction() throws Exception {
        for (int i = 0; i < CACHE_MODES.length; i++) {
            for (int j = 0; j < ATOMICITY_MODES.length; j++) {
                for (int k = 0; k < WRITE_MODES.length; k++) {
                    if (i + j + Math.min(k, 1) <= 1) {
                        CacheConfiguration<Object, Object> cfg = cacheConfig(
                            "evict" + i + j + k, null, CACHE_MODES[i], ATOMICITY_MODES[j], WRITE_MODES[k]);

                        createCacheAndTestEviction(cfg);
                    }
                }
            }
        }
    }

    /**
     * @param cfg Config.
     * @throws Exception If failed.
     */
    protected void createCacheAndTestEviction(CacheConfiguration<Object, Object> cfg) throws Exception {
        IgniteCache<Object, Object> cache = clientGrid().getOrCreateCache(cfg);

        for (int i = 1; i <= ENTRIES; i++) {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            cache.put(i, new TestObject(LARGE_ENTRY_SIZE / Integer.BYTES));

//            if (r.nextInt() % 5 == 0)
//                cache.put(i, new TestObject(PAGE_SIZE / 4 - 50 + r.nextInt(5000))); // Fragmented object.
//            else
//                cache.put(i, new TestObject(r.nextInt(PAGE_SIZE / 4 - 50))); // Fits in one page.

            if (r.nextInt() % 7 == 0)
                cache.get(r.nextInt(i)); // Touch.
            else if (r.nextInt() % 11 == 0)
                cache.remove(r.nextInt(i)); // Remove.
            else if (r.nextInt() % 13 == 0)
                cache.put(r.nextInt(i), new TestObject(r.nextInt(PAGE_SIZE / 2))); // Update.

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries put: " + i);
        }

        int resultingSize = cache.size(CachePeekMode.PRIMARY);

        System.out.println(">>> Resulting size: " + resultingSize);

//        // Eviction started, no OutOfMemory occurred, success.
//        assertTrue(resultingSize < ENTRIES * 10 / 11);

        clientGrid().destroyCache(cfg.getName());
    }
}
