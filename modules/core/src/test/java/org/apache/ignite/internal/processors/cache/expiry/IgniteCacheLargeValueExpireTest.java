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

package org.apache.ignite.internal.processors.cache.expiry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test touch expiration policy for large entries.
 */
@RunWith(Parameterized.class)
public class IgniteCacheLargeValueExpireTest extends GridCommonAbstractTest {
    /** Page size. */
    private static final int PAGE_SIZE = 1024;

    /** Number of keys that insrted/updated on each iteration. */
    private static final int NUMBER_OF_ENTRIES = GridTestUtils.SF.applyLB(10_000, 1_000);

    /** Name of the default data region." */
    private static final String DEFAULT_DATA_REGION_NAME = "default";

    /** Name of the additional data region." */
    private static final String SHARED_DATA_REGION_NAME = "shared";

    /**
     * Parameter that defines using a shared cache group.
     **/
    @Parameterized.Parameter
    public Boolean useSharedGroup;

    /** Test run configurations. */
    @Parameterized.Parameters(name = "useSharedGroup = {0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>(2);

        params.add(new Object[] {Boolean.FALSE});
        params.add(new Object[] {Boolean.TRUE});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();
        dbCfg.setPageSize(1024);

        DataRegionConfiguration dataRegion1 = new DataRegionConfiguration();
        dataRegion1.setName(DEFAULT_DATA_REGION_NAME);

        DataRegionConfiguration dataRegion2 = new DataRegionConfiguration();
        dataRegion2.setName(SHARED_DATA_REGION_NAME);
        dataRegion2.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);

        dbCfg.setDefaultDataRegionConfiguration(dataRegion1);
        dbCfg.setDataRegionConfigurations(dataRegion2);

        cfg.setDataStorageConfiguration(dbCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpire() throws Exception {
        checkExpire(grid(0), cacheConfiguration(groupName(), dataRegionName(), 1, false));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpireWithEagerTtlEnabled() throws Exception {
        checkExpire(grid(0), cacheConfiguration(groupName(), dataRegionName(), 1, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProlongation() throws Exception {
        checkTtlProlongationAndExpirtion(grid(0), cacheConfiguration(groupName(), dataRegionName(), 1, false));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProlongationWithEagerTtlEnabled() throws Exception {
        checkTtlProlongationAndExpirtion(grid(0), cacheConfiguration(groupName(), dataRegionName(), 1, true));
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkExpire(Ignite ignite, CacheConfiguration<Object, Object> ccfg) throws Exception {
        ignite.createCache(ccfg);

        try {
            IgniteCache<Object, Object> cache =
                ignite.cache(DEFAULT_CACHE_NAME).withExpiryPolicy(new TouchedExpiryPolicy(new Duration(0, 500)));

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < 10; i++) {
                log.info("Iteration: " + i);

                AtomicInteger cntr = new AtomicInteger();

                List<Object> keys = new ArrayList<>();

                for (int j = 0; j < NUMBER_OF_ENTRIES; j++) {
                    IgniteBiTuple<Object, Object> keyValPair = generateKeyValuePair(cntr, rnd);

                    cache.put(keyValPair.getKey(), keyValPair.getValue());

                    keys.add(keyValPair.getKey());
                }

                U.sleep(1000);

                for (Object key : keys)
                    assertNull(cache.get(key));
            }
        }
        finally {
            ignite.destroyCache(ccfg.getName());
        }
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkTtlProlongationAndExpirtion(Ignite ignite, CacheConfiguration<Object, Object> ccfg) throws Exception {
        ignite.createCache(ccfg);

        try {
            IgniteCache<Object, Object> cache =
                ignite.cache(DEFAULT_CACHE_NAME).withExpiryPolicy(new TouchedExpiryPolicy(new Duration(0, 10_000)));

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            List<Object> allkeys = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                log.info("Iteration: " + i);

                AtomicInteger cntr = new AtomicInteger();

                List<Object> keys = new ArrayList<>();

                for (int j = 0; j < NUMBER_OF_ENTRIES; j++) {
                    IgniteBiTuple<Object, Object> keyValPair = generateKeyValuePair(cntr, rnd);

                    cache.put(keyValPair.getKey(), keyValPair.getValue());

                    keys.add(keyValPair.getKey());
                    allkeys.add(keyValPair.getKey());
                }

                for (Object key : keys) {
                    // Check that the entry is not expired and prolongation works.
                    assertNotNull(cache.get(key));
                }
            }

            U.sleep(15_000);

            for (Object key : allkeys)
                assertNull(cache.get(key));
        }
        finally {
            ignite.destroyCache(ccfg.getName());
        }
    }

    /**
     * Generates and returns key-value pair.
     *
     * @param cnt Counter.
     * @param rnd Random.
     * @return Key-value pair.
     */
    private IgniteBiTuple<Object, Object> generateKeyValuePair(AtomicInteger cnt, ThreadLocalRandom rnd) {
        Object key = null;
        Object val = null;

        switch (rnd.nextInt(3)) {
            case 0:
                key = rnd.nextInt(100_000);
                val = new TestKeyValue(cnt.getAndIncrement(), new byte[rnd.nextInt(3 * PAGE_SIZE)]);
                break;

            case 1:
                key = new TestKeyValue(cnt.getAndIncrement(), new byte[rnd.nextInt(3 * PAGE_SIZE)]);
                val = rnd.nextInt();
                break;

            case 2:
                key = new TestKeyValue(cnt.getAndIncrement(), new byte[rnd.nextInt(3 * PAGE_SIZE)]);
                val = new TestKeyValue(cnt.getAndIncrement(), new byte[rnd.nextInt(3 * PAGE_SIZE)]);
                break;

            default:
                fail();
        }

        return new IgniteBiTuple<>(key, val);
    }

    /**
     * Creates a new cache configuration.
     *
     * @param groupName Group name.
     * @param backups Number of backups.
     * @param eagerTtl Value for {@link CacheConfiguration#setEagerTtl(boolean)}.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String groupName, String regionName, int backups, boolean eagerTtl) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setBackups(backups);
        ccfg.setEagerTtl(eagerTtl);
        ccfg.setGroupName(groupName);
        ccfg.setDataRegionName(regionName);

        return ccfg;
    }

    /**
     * Returns cache group name.
     * If {@link #useSharedGroup} is {@code true} then returns {@code "testGroup"} otherwise returns {@code null}.
     *
     * @return Cache group name.
     */
    private String groupName() {
        return useSharedGroup ? "testGroup" : null;
    }

    /**
     * Returns cache data region name.
     * If {@link #useSharedGroup} is {@code true} then returns {@link #DEFAULT_DATA_REGION_NAME}
     * otherwise returns {@link #SHARED_DATA_REGION_NAME}.
     *
     * @return Cache data region name.
     */
    private String dataRegionName() {
        return useSharedGroup ? SHARED_DATA_REGION_NAME : DEFAULT_DATA_REGION_NAME;
    }

    /**
     *
     */
    private static class TestKeyValue implements Serializable {
        /** */
        private int id;

        /** */
        private byte[] val;

        /**
         * @param id ID.
         * @param val Value.
         */
        TestKeyValue(int id, byte[] val) {
            this.id = id;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKeyValue that = (TestKeyValue)o;

            return id == that.id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }
}
