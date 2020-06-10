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


package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test TTL worker with persistence enabled after node is removed from baseline.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "5")
public class IgnitePdsWithTtlDeferredDeleteOnRestartTest extends GridCommonAbstractTest {
    /**
     *
     */
    public static final String CACHE_NAME = "expirableCache";

    /**
     *
     */
    public static final String GROUP_NAME = "group1";

    /**
     *
     */
    private static final int EXPIRATION_TIMEOUT = 60;

    /**
     *
     */
    public static final int ENTRIES = 100_000;

    /**
     * Fail.
     */
    private volatile boolean fail;

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(2L * 1024 * 1024 * 1024)
                        .setPersistenceEnabled(true)
                ).setWalMode(WALMode.LOG_ONLY));

        cfg.setCacheConfiguration(getCacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new NoOpFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                fail = true;

                return super.handle(ignite, failureCtx);
            }
        };
    }

    /**
     * Returns a new cache configuration with the given name and {@code GROUP_NAME} group.
     *
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration getCacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(GROUP_NAME);
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, EXPIRATION_TIMEOUT)));
        ccfg.setEagerTtl(true);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * Tests that TTL cleanup worker correctly stop work after node was removed from baseline.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testDeferredDeleteOnRestartNodeWithBaselineChange() throws Exception {
        try {
            IgniteEx srv = startGrid(0);

            srv.cluster().baselineAutoAdjustEnabled(false);

            startGrid(2);

            srv.cluster().active(true);

            ExpiryPolicy plc = AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 20)).create();

            IgniteCache<Integer, byte[]> cache0 = srv.cache(CACHE_NAME);

            fillCache(cache0.withExpiryPolicy(plc));

            stopGrid(2);

            awaitPartitionMapExchange();

            srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

            startGrid(2);

            assertFalse("Failure handler was called. See log above.", fail);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    protected void fillCache(IgniteCache<Integer, byte[]> cache) {
        cache.putAll(new TreeMap<Integer, byte[]>() {{
            for (int i = 0; i < ENTRIES; i++)
                put(i, new byte[1024]);
        }});

        //Touch entries.
        for (int i = 0; i < ENTRIES; i++)
            cache.get(i); // touch entries
    }
}
