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

package org.apache.ignite.internal.processors.cache.ttl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Checks that enabled read-only mode doesn't affect data expiration.
 */
public class CacheTtlReadOnlyModeSelfTest extends GridCommonAbstractTest {
    /** Expiration timeout in seconds. */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** Cache configurations. */
    private static final CacheConfiguration[] CACHE_CONFIGURATIONS = getCacheConfigurations();

    /** Cache names. */
    private static final Collection<String> CACHE_NAMES =
        Stream.of(CACHE_CONFIGURATIONS).map(CacheConfiguration::getName).collect(Collectors.toList());

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(CACHE_CONFIGURATIONS);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test must be deleted after https://ggsystems.atlassian.net/browse/GG-25084
     */
    @Test
    public void testOldReadOnlyPublicApiIsNotAvailable() throws Exception {
        // TODO: Remove me after https://ggsystems.atlassian.net/browse/GG-25084

        assertThrows(log,() -> IgniteCluster.class.getMethod("readOnly", boolean.class), NoSuchMethodException.class, null);
        assertThrows(log,() -> IgniteCluster.class.getMethod("readOnly"), NoSuchMethodException.class, null);

        assertThrows(log,() -> GridClientClusterState.class.getMethod("readOnly", boolean.class), NoSuchMethodException.class, null);
        assertThrows(log,() -> GridClientClusterState.class.getMethod("readOnly"), NoSuchMethodException.class, null);
    }

    /** */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-25084")
    public void testTtlExpirationWorksInReadOnlyMode() throws Exception {
        Ignite grid = startGrid();

        assertTrue(grid.cluster().active());
        //assertFalse(grid.cluster().readOnly());

        //assertCachesReadOnlyMode(grid.cluster().readOnly(), CACHE_NAMES);

        for (String cacheName : CACHE_NAMES) {
            assertEquals(cacheName, 0, grid.cache(cacheName).size());

            for (int i = 0; i < 10; i++)
                grid.cache(cacheName).put(i, i);

            assertEquals(cacheName, 10, grid.cache(cacheName).size());
        }

        //grid.cluster().readOnly(true);
        //assertTrue(grid.cluster().readOnly());

        //assertCachesReadOnlyMode(grid.cluster().readOnly(), CACHE_NAMES);
        //assertDataStreamerReadOnlyMode(grid.cluster().readOnly(), CACHE_NAMES);

        SECONDS.sleep(EXPIRATION_TIMEOUT + 1);

        for (String cacheName : CACHE_NAMES)
            assertEquals(cacheName, 0, grid.cache(cacheName).size());
    }

    /** */
    private static CacheConfiguration[] getCacheConfigurations() {
        CacheConfiguration[] cfgs = cacheConfigurations();

        List<CacheConfiguration> newCfgs = new ArrayList<>(cfgs.length);

        for (CacheConfiguration cfg : cfgs) {
            if (cfg.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT) {
                // Expiry policy cannot be used with TRANSACTIONAL_SNAPSHOT.
                continue;
            }

            cfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(SECONDS, EXPIRATION_TIMEOUT)));
            cfg.setEagerTtl(true);

            newCfgs.add(cfg);
        }

        return newCfgs.toArray(new CacheConfiguration[0]);
    }
}
