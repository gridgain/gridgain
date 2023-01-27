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

import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Attribute validation self test.
 */
public class GridCacheConfigurationValidationSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** */
    private static final String WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME = "preloadModeCheckFails";

    /** */
    private static final String WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME = "cacheModeCheckFails";

    /** */
    private static final String WRONG_AFFINITY_IGNITE_INSTANCE_NAME = "cacheAffinityCheckFails";

    /** */
    private static final String WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME = "cacheAffinityMapperCheckFails";

    /** */
    private static final String DUP_CACHES_IGNITE_INSTANCE_NAME = "duplicateCachesCheckFails";

    /** */
    private static final String DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME = "duplicateDefaultCachesCheckFails";

    /** */
    private static final String RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME =
        "reservedForDsCacheNameCheckFails";

    /** */
    private static final String RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME =
            "reservedForDsCacheGroupNameCheckFails";

    /** */
    private static final String RESERVED_FOR_VOLATILE_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME =
        "reservedForVolatileDsCacheGroupNameCheckFails";

    /** */
    private static final String CACHE_NAME_WITH_SPECIAL_CHARACTERS_REPLICATED = "--№=+:(replicated)";

    /** */
    private static final String CACHE_NAME_WITH_SPECIAL_CHARACTERS_PARTITIONED = ":_&:: (partitioned)";

    /** */
    private static final String REGION_NAME = "test";

    /** */
    private static final String DEFAULT_CACHE_GROUP_NAME = DEFAULT_CACHE_NAME + "_grp";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDataRegionConfigurations(
                    new DataRegionConfiguration().setName(REGION_NAME)
                )
            );

        // Default cache config.
        CacheConfiguration<?, ?> dfltCacheCfg = defaultCacheConfiguration()
            .setCacheMode(PARTITIONED)
            .setRebalanceMode(ASYNC)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction())
            .setIndexedTypes(Integer.class, String.class)
            .setName(CACHE_NAME_WITH_SPECIAL_CHARACTERS_PARTITIONED);

        // Non-default cache configuration.
        CacheConfiguration<?, ?> namedCacheCfg = defaultCacheConfiguration()
            .setCacheMode(PARTITIONED)
            .setRebalanceMode(ASYNC)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction());

        // Local cache configuration.
        CacheConfiguration<?, ?> localCacheCfg = defaultCacheConfiguration()
            .setCacheMode(LOCAL);

        // Modify cache config according to test parameters.
        if (igniteInstanceName.contains(WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setRebalanceMode(SYNC);
        else if (igniteInstanceName.contains(WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setCacheMode(REPLICATED);
        else if (igniteInstanceName.contains(WRONG_AFFINITY_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setAffinity(new TestRendezvousAffinityFunction());
        else if (igniteInstanceName.contains(WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setAffinityMapper(new TestCacheDefaultAffinityKeyMapper());

        if (igniteInstanceName.contains(DUP_CACHES_IGNITE_INSTANCE_NAME))
            cfg.setCacheConfiguration(namedCacheCfg, namedCacheCfg);
        else if (igniteInstanceName.contains(DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME))
            cfg.setCacheConfiguration(dfltCacheCfg, dfltCacheCfg);
        else {
            // Normal configuration.
            if (client)
                cfg.setClientMode(true);
            else
                cfg.setCacheConfiguration(dfltCacheCfg, namedCacheCfg, localCacheCfg);
        }

        if (igniteInstanceName.contains(RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME))
            namedCacheCfg.setName(DataStructuresProcessor.ATOMICS_CACHE_NAME + "@abc");
        else {
            namedCacheCfg.setCacheMode(REPLICATED);
            namedCacheCfg.setName(CACHE_NAME_WITH_SPECIAL_CHARACTERS_REPLICATED);
        }

        if (igniteInstanceName.contains(RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME))
            namedCacheCfg.setGroupName("default-ds-group");

        if (igniteInstanceName.contains(RESERVED_FOR_VOLATILE_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME))
            namedCacheCfg.setGroupName("default-volatile-ds-group@volatileDsMemPlc");

        return cfg;
    }

    /**
     * This test method does not require remote nodes.
     */
    @Test
    public void testDuplicateCacheConfigurations() {
        // This grid should not start.
        startInvalidGrid(DUP_CACHES_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testCacheAttributesValidation() throws Exception {
        startGrid(0);

        // This grid should not start.
        startInvalidGrid(WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(WRONG_AFFINITY_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(RESERVED_FOR_VOLATILE_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME);

        // This grid will start normally.
        startGrid(1);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testCacheNames() throws Exception {
        startGridsMultiThreaded(2);

        Collection<String> names = grid(0).cacheNames();

        assertEquals(3, names.size());

        for (String name : names)
            assertTrue(name.equals(CACHE_NAME_WITH_SPECIAL_CHARACTERS_PARTITIONED)
                || name.equals(CACHE_NAME_WITH_SPECIAL_CHARACTERS_REPLICATED)
                || DEFAULT_CACHE_NAME.equals(name));

        client = true;

        Ignite client = startGrid(2);

        names = client.cacheNames();

        assertEquals(3, names.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCachesOfSameGroupButWithDifferentRegions() throws Exception {
        IgniteEx server = startGrid(0);

        server.cluster().state(ACTIVE);

        client = true;

        IgniteEx client = startGrid(1);

        awaitPartitionMapExchange();

        client.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME + 0)
                .setGroupName(DEFAULT_CACHE_GROUP_NAME)
                .setBackups(1)
                .setCacheMode(REPLICATED)
        );

        CacheConfiguration<Object, Object> cacheCfg0 = new CacheConfiguration<>(DEFAULT_CACHE_NAME + 1)
            .setGroupName(DEFAULT_CACHE_GROUP_NAME)
            .setDataRegionName(REGION_NAME)
            .setBackups(1)
            .setCacheMode(PARTITIONED);

        CacheConfiguration<Object, Object> cacheCfg1 = new CacheConfiguration<>(DEFAULT_CACHE_NAME + 1)
            .setGroupName(DEFAULT_CACHE_GROUP_NAME)
            .setDataRegionName(REGION_NAME)
            .setBackups(1)
            .setCacheMode(REPLICATED);

        assertThrows(
            log,
            () -> client.getOrCreateCache(cacheCfg0),
            IgniteCheckedException.class,
            "Cache mode mismatch for caches related to the same group"
        );

        assertThrows(
            log,
            () -> server.getOrCreateCache(cacheCfg0),
            IgniteCheckedException.class,
            "Cache mode mismatch for caches related to the same group"
        );

        assertThrows(
            log,
            () -> client.getOrCreateCache(cacheCfg1),
            CacheException.class,
            "Data region mismatch for caches related to the same group"
        );

        assertThrows(
            log,
            () -> server.getOrCreateCache(cacheCfg1),
            CacheException.class,
            "Data region mismatch for caches related to the same group"
        );
    }

    /**
     * Starts grid that will fail to start due to invalid configuration.
     *
     * @param name Name of the grid which will have invalid configuration.
     */
    private void startInvalidGrid(String name) {
        assertThrows(log, () -> startGrid(name), Exception.class, null);
    }

    /**
     *
     */
    private static class TestRendezvousAffinityFunction extends RendezvousAffinityFunction {
        // No-op. Just to have another class name.

        /**
         * Empty constructor required by Externalizable.
         */
        public TestRendezvousAffinityFunction() {
            // No-op.
        }
    }

    /**
     *
     */
    private static class TestCacheDefaultAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper {
        // No-op. Just to have another class name.
    }
}
