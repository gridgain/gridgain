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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetItemKey;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;

/**
 * Tests that reconciliation works with data collections.
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationSetDataStructureTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 4;

    /** Keys count. */
    protected static final int KEYS_CNT = 100;

    /** Corrupted keys count. */
    protected static final int BROKEN_KEYS_CNT = 10;

    /** Data structure name. */
    protected static final String DS_NAME = "DefaultDS";

    /** Cache atomicity mode. */
    @Parameterized.Parameter(0)
    public CacheAtomicityMode cacheAtomicityMode;

    /** Fix mode. */
    @Parameterized.Parameter(1)
    public boolean fixMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(300L * 1024 * 1024))
        );

        cfg.setConsistentId(name);
        cfg.setAutoActivationEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**  */
    @Parameterized.Parameters(name = "atomicity = {0}, repair = {1}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[]{
            CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        for (CacheAtomicityMode atomicityMode : atomicityModes) {
            params.add(new Object[]{atomicityMode, false});
            params.add(new Object[]{atomicityMode, true});
        }

        return params;
    }

    /**
     * Tests that reconciliation works with data structures using cache name explicitly.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testReconciliationOfIgniteSetExplicitly() throws Exception {
        runIgniteSetReconciliation(true);
    }

    /**
     * Tests that reconciliation works with internal caches.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testReconciliationOfInternalCaches() throws Exception {
        runIgniteSetReconciliation(false);
    }

    /**
     * Run Ignite set reconciliation.
     * @param useCacheName use cache name explicitly.
     * @throws Exception if failed.
     */
    private void runIgniteSetReconciliation(boolean useCacheName) throws Exception {
        String cacheName = null;

        IgniteEx ig = startGrids(NODES_CNT);

        IgniteEx client = startClientGrid(NODES_CNT);

        client.cluster().active(true);

        CollectionConfiguration cfg = new CollectionConfiguration().setBackups(NODES_CNT - 1);
        cfg.setAtomicityMode(cacheAtomicityMode);

        IgniteSet<Integer> set = client.set(DS_NAME, cfg);

        for (int i = 0; i < KEYS_CNT; i++)
            set.add(i);

        log.info(">>>> Initial data loading finished");

        int firstBrokenKey = KEYS_CNT / 2;

        GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++) {
            Collection<IgniteInternalCache<?, ?>> ds_name = grid(i).cachesx(p -> p.name().contains(DS_NAME));
            GridCacheContext<?, ?> ctx = ds_name.iterator().next().context();
            nodeCacheCtxs[i] = ctx;
            cacheName = ctx.name();
        }

        Constructor<GridCacheSetItemKey> ctor = getGridCacheSetItemKeyCtor();
        for (int i = firstBrokenKey; i < firstBrokenKey + BROKEN_KEYS_CNT; i++) {
            Object brokenKey = ctor.newInstance(null, i);
            if (i % 3 == 0)
                simulateMissingEntryCorruption(nodeCacheCtxs[i % NODES_CNT], brokenKey);
            else
                simulateOutdatedVersionCorruption(nodeCacheCtxs[i % NODES_CNT], brokenKey);
        }

        forceCheckpoint();

        log.info(">>>> Simulating data corruption finished");

        stopAllGrids();

        ig = startGrids(NODES_CNT);

        ig.cluster().active(true);

        String[] cacheNameToRepair = useCacheName ? new String[]{cacheName} : new String[0];
        ReconciliationResult res = partitionReconciliation(ig, fixMode, RepairAlgorithm.PRIMARY, 4, cacheNameToRepair);

        log.info(">>>> Partition reconciliation finished");

        Set<PartitionReconciliationKeyMeta> conflictKeyMetas = conflictKeyMetas(res, cacheName);

        assertEquals(BROKEN_KEYS_CNT, conflictKeyMetas.size());

        for (int i = firstBrokenKey; i < firstBrokenKey + BROKEN_KEYS_CNT; i++) {
            boolean keyMatched = false;

            for (PartitionReconciliationKeyMeta keyMeta : conflictKeyMetas) {
                if (keyMeta.stringView(true).contains("item=" + String.valueOf(i)))
                    keyMatched = true;
            }

            assertTrue(
                "Unmatched key: " + i + ", got conflict key metas: " +
                    conflictKeyMetas.stream().map(m -> m.stringView(true)).reduce((s1, s2) -> s1 + ", " + s2).get(),
                keyMatched
            );
        }
    }

    /** */
    private static Constructor<GridCacheSetItemKey> getGridCacheSetItemKeyCtor() throws ClassNotFoundException {
        Class<?> aCls = Class.forName(GridCacheSetItemKey.class.getCanonicalName());
        for (Constructor<?> constructor : aCls.getDeclaredConstructors()) {
            constructor.setAccessible(true);
            Class<?>[] types = constructor.getParameterTypes();
            if (types.length == 2 && types[0] == IgniteUuid.class && types[1] == Object.class)
                return (Constructor<GridCacheSetItemKey>)constructor;
        }

        throw new NullPointerException("Unable to locate constructor of type " + GridCacheSetItemKey.class +
            "with arguments: [" + IgniteUuid.class + ", " + Object.class + "]");
    }
}
