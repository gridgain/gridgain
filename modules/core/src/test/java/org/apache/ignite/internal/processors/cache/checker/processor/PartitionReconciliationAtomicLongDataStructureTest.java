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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;

/**
 * Tests that reconciliation works with data structures.
 */
@RunWith(Parameterized.class)
public class PartitionReconciliationAtomicLongDataStructureTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 4;

    /** Keys count. */
    protected static final int KEYS_CNT = 100;

    /** Corrupted keys count. */
    protected static final int BROKEN_KEYS_CNT = 10;

    /** Data structure name. */
    protected static final String DS_NAME = "DefaultDS";

    /** Fix mode. */
    @Parameterized.Parameter(0)
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

    /**
     *
     */
    @Parameterized.Parameters(name = "repair = {0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();
        params.add(new Object[] {false});
        params.add(new Object[] {true});
        return params;
    }

    /**
     * Tests that reconciliation works with atomic long.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testReconciliationOfAtomicLong() throws Exception {
        String cacheName = "ignite-sys-atomic-cache@default-ds-group";

        IgniteEx ig = startGrids(NODES_CNT);

        IgniteEx client = startClientGrid(NODES_CNT);

        client.cluster().active(true);

        AtomicConfiguration cfg = new AtomicConfiguration().setBackups(NODES_CNT - 1);

        for (int i = 0; i < KEYS_CNT; i++)
            client.atomicLong(DS_NAME + i, cfg, 1, true);

        log.info(">>>> Initial data loading finished");

        int firstBrokenKey = KEYS_CNT / 2;

        GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++)
            nodeCacheCtxs[i] = grid(i).cachex(cacheName).context();

        for (int i = firstBrokenKey; i < firstBrokenKey + BROKEN_KEYS_CNT; i++) {
            GridCacheInternalKeyImpl brokenKey = new GridCacheInternalKeyImpl(DS_NAME + i, "default-ds-group");
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

        ReconciliationResult res = partitionReconciliation(ig, fixMode, RepairAlgorithm.PRIMARY, 4, cacheName);

        log.info(">>>> Partition reconciliation finished");

        Set<PartitionReconciliationKeyMeta> conflictKeyMetas = conflictKeyMetas(res, cacheName);

        assertEquals(BROKEN_KEYS_CNT, conflictKeyMetas.size());

        for (int i = firstBrokenKey; i < firstBrokenKey + BROKEN_KEYS_CNT; i++) {
            boolean keyMatched = false;

            for (PartitionReconciliationKeyMeta keyMeta : conflictKeyMetas) {
                if (keyMeta.stringView(true).contains(DS_NAME + String.valueOf(i)))
                    keyMatched = true;
            }

            assertTrue(
                "Unmatched key: " + i + ", got conflict key metas: " +
                    conflictKeyMetas.stream().map(m -> m.stringView(true)).reduce((s1, s2) -> s1 + ", " + s2).get(),
                keyMatched
            );
        }

        if (fixMode)
            assertFalse(idleVerify(ig, cacheName).hasConflicts());
    }
}
