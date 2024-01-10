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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder.SkippingReason;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.RECONCILIATION_DIR;

public class PartitionReconciliationLostPartitionsTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setMaxSize(300L * 1024 * 1024))
        );

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 12));
        ccfg.setBackups(0);
        ccfg.setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), RECONCILIATION_DIR, false));
    }

    /**
     * Tests partition reconciliation in the case when a verifying cache has lost partitions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLostPartitions() throws Exception {
        // Start cluster and fill the cache.
        startGridsMultiThreaded(NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        // Stop node to get lost partitions.
        stopGrid(1);

        Set<Integer> lostParts = grid(0).cachex(DEFAULT_CACHE_NAME).context().topology().lostPartitions();

        assertFalse("There are no lost partitions!", lostParts.isEmpty());

        ReconciliationResult res = partitionReconciliation(
            grid(0),
            new VisorPartitionReconciliationTaskArg.Builder()
                .locOutput(true)
                .repair(false)
                .caches(Collections.singleton(DEFAULT_CACHE_NAME)));

        ReconciliationResult compactRes = partitionReconciliation(
            grid(0),
            new VisorPartitionReconciliationTaskArg.Builder()
                .locOutput(false)
                .repair(false)
                .caches(Collections.singleton(DEFAULT_CACHE_NAME)));

        assertEquals(
            "Number of lost partitions does not match to the number of skipped entities.",
            res.partitionReconciliationResult().skippedEntriesCount(),
            lostParts.size());

        assertEquals(
            "Number of lost partitions does not match to the number of skipped entities (compact mode).",
            compactRes.partitionReconciliationResult().skippedEntriesCount(),
            lostParts.size());

        Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>> entries =
            res.partitionReconciliationResult().skippedEntries();

        Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>> cacheEntries
            = entries.get(DEFAULT_CACHE_NAME);

        assertNotNull("Failed to detect skipped entities (partitions).", cacheEntries);

        for (Integer partId : lostParts) {
            Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>> hld = cacheEntries.get(partId);

            assertNotNull("Failed to detect find lost partition [partId=" + partId + ']', hld);
            assertEquals("Unexpected number of skipped partitions [lhd=" + hld.size() + ']', 1, hld.size());

            SkippingReason reason = hld.iterator().next().skippingReason();
            assertEquals("Unexpected reason [reason=" + reason + ']', SkippingReason.LOST_PARTITION, reason);
        }
    }
}
