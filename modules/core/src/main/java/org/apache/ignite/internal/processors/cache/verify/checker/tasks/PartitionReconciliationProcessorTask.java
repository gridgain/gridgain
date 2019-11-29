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

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.verify.checker.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationValueMeta;
import org.apache.ignite.internal.processors.cache.verify.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.resources.IgniteInstanceResource;

// TODO: 21.11.19 Tmp class only for test purposes, will be substituted with Max's class.
// TODO: 19.11.19 Consider extending ComputeTaskSplitAdapter
@GridInternal
public class PartitionReconciliationProcessorTask extends
    ComputeTaskAdapter<VisorPartitionReconciliationTaskArg, PartitionReconciliationResult> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        VisorPartitionReconciliationTaskArg arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        int mgrNodeIdx = ThreadLocalRandom.current().nextInt(subgrid.size());

        jobs.put(new PartitionReconciliationJob(arg), subgrid.get(mgrNodeIdx));

        return jobs;
    }

    @Override public PartitionReconciliationResult reduce(List<ComputeJobResult> results) throws IgniteException {
        // TODO: 22.11.19 Mock
        Map<String, Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>>> inconsistentKeys = new HashMap<>();

        Map<UUID, PartitionReconciliationDataRowMeta> versionsOfInconsistentKeysForKey1 = new HashMap<>();

        Map<UUID, PartitionReconciliationDataRowMeta> versionsOfInconsistentKeysForKey2 = new HashMap<>();

        Map<PartitionReconciliationDataRowMeta, Map<UUID, GridCacheVersion>> keysForCache1 = new HashMap();

        PartitionReconciliationDataRowMeta dataRow1ForCache1 = new PartitionReconciliationDataRowMeta(
            new PartitionReconciliationKeyMeta(new byte[] {1, 2, 3}, "key1",
                new GridCacheVersion(1, 10, 100)),
            new PartitionReconciliationValueMeta(new byte[] {4, 5, 6}, "value1"));

        PartitionReconciliationDataRowMeta dataRow1ForCache2 = new PartitionReconciliationDataRowMeta(
            new PartitionReconciliationKeyMeta(new byte[] {7, 8, 9}, "key2",
                new GridCacheVersion(2, 20, 200)),
            new PartitionReconciliationValueMeta(new byte[] {10, 11, 12}, "value2"));

        versionsOfInconsistentKeysForKey1.put(UUID.randomUUID(), dataRow1ForCache1);
        versionsOfInconsistentKeysForKey1.put(UUID.randomUUID(), dataRow1ForCache2);

        versionsOfInconsistentKeysForKey2.put(UUID.randomUUID(), dataRow1ForCache2);
        versionsOfInconsistentKeysForKey2.put(UUID.randomUUID(), dataRow1ForCache2);

        Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>> partToMeta = new HashMap<>();

        partToMeta.put(1, Collections.singletonList(versionsOfInconsistentKeysForKey1));
        partToMeta.put(10, Collections.singletonList(versionsOfInconsistentKeysForKey1));

        Map<Integer, List<Map<UUID, PartitionReconciliationDataRowMeta>>> partToMeta2 = new HashMap<>();

        List l = new ArrayList();

        l.add(versionsOfInconsistentKeysForKey2);
        l.add(versionsOfInconsistentKeysForKey2);
        l.add(versionsOfInconsistentKeysForKey2);

        partToMeta2.put(11, l);

        inconsistentKeys.put("cache1", partToMeta);

        inconsistentKeys.put("cache2", partToMeta2);

        // Skipped Caches
        Set<PartitionReconciliationSkippedEntityHolder<String>> skippedCaches = new HashSet<>();

        skippedCaches.add(new PartitionReconciliationSkippedEntityHolder<String>("cache_a",
            PartitionReconciliationSkippedEntityHolder.SkippingReason.ENTITY_WITH_TTL));
        skippedCaches.add(new PartitionReconciliationSkippedEntityHolder<String>("cache_b",
            PartitionReconciliationSkippedEntityHolder.SkippingReason.ENTITY_WITH_TTL));

        // Skipped Entities
        Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>>
            skippedEntries = new HashMap<>();

        Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>> partMap =
            new HashMap<>();

        partMap.put(1, Collections.singleton(
            new PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>(
                new PartitionReconciliationKeyMeta(
                    new byte[] {7, 8, 9}, "key2", new GridCacheVersion(2, 20, 200)),
                PartitionReconciliationSkippedEntityHolder.SkippingReason.ENTITY_WITH_TTL)));

        skippedEntries.put("cache_c", partMap);

        return new PartitionReconciliationResult(inconsistentKeys, skippedCaches, skippedEntries);
    }

    /**
     *
     */
    private static class PartitionReconciliationJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /**
         *
         */
        private final VisorPartitionReconciliationTaskArg reconciliationTaskArg;

        /**
         * @param arg
         */
        public PartitionReconciliationJob(VisorPartitionReconciliationTaskArg arg) {
            this.reconciliationTaskArg = arg;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            Collection<String> caches = reconciliationTaskArg.caches() == null || reconciliationTaskArg.caches().isEmpty() ?
                ignite.context().cache().publicCacheNames(): reconciliationTaskArg.caches();

            Object res = new PartitionReconciliationProcessor(
                ignite,
                caches,
                reconciliationTaskArg.fixMode(),
                reconciliationTaskArg.throttlingIntervalMillis(),
                reconciliationTaskArg.batchSize(),
                reconciliationTaskArg.recheckAttempts()
            ).execute();

            return res;
        }
    }
}
