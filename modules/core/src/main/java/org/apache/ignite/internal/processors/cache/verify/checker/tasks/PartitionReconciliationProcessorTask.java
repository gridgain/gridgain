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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor;
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
        assert results.size() == 1;

        return results.get(0).getData();
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
        @Override public PartitionReconciliationResult execute() throws IgniteException {
            Collection<String> caches = reconciliationTaskArg.caches() == null || reconciliationTaskArg.caches().isEmpty() ?
                ignite.context().cache().publicCacheNames(): reconciliationTaskArg.caches();

            try {
                return new PartitionReconciliationProcessor(
                    ignite,
                    ignite.context().cache().context().exchange(),
                    caches,
                    reconciliationTaskArg.fixMode(),
                    reconciliationTaskArg.throttlingIntervalMillis(),
                    reconciliationTaskArg.batchSize(),
                    reconciliationTaskArg.recheckAttempts()
                ).execute();
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
                return null; //TODO
            }
        }
    }
}
