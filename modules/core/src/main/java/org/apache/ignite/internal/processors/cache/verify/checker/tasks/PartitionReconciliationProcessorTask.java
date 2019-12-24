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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.createLocalResultFile;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.parallelismLevel;

@GridInternal
public class PartitionReconciliationProcessorTask extends
    ComputeTaskAdapter<VisorPartitionReconciliationTaskArg, ReconciliationResult> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        VisorPartitionReconciliationTaskArg arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        LocalDateTime startTime = LocalDateTime.now();

        for (ClusterNode node : subgrid)
            jobs.put(new PartitionReconciliationJob(arg, startTime), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public ReconciliationResult reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<UUID, String> nodeIdToFolder = new HashMap<>();
        PartitionReconciliationResult res = new PartitionReconciliationResult();

        for (ComputeJobResult result : results) {
            UUID nodeId = result.getNode().id();
            T2<String, PartitionReconciliationResult> data = result.getData();

            nodeIdToFolder.put(nodeId, data.get1());
            res.merge(data.get2());
        }

        return new ReconciliationResult(res, nodeIdToFolder);
    }

    /**
     *
     */
    private static class PartitionReconciliationJob extends ComputeJobAdapter {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         *
         */
        private final VisorPartitionReconciliationTaskArg reconciliationTaskArg;

        /**
         *
         */
        private final LocalDateTime startTime;

        /**
         * @param arg
         */
        public PartitionReconciliationJob(VisorPartitionReconciliationTaskArg arg, LocalDateTime startTime) {
            this.reconciliationTaskArg = arg;
            this.startTime = startTime;
        }

        /** {@inheritDoc} */
        @Override public T2<String, PartitionReconciliationResult> execute() throws IgniteException {
            Collection<String> caches = reconciliationTaskArg.caches() == null || reconciliationTaskArg.caches().isEmpty() ?
                ignite.context().cache().publicCacheNames() : reconciliationTaskArg.caches();

            try {
                PartitionReconciliationResult reconciliationRes = new PartitionReconciliationProcessor(
                    ignite,
                    ignite.context().cache().context().exchange(),
                    caches,
                    reconciliationTaskArg.fixMode(),
                    parallelismLevel(reconciliationTaskArg.loadFactor(), caches, ignite),
                    reconciliationTaskArg.batchSize(),
                    reconciliationTaskArg.recheckAttempts(),
                    reconciliationTaskArg.repairAlg()
                ).execute();

                File file = null;

                if (reconciliationRes != null && !reconciliationRes.isEmpty()) {
                    file = createLocalResultFile(ignite.context().discovery().localNode(), startTime);

                    try (PrintWriter pw = new PrintWriter(file)) {
                        reconciliationRes.print(pw::write, reconciliationTaskArg.verbose());

                        pw.flush();
                    }
                    catch (IOException e) {
                        log.error("Unable to write report to file " + e.getMessage());
                        //TODO
                    }
                }

                return new T2<>(
                    file == null ? null : file.getAbsolutePath(),
                    reconciliationTaskArg.console() ? reconciliationRes : new PartitionReconciliationResult()
                );
            }
            catch (IgniteCheckedException | IOException e) {
                e.printStackTrace();
                return null; //TODO
            }
        }
    }
}
