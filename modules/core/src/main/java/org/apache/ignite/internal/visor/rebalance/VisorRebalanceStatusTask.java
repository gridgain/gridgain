/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.rebalance;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Rebalance status task.
 */
@GridInternal
@GridVisorManagementTask
public class VisorRebalanceStatusTask extends VisorOneNodeTask<VisorRebalanceStatusTaskArg, VisorRebalanceStatusTaskResult> {
    /** {@inheritDoc} */
    @Override protected VisorJob<VisorRebalanceStatusTaskArg, VisorRebalanceStatusTaskResult> job(
        VisorRebalanceStatusTaskArg arg) {
        return new VisorRebalanceStatusJob(arg, debug);
    }

    private static class VisorRebalanceStatusJob extends VisorJob<VisorRebalanceStatusTaskArg, VisorRebalanceStatusTaskResult> {
        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Auto-inject job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param arg Argumants.
         * @param debug True if debug mode enable.
         */
        protected VisorRebalanceStatusJob(VisorRebalanceStatusTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorRebalanceStatusTaskResult run(@Nullable VisorRebalanceStatusTaskArg arg) throws IgniteException {
            VisorRebalanceStatusTaskResult res = new VisorRebalanceStatusTaskResult();

            boolean isRebalanceProgressing = ignite.context().cache().context().affinity().rebalanceRequired();
            Map<Integer, Set<ClusterNode>> rebGrpNodes = ignite.context().cache().context().affinity().waitGroupNodes();

            Set<ClusterNode> rebNodes = new HashSet<>();

            int minBackups = Integer.MAX_VALUE;

            Map<VisorRebalanceStatusGroupView, Integer> grpViews = new HashMap<>();

            int coutOfServers = ignite.cluster().forServers().nodes().size();

            Map<ClusterNode, List<Integer>> checkedCaches = new HashMap<>();

            for (CacheGroupContext grp : ignite.context().cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

               Set<ClusterNode> grpNodes = rebGrpNodes.get(grp.groupId());

                if (!F.isEmpty(grpNodes))
                    rebNodes.addAll(grpNodes);

                if (grp.config().getCacheMode() != CacheMode.REPLICATED && grp.config().getBackups() < minBackups)
                    minBackups = grp.config().getBackups();

                if (arg.isRebCacheView()) {
                    VisorRebalanceStatusGroupView grpView = new VisorRebalanceStatusGroupView(
                        grp.groupId(),
                        grp.cacheOrGroupName(),
                        F.isEmpty(grpNodes)
                    );

                    int backups = grp.config().getCacheMode() == CacheMode.REPLICATED ? coutOfServers - 1 : grp.config().getBackups();

                    int nodesInRebalance = F.isEmpty(grpNodes) ? 0 : grpNodes.size();

                    grpViews.put(grpView, Math.min(backups, coutOfServers - nodesInRebalance - 1));
                }
            }

            res.setRebalanceComplited(!isRebalanceProgressing);
            res.setRebNodes(rebNodes.stream().map(VisorBaselineNode::new).collect(Collectors.toSet()));
            res.setReplicatedNodeCount(Math.min(minBackups, coutOfServers - rebNodes.size() - 1));
            res.setGroups(grpViews);

            System.out.println("Job context: " + jobCtx);

            return res;
        }
    }

    /**
     * Closure checks, that last checkpoint on applicable for particular groups.
     */
    public static class CheckCpHistClosure implements IgniteClosure<List<Integer>, Map<Integer, Boolean>> {
        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Auto-inject ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<Integer, Boolean> apply(List<Integer> grpIds) {
            IgniteEx igniteEx = (IgniteEx)ignite;

            Map<Integer, Boolean> res = new HashMap<>();

            if (igniteEx.context().cache().context().database() instanceof GridCacheDatabaseSharedManager) {
                CheckpointHistory cpHist = ((GridCacheDatabaseSharedManager)igniteEx.context().cache().context().database()).checkpointHistory();

                CheckpointEntry lastCp = cpHist.lastCheckpoint();

                for (Integer grpId: grpIds) {
                    try {
                        res.put(grpId, cpHist.isCheckpointApplicableForGroup(grpId, lastCp));
                    }
                    catch (IgniteCheckedException e) {
                        log.warning("Can not check checkpoint for a group [grp=" + grpId + ", " +
                            " cp=" + lastCp.checkpointId() + ']', e);

                        res.put(grpId, false);
                    }
                }
            }

            return res;
        }
    }
}
