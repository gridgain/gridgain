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


package org.apache.ignite.internal.visor.cache.affinityView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinitySharedManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Task for fetching affinity assignment for cache group.
 */
@GridInternal
public class VisorAffinityViewTask extends VisorOneNodeTask<VisorAffinityViewTaskArg, VisorAffinityViewTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Job for fetching affinity assignment from custer
     */
    private static class VisorAffinityViewJob extends VisorJob<VisorAffinityViewTaskArg, VisorAffinityViewTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug flag.
         */
        VisorAffinityViewJob(VisorAffinityViewTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorAffinityViewTaskResult run(@Nullable VisorAffinityViewTaskArg arg) throws IgniteException {
            if (arg == null)
                throw new IgniteException("VisorAffinityViewTaskArg is null");

            IgniteEx affSrc = getAffinitySrc(arg.getAffinitySrcNodeId());

            CacheAffinitySharedManager affinitySharedManager = affSrc.context()
                                              .cache()
                                              .context()
                                              .affinity();

            AffinityAssignment affAss;

            int gropuId = CU.cacheId(arg.getCacheGrpName());

            if (affinitySharedManager.cacheGroupExists(gropuId))
                affAss = affinitySharedManager.affinity(gropuId).cachedAffinity(AffinityTopologyVersion.NONE);
            else
                throw new IgniteException("Group " + arg.getCacheGrpName() + " not found");

            VisorAffinityViewTaskArg.Mode mode = arg.getMode();

            switch (mode) {
                case CURRENT:
                    return new VisorAffinityViewTaskResult(assignmentAsMap(affAss.assignment()), Collections.emptySet());

                case IDEAL:
                    return new VisorAffinityViewTaskResult(assignmentAsMap(affAss.idealAssignment()), Collections.emptySet());

                case DIFF:
                    return new VisorAffinityViewTaskResult(Collections.emptyMap(), affAss.partitionPrimariesDifferentToIdeal());

                default:
                    throw new IgniteException("Unexpected mode: " + mode);
            }
        }

        /**
         * Gets affinity source.
         *
         * @param srcNodeId source node id.
         * @return {@code IgniteEx} corresponding to {@code srcNodeId} if
         * it is not null and auto-injected grid instance otherwise.
         */
        private IgniteEx getAffinitySrc(@Nullable UUID srcNodeId) {
            IgniteEx affSrc;

            if (srcNodeId != null) {
                IgniteEx affSrcFromNode = IgnitionEx.gridxx(srcNodeId);

                if (affSrcFromNode != null)
                    affSrc = affSrcFromNode;
                else
                    affSrc = ignite;
            }
            else
                affSrc = ignite;

            return affSrc;
        }
    }

    /**
     * Converts affinity assignment to map having {@code ClusterNode} as key
     * and {@code IgniteBiTuple} with partitions ids as value.
     * First val of {@code IgniteBiTuple} contains primary partitions ids,
     * second val contains backup partitions ids.
     *
     * @param assignment Affinity assignment.
     * @return Converted assignment.
     */
    private static Map<ClusterNode, IgniteBiTuple<char[], char[]>> assignmentAsMap(List<List<ClusterNode>> assignment) {
        Map<ClusterNode, IgniteBiTuple<List<Integer>, List<Integer>>> nodeMap = new HashMap<>();

        for (int partNum = 0; partNum < assignment.size(); partNum++) {
            List<ClusterNode> nodes = assignment.get(partNum);

            if (nodes.isEmpty())
                continue;

            // Primary partition id processing.
            ClusterNode primNode = nodes.get(0);

            if (!nodeMap.containsKey(primNode)) {
                List<Integer> primList = new ArrayList<>();
                primList.add(partNum);

                IgniteBiTuple<List<Integer>, List<Integer>> partTuple = new IgniteBiTuple<>();
                partTuple.set(primList, new ArrayList<>());

                nodeMap.put(primNode, partTuple);
            } else
                nodeMap.get(primNode).get1().add(partNum);

            // Backup partitions ids processing.
            for (int nodeNum = 1; nodeNum < nodes.size(); nodeNum++) {
                ClusterNode bakNode = nodes.get(nodeNum);

                if (!nodeMap.containsKey(bakNode)) {
                    List<Integer> bakList = new ArrayList<>();
                    bakList.add(partNum);

                    IgniteBiTuple<List<Integer>, List<Integer>> partTuple = new IgniteBiTuple<>();
                    partTuple.set(new ArrayList<>(), bakList);

                    nodeMap.put(bakNode, partTuple);
                } else
                    nodeMap.get(bakNode).get2().add(partNum);
            }
        }

        Map<ClusterNode, IgniteBiTuple<char[], char[]>> res = new HashMap<>();

        nodeMap.forEach((node, tuple) -> {
            IgniteBiTuple<char[], char[]> charTuple = new IgniteBiTuple<>();

            char[] primIds = toCharArray(tuple.get1());
            char[] backIds = toCharArray(tuple.get2());

            charTuple.set(primIds, backIds);

            res.put(node, charTuple);
        });

        return res;
    }

    /** */
    private static char[] toCharArray(List<Integer> partIdList) {
        char[] res = new char[partIdList.size()];

        for(int i = 0; i < partIdList.size(); i++)
            res[i] = (char)partIdList.get(i).intValue();

        return res;
    }

    /** {@inheritDoc} */
    @Override protected VisorAffinityViewJob job(VisorAffinityViewTaskArg arg) {
        return new VisorAffinityViewJob(arg, debug);
    }
}
