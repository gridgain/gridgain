/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.dr;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.logMapped;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;

/**
 * Abstract dr partition counters task. Contains map/reduce methods implementation.
 */
public abstract class VisorDrPartitionCountersTask<K, V, J> extends VisorMultiNodeTask<K, V, J> {
    /**
     * Gets caches.
     *
     * @param args Task args.
     * @return Set of cache or cache group names.
     */
    protected abstract Set<String> getCaches(K args);

    /**
     * Creates job from task args and cache-partitions map.
     *
     * @param args Task args.
     * @param cachePartsMap Cache-partition map, contains caches and partitions assigned for the node.
     * @param debug Debug flag.
     * @return VisorJob job.
     */
    protected abstract VisorJob<K, J> createJob(K args, Map<String, Set<Integer>> cachePartsMap, boolean debug);

    /**
     * Creates task result from exceptions map and results map.
     *
     * @param exceptions Node-exception map.
     * @param results Node-result map.
     * @return Task result.
     */
    protected abstract V createResult(Map<UUID, Exception> exceptions, Map<UUID, J> results);

    /** {@inheritDoc} */
    @Override protected VisorJob<K, J> job(K arg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid, VisorTaskArgument<K> arg) {
        K argument = arg.getArgument();

        Set<String> caches = getCaches(argument);

        if (caches == null || caches.isEmpty())
            caches = ignite.context().cache().cacheDescriptors().entrySet()
                    .stream().filter(e -> e.getValue().cacheType() == CacheType.USER)
                    .map(Entry::getKey).collect(Collectors.toSet());

        caches.forEach(ignite::cache);

        Set<Integer> groups = new HashSet<>();

        List<GridCacheContext> contexts = caches.stream()
                .map(name -> ignite.context().cache().cache(name).context())
                .filter(cctx -> groups.add(cctx.groupId()))
                .collect(Collectors.toList());

        Map<ClusterNode, Map<String, Set<Integer>>> nodeCachePartsMap = new HashMap<>();

        for (GridCacheContext cctx : contexts) {
            int parts = cctx.affinity().partitions();

            AffinityAssignment assignment = cctx.affinity()
                    .assignment(cctx.affinity().affinityTopologyVersion());

            for (int p = 0; p < parts; p++) {
                Collection<ClusterNode> nodes = ignite.cluster()
                        .forNodes(assignment.assignment().get(p)).nodes();

                for (ClusterNode node : nodes) {
                    String cache = cctx.group().cacheOrGroupName();

                    nodeCachePartsMap
                            .computeIfAbsent(node, n -> new HashMap<>())
                            .computeIfAbsent(cache, c -> new HashSet<>()).add(p);
                }
            }
        }

        Map<ComputeJob, ClusterNode> map = new HashMap<>();

        for (ClusterNode clusterNode : nodeCachePartsMap.keySet()) {
            Map<String, Set<Integer>> cachePartsMap = nodeCachePartsMap.get(clusterNode);
            map.put(createJob(argument, cachePartsMap, debug), clusterNode);
        }

        try {
            if (map.isEmpty())
                ignite.log().warning(NO_SUITABLE_NODE_MESSAGE + ": [task=" + getClass().getName() +
                        ", topVer=" + ignite.cluster().topologyVersion() +
                        ", subGrid=" + U.toShortString(subgrid) + "]");

            return map;
        }
        finally {
            if (debug)
                logMapped(ignite.log(), getClass(), map.values());
        }
    }

    /** {@inheritDoc} */
    @Override protected V reduce0(List<ComputeJobResult> results)
            throws IgniteException {
        Map<UUID, J> nodeMetricsMap = new HashMap<>();
        Map<UUID, Exception> exceptions = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                exceptions.put(res.getNode().id(), res.getException());
            else {
                J metrics = res.getData();

                nodeMetricsMap.put(res.getNode().id(), metrics);
            }
        }

        return createResult(exceptions, nodeMetricsMap);
    }
}
