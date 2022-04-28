package org.apache.ignite.internal.visor.dr;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.logMapped;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;

public abstract class VisorDrPartitionCountersTask<K, V, J> extends VisorMultiNodeTask<K, V, J> {
    protected abstract Set<String> getCaches(K args);

    protected abstract VisorJob<K, J> createJob(K args, Map<String, Set<Integer>> cachePartsMap, boolean debug);

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
}
