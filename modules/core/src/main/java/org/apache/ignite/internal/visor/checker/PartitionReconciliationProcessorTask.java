package org.apache.ignite.internal.visor.checker;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: 21.11.19 Tmp class only for test purposes, will be subsituted with Max's class.
// TODO: 19.11.19 Consider extending ComputeTaskSplitAdapter
public class PartitionReconciliationProcessorTask extends ComputeTaskAdapter<VisorPartitionReconciliationTaskArg, PartitionReconciliationResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;
    
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        VisorPartitionReconciliationTaskArg arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new JobProvidedByMaxThatDoAllWork(), node);

        return jobs;
    }

    @Override public PartitionReconciliationResult reduce(List<ComputeJobResult> results) throws IgniteException {
        // TODO: 21.11.19 Will be substituted with Max's code.
        CacheObjectContext cotx = ignite.context().cache().context().cacheContext(GridCacheUtils.cacheId("Cache123")).cacheObjectContext();

        for (ComputeJobResult r: results) {
            Map<String, List<KeyCacheObject>> res = (Map) r.getData();

            for (String cacheName: res.keySet()) {
                for (KeyCacheObject k: res.get(cacheName)){
                    try {
                        k.finishUnmarshal(cotx, null);

                        System.out.println("<<<<<<<<<<< " + k);
                    }
                    catch (IgniteCheckedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return null;
    }
}
