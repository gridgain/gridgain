package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;

/**
 *
 */
public class LocalAffinityFunction implements AffinityFunction {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     * {@inheritDoc}
     */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        ClusterNode locNode = null;

        for (ClusterNode n : affCtx.currentTopologySnapshot()) {
            if (n.isLocal()) {
                locNode = n;

                break;
            }
        }

        if (locNode == null)
            throw new IgniteException("Local node is not included into affinity nodes for 'LOCAL' cache");

        List<List<ClusterNode>> res = new ArrayList<>(partitions());

        for (int part = 0; part < partitions(); part++)
            res.add(Collections.singletonList(locNode));

        return Collections.unmodifiableList(res);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void reset() {
        // No-op.
    }

    /**
     * {@inheritDoc}
     */
    @Override public int partitions() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override public int partition(Object key) {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }
}