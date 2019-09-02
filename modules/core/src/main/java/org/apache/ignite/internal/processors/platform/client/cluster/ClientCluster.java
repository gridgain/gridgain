package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.jetbrains.annotations.Nullable;

/**
 * Client cluster
 *
 */
public class ClientCluster {

    /** Projection. */
    private final IgniteClusterEx prj;

    /**
     * Constructor.
     *
     * @param prj cluster group projection.
     */
    ClientCluster(IgniteClusterEx prj){
        this.prj = prj;
    }

    /**
     * Creates a new cluster group for nodes containing given name and value
     * specified in user attributes.
     * <p>
     *
     * @param name Name of the attribute.
     * @param val Optional attribute value to match.
     * @return Cluster group for nodes containing specified attribute.
     */
    public ClientCluster forAttribute(String name, @Nullable Object val){
        ClusterGroup clusterGrp = prj.forAttribute(name, val);
        return new ClientCluster((IgniteClusterEx) clusterGrp);
    }

    /**
     * Checks Ignite grid is active or not active.
     *
     * @return {@code True} if grid is active. {@code False} If grid is not active.
     */
    public boolean isActive() {
        return prj.active();
    }
}
