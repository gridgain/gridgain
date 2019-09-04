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

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.jetbrains.annotations.Nullable;

/**
 * Client cluster.
 */
public class ClientCluster {

    /** Projection. */
    private final IgniteCluster prj;

    /**
     * Constructor.
     *
     * @param prj cluster group projection.
     */
    ClientCluster(IgniteCluster prj){
        this.prj = prj;
    }

    /**
     * Creates a new cluster group for nodes containing given name and value
     * specified in user attributes.
     *
     * @param name Name of the attribute.
     * @param val Optional attribute value to match.
     * @return Cluster group for nodes containing specified attribute.
     */
    public ClientCluster forAttribute(String name, @Nullable Object val){
        ClusterGroup clusterGrp = prj.forAttribute(name, val);
        IgniteCluster cluster = clusterGrp.ignite().cluster();
        return new ClientCluster(cluster);
    }

    /**
     * Checks Ignite grid is active or not active.
     *
     * @return {@code True} if grid is active. {@code False} If grid is not active.
     */
    public boolean isActive() {
        return prj.active();
    }

    /**
     * Changes Ignite grid state to active or inactive.
     *
     * @param isActive If {@code True} start activation process. If {@code False} start deactivation process.
     * @throws IgniteException If there is an already started transaction or lock in the same thread.
     */
    public void changeGridState(boolean isActive){
        prj.active(isActive);
    }

    /**
     * Enables write-ahead logging for specified cache. Restoring crash-recovery guarantees of a previous call to
     * {@link #disableWal(String)}.
     * WAL state can be changed only for persistent caches.
     *
     * @param cacheName Cache name.
     * @return Whether WAL enabled by this call.
     * @throws IgniteException If error occurs.
     */
    public boolean enableWal(String cacheName) throws IgniteException {
        return prj.enableWal(cacheName);
    }

    /**
     * Disables write-ahead logging for specified cache. When WAL is disabled, changes are not logged to disk.
     * This significantly improves cache update speed. The drawback is absence of local crash-recovery guarantees.
     * If node is crashed, local content of WAL-disabled cache will be cleared on restart to avoid data corruption.
     *
     * @param cacheName Cache name.
     * @return Whether WAL disabled by this call.
     * @throws IgniteException If error occurs.
     */
    public boolean disableWal(String cacheName) throws IgniteException{
        return prj.disableWal(cacheName);
    }

    /**
     * Checks if write-ahead logging is enabled for specified cache.
     *
     * @param cacheName Cache name.
     * @return {@code True} if WAL is enabled for cache.
     */
    public boolean isWalEnabled(String cacheName){
        return prj.isWalEnabled(cacheName);
    }
}
