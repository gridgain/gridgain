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

package org.apache.ignite.internal.client;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterState;

/**
 *  Interface for manage state of grid cluster and obtain information about it: ID and tag.
 */
public interface GridClientClusterState {
    /**
     * @param active {@code True} activate, {@code False} deactivate.
     * @deprecated Use {@link #state()} instead.
     */
    @Deprecated
    public void active(boolean active) throws GridClientException;

    /**
     * @return {@code Boolean} - Current cluster state. {@code True} active, {@code False} inactive.
     * @deprecated Use {@link #state(ClusterState)} instead.
     */
    @Deprecated
    public boolean active() throws GridClientException;

    /**
     * @return Current cluster state.
     * @throws GridClientException If the request to get the cluster state failed.
     */
    public ClusterState state() throws GridClientException;

    /**
     * Changes cluster state to {@code newState}.
     *
     * @param newState New cluster state.
     * @param forceDeactivation If {@code true}, cluster deactivation will be forced, this flag makes sense only for
     * {@link ClusterState#INACTIVE} transition.
     * @throws GridClientException If the request to change the cluster state failed.
     * @see ClusterState#INACTIVE
     */
    public void state(ClusterState newState, boolean forceDeactivation) throws GridClientException;

    /**
     * Changes cluster state to {@code newState}.
     *
     * @param newState New cluster state.
     * @throws GridClientException If the request to change the cluster state failed.
     * @see ClusterState#INACTIVE
     */
    public void state(ClusterState newState) throws GridClientException;

    /**
     * Unique identifier of cluster STATE command was sent to.
     *
     * @return ID of the cluster.
     */
    public UUID id() throws GridClientException;

    /**
     * User-defined tag of cluster STATE command was sent to.
     *
     * @return Tag of the cluster.
     */
    public String tag() throws GridClientException;

    /**
     * Get the cluster name.
     *
     * @return The name of the cluster.
     * @throws GridClientException If the request to get the cluster name failed.
     * */
    String clusterName() throws GridClientException;
}
