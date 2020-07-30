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

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.cluster.ClusterState;

/**
 *
 */
public class GridRestClusterStateRequest extends GridRestRequest {
    /** Request current state. */
    private boolean reqCurrentMode;

    /** New state. */
    private ClusterState state;

    /** */
    public void reqCurrentMode() {
        reqCurrentMode = true;
    }

    /** */
    public boolean isReqCurrentMode() {
        return reqCurrentMode;
    }

    /** */
    public ClusterState state() {
        return state;
    }

    /**
     * Sets new cluster state to request.
     *
     * @param state New cluster state.
     * @throws NullPointerException If {@code state} is null.
     */
    public void state(ClusterState state) {
        if (state == null)
            throw new NullPointerException("State can't be null.");

        this.state = state;
    }
}
