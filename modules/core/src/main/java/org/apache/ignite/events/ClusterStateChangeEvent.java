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

package org.apache.ignite.events;

import java.util.Collection;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CLUSTER_STATE_CHANGED;

public class ClusterStateChangeEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Previous cluster state. */
    private final ClusterState prevState;

    /** Current cluster state. */
    private final ClusterState state;

    /** Baseline nodes. */
    private final Collection<? extends BaselineNode> baselineNodes;

    /**
     * @param prevState Previous cluster state.
     * @param state Current cluster state.
     * @param baselineNodes Collection of baseline nodes. (Optional for in-memory case)
     * @param node Node.
     * @param msg Optional event message.
     */
    public ClusterStateChangeEvent(
        ClusterState prevState,
        ClusterState state,
        @Nullable Collection<? extends BaselineNode> baselineNodes,
        ClusterNode node,
        String msg
    ) {
        super(node, msg, EVT_CLUSTER_STATE_CHANGED);

        A.notNull(prevState, "prevState");
        A.notNull(state, "state");

        this.state = state;
        this.prevState = prevState;
        this.baselineNodes = baselineNodes;
    }

    /**
     * @return Previous cluster state.
     */
    public ClusterState previousState() {
        return prevState;
    }

    /**
     * @return Current cluster state.
     */
    public ClusterState state() {
        return state;
    }

    /**
     * Gets baseline nodes.
     *
     * @return Baseline nodes.
     */
    public @Nullable Collection<? extends BaselineNode> baselineNodes() {
        return baselineNodes;
    }
}
