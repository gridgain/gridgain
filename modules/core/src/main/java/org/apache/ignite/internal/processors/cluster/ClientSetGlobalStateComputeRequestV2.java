/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Request for the change cluster state from client node.
 */
@GridInternal
public class ClientSetGlobalStateComputeRequestV2 implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ClusterState state;

    /** If {@code true}, cluster deactivation will be forced. */
    private final boolean forceDeactivation;

    /** */
    private final BaselineTopology baselineTopology;

    /** */
    private final boolean forceChangeBaselineTopology;

    /** Ignite. */
    @IgniteInstanceResource
    private IgniteEx ig;

    /**
     * @param state New cluster state.
     * @param forceDeactivation If {@code true}, cluster deactivation will be forced.
     * @param blt New baseline topology.
     * @param forceBlt Force change cluster state.
     */
    ClientSetGlobalStateComputeRequestV2(
        ClusterState state,
        boolean forceDeactivation,
        BaselineTopology blt,
        boolean forceBlt
    ) {
        this.state = state;
        this.baselineTopology = blt;
        this.forceChangeBaselineTopology = forceBlt;
        this.forceDeactivation = forceDeactivation;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            ig.context().state().changeGlobalState(
                state,
                forceDeactivation,
                baselineTopology != null ? baselineTopology.currentBaseline() : null,
                forceChangeBaselineTopology
            ).get();
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }
    }
}
