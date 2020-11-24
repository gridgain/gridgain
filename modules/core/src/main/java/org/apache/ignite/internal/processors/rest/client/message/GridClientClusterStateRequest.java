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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
@Deprecated
public class GridClientClusterStateRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request current state. */
    private boolean reqCurrentState;

    /** New cluster state. */
    private ClusterState state;

    /** */
    public boolean isReqCurrentState() {
        return reqCurrentState;
    }

    /** */
    public ClusterState state() {
        return state;
    }

    /**
     * @return Current read-only mode request.
     */
    public static GridClientClusterStateRequest currentState() {
        GridClientClusterStateRequest msg = new GridClientClusterStateRequest();

        msg.reqCurrentState = true;

        return msg;
    }

    /**
     * @param state New cluster state.
     * @return Cluster state change request.
     */
    public static GridClientClusterStateRequest state(ClusterState state) {
        GridClientClusterStateRequest msg = new GridClientClusterStateRequest();

        msg.state = state;

        return msg;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(reqCurrentState);
        U.writeEnum(out, state);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        reqCurrentState = in.readBoolean();
        state = ClusterState.fromOrdinal(in.readByte());
    }
}
