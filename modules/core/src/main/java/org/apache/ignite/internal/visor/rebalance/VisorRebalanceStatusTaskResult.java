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

package org.apache.ignite.internal.visor.rebalance;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;

/**
 * Result of rebalance status task execution.
 */
public class VisorRebalanceStatusTaskResult extends IgniteDataTransferObject {
    /** Cluster rebalance state. */
    private boolean isRebalanceComplited;

    /** Nodes that are rebalanced. */
    private Set<VisorBaselineNode> rebNodes;

    /** Count of nodes that can leave cluster. */
    private int replicatedNodeCount;

    /**
     * Group mapped with count of node that can leave topology without loss data in the group.
     */
    private Map<VisorRebalanceStatusGroupView, Integer> groups;

    /**
     * @return True if rebalance was completed, false otherwise.
     */
    public boolean isRebalanceComplited() {
        return isRebalanceComplited;
    }

    /**
     * @param rebalanceComplited Flag of rebalance completed.
     */
    public void setRebalanceComplited(boolean rebalanceComplited) {
        isRebalanceComplited = rebalanceComplited;
    }

    /**
     * @return Set of nodes.
     */
    public Set<VisorBaselineNode> getRebNodes() {
        return rebNodes;
    }

    /**
     * @param rebNodes Set of nodes.
     */
    public void setRebNodes(Set<VisorBaselineNode> rebNodes) {
        this.rebNodes = rebNodes;
    }

    /**
     * @return Count of nodes, that can leave cluster.
     */
    public int getReplicatedNodeCount() {
        return replicatedNodeCount;
    }

    /**
     * @param replicatedNodeCount Count of nodes.
     */
    public void setReplicatedNodeCount(int replicatedNodeCount) {
        this.replicatedNodeCount = replicatedNodeCount;
    }

    /**
     * @return Groups map.
     */
    public Map<VisorRebalanceStatusGroupView, Integer> getGroups() {
        return groups;
    }

    /**
     * @param groups View of groups map.
     */
    public void setGroups(Map<VisorRebalanceStatusGroupView, Integer> groups) {
        this.groups = groups;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(isRebalanceComplited);
        U.writeCollection(out, rebNodes);
        out.writeInt(replicatedNodeCount);
        U.writeMap(out, groups);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        isRebalanceComplited = in.readBoolean();
        rebNodes = U.readSet(in);
        replicatedNodeCount = in.readInt();
        groups = U.readMap(in);
    }
}
