/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.processors.affinity;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Shallow copy that contains reference to delegate {@link HistoryAffinityAssignment}.
 */
public class HistoryAffinityAssignmentShallowCopy implements HistoryAffinityAssignment {
    /** History assignment. */
    private final HistoryAffinityAssignment histAssignment;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /**
     * @param histAssignment History assignment.
     * @param topVer Topology version.
     */
    public HistoryAffinityAssignmentShallowCopy(
        HistoryAffinityAssignment histAssignment,
        AffinityTopologyVersion topVer
    ) {
        this.histAssignment = histAssignment;
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean requiresHistoryCleanup() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> idealAssignment() {
        return histAssignment.idealAssignment();
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignment() {
        return histAssignment.assignment();
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> get(int part) {
        return histAssignment.get(part);
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> getIds(int part) {
        return histAssignment.getIds(part);
    }

    /** {@inheritDoc} */
    @Override public Set<ClusterNode> nodes() {
        return histAssignment.nodes();
    }

    /** {@inheritDoc} */
    @Override public Set<ClusterNode> primaryPartitionNodes() {
        return histAssignment.primaryPartitionNodes();
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> primaryPartitions(UUID nodeId) {
        return histAssignment.primaryPartitions(nodeId);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> backupPartitions(UUID nodeId) {
        return histAssignment.backupPartitions(nodeId);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> partitionPrimariesDifferentToIdeal() {
        return histAssignment.partitionPrimariesDifferentToIdeal();
    }

    /** {@inheritDoc} */
    @Override public HistoryAffinityAssignment origin() {
        return histAssignment;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HistoryAffinityAssignmentShallowCopy.class, this);
    }
}
