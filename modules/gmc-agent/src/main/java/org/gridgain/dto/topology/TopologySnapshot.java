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

package org.gridgain.dto.topology;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Class with topology snapshot.
 */
public class TopologySnapshot {
    /** */
    private long topVer;

    /***/
    private List<Node> nodes = emptyList();

    /**
     * @param topVer Topology version.
     * @param nodes Cluster nodes.
     * @return Topology snapshot.
     */
    public static TopologySnapshot topology(long topVer, Collection<ClusterNode> nodes) {
        List<Node> clusterNodes = nodes != null ? nodes.stream().map(Node::new).collect(toList()) : emptyList();
        return new TopologySnapshot(topVer, clusterNodes);
    }

    /**
     * @param topVer Topology version.
     * @param nodes Cluster nodes.
     * @return Baseline snapshot.
     */
    public static TopologySnapshot baseline(long topVer, Collection<BaselineNode> nodes) {
        List<Node> baseLineNodes = nodes != null ? nodes.stream().map(Node::new).collect(toList()) : emptyList();
        return new TopologySnapshot(topVer, baseLineNodes);
    }

    /**
     * Default constructor for serialization.
     */
    public TopologySnapshot() {
        // No-op.
    }

    /**
     * Create topology snapshot for cluster.
     *
     * @param topVer Topology version.
     * @param nodes List of cluster nodes.
     */
    private TopologySnapshot(long topVer, List<Node> nodes) {
        this.topVer = topVer;
        this.nodes = nodes;
    }

    /**
     * @return Cluster topology version.
     */
    public long getTopologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Cluster topology version.
     */
    public void setTopologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Cluster nodes.
     */
    public List<Node> getNodes() {
        return nodes;
    }

    /**
     * @param nodes Cluster nodes.
     */
    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TopologySnapshot.class, this);
    }
}
