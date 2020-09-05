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
package org.apache.ignite.internal.compute.flow;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.stream.Collectors.toList;

public class GridTaskFlowBuilder {
    private FlowResultAggregator aggregator;
    private GridFlowTempNode rootNode;
    private Map<String, GridFlowTempNode> nodes = new HashMap<>();

    public GridTaskFlowBuilder(FlowResultAggregator aggregator) {
        this.aggregator = aggregator;
    }

    public GridTaskFlowBuilder addTask(String name, String parentName, GridFlowTaskAdapter node, FlowCondition condition) {
        if (parentName == null) {
            rootNode = new GridFlowTempNode(name, node);

            nodes.put(name, rootNode);
        }
        else {
            GridFlowTempNode parent = nodes.get(parentName);

            GridFlowTempNode newNode = new GridFlowTempNode(name, node);

            parent.childNodes.add(new IgniteBiTuple<>(condition, newNode));

            nodes.put(name, newNode);
        }

        return this;
    }

    public GridTaskFlow build() {
        return new GridTaskFlow(tempNodeToFlowElement(rootNode), aggregator);
    }

    private GridFlowElement tempNodeToFlowElement(GridFlowTempNode tempNode) {
        return new GridFlowElement(
            tempNode.name,
            tempNode.node,
            tempNode.childNodes.stream().map(c -> new IgniteBiTuple(c.get1(), tempNodeToFlowElement(c.get2()))).collect(toList()));
    }

    private static class GridFlowTempNode {
        final String name;
        final GridFlowTaskAdapter node;
        final List<IgniteBiTuple<FlowCondition, GridFlowTempNode>> childNodes;

        public GridFlowTempNode(String name, GridFlowTaskAdapter node) {
            this.name = name;
            this.node = node;
            this.childNodes = new LinkedList<>();
        }
    }
}
