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

public class TaskFlowBuilder {
    private GridFlowTempElement rootElement;
    private Map<String, GridFlowTempElement> elements = new HashMap<>();

    public TaskFlowBuilder addTask(String name, String parentName, FlowTaskAdapter taskAdapter, FlowCondition condition) {
        if (parentName == null) {
            rootElement = new GridFlowTempElement(name, taskAdapter);

            elements.put(name, rootElement);
        }
        else {
            GridFlowTempElement parent = elements.get(parentName);

            GridFlowTempElement newElement = new GridFlowTempElement(name, taskAdapter);

            parent.childElements.add(new IgniteBiTuple<>(condition, newElement));

            elements.put(name, newElement);
        }

        return this;
    }

    public TaskFlow build() {
        return new TaskFlow(tempElementToFlowElement(rootElement));
    }

    private TaskFlowElement tempElementToFlowElement(GridFlowTempElement tempElement) {
        return new TaskFlowElement(
            tempElement.name,
            tempElement.taskAdapter,
            tempElement.childElements.stream().map(c -> new IgniteBiTuple(c.get1(), tempElementToFlowElement(c.get2()))).collect(toList()));
    }

    private static class GridFlowTempElement {
        final String name;
        final FlowTaskAdapter taskAdapter;
        final List<IgniteBiTuple<FlowCondition, GridFlowTempElement>> childElements;

        public GridFlowTempElement(String name, FlowTaskAdapter taskAdapter) {
            this.name = name;
            this.taskAdapter = taskAdapter;
            this.childElements = new LinkedList<>();
        }
    }
}
