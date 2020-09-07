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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.Collections.unmodifiableList;

public class TaskFlowElement<T extends FlowTask<A, R>, A, R> implements Serializable {
    private final String name;

    private final FlowTaskAdapter<T, A, R> taskAdapter;

    private final Collection<IgniteBiTuple<FlowCondition, TaskFlowElement>> childElements;

    public TaskFlowElement(String name, FlowTaskAdapter<T, A, R> taskAdapter,
        List<IgniteBiTuple<FlowCondition, TaskFlowElement>> childElements) {
        this.name = name;
        this.taskAdapter = taskAdapter;
        this.childElements = unmodifiableList(childElements);
    }

    public String name() {
        return name;
    }

    public FlowTaskAdapter<T, A, R> taskAdapter() {
        return taskAdapter;
    }

    public Collection<IgniteBiTuple<FlowCondition, TaskFlowElement>> childElements() {
        return childElements;
    }
}
