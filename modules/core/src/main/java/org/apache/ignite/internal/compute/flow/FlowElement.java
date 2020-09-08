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
import java.util.LinkedList;
import org.apache.ignite.compute.ComputeTask;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;
import static java.util.stream.Collectors.toList;

public class FlowElement<T extends ComputeTask<A, R>, A, R> implements Serializable {
    private static final long serialVersionUID = 0L;

    private final String name;

    private final ComputeTaskFlowAdapter<T, A, R> taskAdapter;

    private final Collection<FlowElement> childElements;

    private final FlowTaskReducer reducer;

    private final FlowCondition condition;

    public FlowElement(
        String name,
        ComputeTaskFlowAdapter<T, A, R> taskAdapter,
        Collection<FlowElement> childElements,
        FlowTaskReducer reducer,
        FlowCondition condition
    ) {
        this.name = name;
        this.taskAdapter = taskAdapter;
        this.childElements = unmodifiableCollection(childElements);
        this.reducer = reducer;
        this.condition = condition;
    }

    public String name() {
        return name;
    }

    public ComputeTaskFlowAdapter<T, A, R> taskAdapter() {
        return taskAdapter;
    }

    public Collection<FlowElement> childElements() {
        return childElements;
    }

    public FlowTaskReducer reducer() {
        return reducer;
    }

    public FlowCondition condition() {
        return condition;
    }

    public static <T extends ComputeTask<A, R>, A, R> Builder<T, A, R> builder(String name, ComputeTaskFlowAdapter<T, A, R> taskAdapter) {
        return new Builder<>(name, taskAdapter);
    }

    public static class Builder<T extends ComputeTask<A, R>, A, R> {
        private String name;
        private ComputeTaskFlowAdapter<T, A, R> taskAdapter;
        private FlowCondition condition;
        private FlowTaskReducer reducer;
        private Collection<FlowElement> childElements = new LinkedList<>();

        public Builder(String name, ComputeTaskFlowAdapter<T, A, R> taskAdapter) {
            this.name = name;
            this.taskAdapter = taskAdapter;
        }

        public Builder<T, A, R> add(FlowElement... elements) {
            childElements.addAll(asList(elements));
            return this;
        }

        public Builder<T, A, R> exceptionally(FlowElement... elements) {
            childElements.addAll(
                asList(elements)
                    .stream()
                    .map(e -> new FlowElement(e.name, e.taskAdapter, e.childElements, e.reducer, new OnFailFlowCondition()))
                    .collect(toList())
            );
            return this;
        }

        public Builder<T, A, R> condition(FlowCondition condition) {
            this.condition = condition;
            return this;
        }

        public Builder<T, A, R> reducer(FlowTaskReducer reducer) {
            this.reducer = reducer;
            return this;
        }

        public FlowElement<T, A, R> build() {
            return new FlowElement(
                name,
                taskAdapter,
                childElements,
                reducer != null ? reducer : new AnyResultReducer(),
                condition != null ? condition : new OnSuccessFlowCondition()
            );
        }
    }
}
