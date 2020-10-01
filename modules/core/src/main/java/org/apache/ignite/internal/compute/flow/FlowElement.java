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

/**
 * Represents a flow element that is stored in TaskFlow. It provides:
 * <ul>
 *   <li>Compute task adapter, that provides compute task for execution;</li>
 *   <li>Flow condition that determines if the compute task should be executed;</li>
 *   <li>Child flow elements that provide child tasks;</li>
 *   <li>Flow reducer that reduces result of child tasks.</li>
 * </ul>
 *
 * @param <T> Compute task class.
 * @param <A> Compute task parameters type.
 * @param <R> Compute task result type.
 */
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

    /**
     * Flow element name.
     *
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * Task adapter for compute task of this flow element.
     *
     * @return Task adapter.
     */
    public ComputeTaskFlowAdapter<T, A, R> taskAdapter() {
        return taskAdapter;
    }

    /**
     * Child flow elements. Each of them will receive the result of this element's task execution and
     * will be launched depending on it's own flow condition. The results of child tasks' are reduced
     * with {@link #reducer()} of this element.
     *
     * @return Collection of child elements.
     */
    public Collection<FlowElement> childElements() {
        return childElements;
    }

    /**
     * Reduces the results of child elements' tasks.
     *
     * @return Reduced result.
     */
    public FlowTaskReducer reducer() {
        return reducer;
    }

    /**
     * Flow condition determines if this element's task should be launched, basing on previous element's task result.
     *
     * @return Flow condition.
     */
    public FlowCondition condition() {
        return condition;
    }

    /**
     * Flow element builder.
     *
     * @param name Name.
     * @param taskAdapter Task adapter.
     * @param <T> Compute task class.
     * @param <A> Compute task parameters type.
     * @param <R> Compute task result type.
     * @return Builder.
     */
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

        /**
         * Adds child elements. If there is no flow condition for element, default condition will be:
         * added element will be executed only in the case of successful execution of current element's task.
         *
         * @param elements Child elements.
         * @return This.
         */
        public Builder<T, A, R> add(FlowElement... elements) {
            childElements.addAll(asList(elements));
            return this;
        }

        /**
         * Adds child elements with condition: all added elements will be executed only in the case of
         * failure of current element's task.
         *
         * @param elements Child elements.
         * @return This.
         */
        public Builder<T, A, R> exceptionally(FlowElement... elements) {
            childElements.addAll(
                asList(elements)
                    .stream()
                    .map(e -> new FlowElement(e.name, e.taskAdapter, e.childElements, e.reducer, new OnFailFlowCondition()))
                    .collect(toList())
            );
            return this;
        }

        /**
         * Sets a flow condition. The task will be executed only if previous task result matches the condition.
         *
         * @param condition Flow condition.
         * @return This.
         */
        public Builder<T, A, R> condition(FlowCondition condition) {
            this.condition = condition;
            return this;
        }

        /**
         * Sets a reducer for child tasks. If it's not set, default reducer will just take any of child task's result.
         *
         * @param reducer Reducer.
         * @return This.
         */
        public Builder<T, A, R> reducer(FlowTaskReducer reducer) {
            this.reducer = reducer;
            return this;
        }

        /**
         * Builds flow element.
         *
         * @return Flow element.
         */
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
