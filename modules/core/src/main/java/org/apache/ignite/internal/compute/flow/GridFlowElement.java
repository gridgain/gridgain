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

public class GridFlowElement<T extends FlowTask<A, R>, A, R> implements Serializable {
    private final String name;

    private final GridFlowTaskAdapter<T, A, R> node;

    private final Collection<IgniteBiTuple<FlowCondition, GridFlowElement>> childElements;

    public GridFlowElement(String name, GridFlowTaskAdapter<T, A, R> node,
        List<IgniteBiTuple<FlowCondition, GridFlowElement>> childElements) {
        this.name = name;
        this.node = node;
        this.childElements = unmodifiableList(childElements);
    }

    public String name() {
        return name;
    }

    public GridFlowTaskAdapter<T, A, R> node() {
        return node;
    }

    public Collection<IgniteBiTuple<FlowCondition, GridFlowElement>> childElements() {
        return childElements;
    }
}