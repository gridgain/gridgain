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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Adapter for ComputeTask to execute it in flow.
 *
 * @param <T> Compute task class.
 * @param <A> Compute task parameters type.
 * @param <R> Compute task result type.
 */
public interface ComputeTaskFlowAdapter<T extends ComputeTask<A, R>, A, R> extends Serializable {
    /**
     * @return Compute task class.
     */
    Class<T> taskClass();

    /**
     * Transforms input (e.g. result of previous task execution) from {@link FlowTaskTransferObject} to
     * compute task parameters object.
     *
     * @param input Input as {@link FlowTaskTransferObject}.
     * @return Compute task parameters object.
     */
    A parameters(FlowTaskTransferObject input);

    /**
     * Transforms compute task output from result object to {@link FlowTaskTransferObject}.
     *
     * @param r Compute task result object.
     * @return Transfer object.
     */
    FlowTaskTransferObject result(R r);

    /**
     * Predicate to filter nodes where compute job should run.
     *
     * @return Predicate.
     */
    IgnitePredicate<ClusterNode> nodeFilter();
}
