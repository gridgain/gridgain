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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

public class GridTaskFlowContext {
    private final GridKernalContext ctx;

    private final GridTaskFlow flow;

    private final GridFlowTaskTransferObject flowParams;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    private final GridFutureAdapter<GridFlowTaskTransferObject> completeFut = new GridFutureAdapter<>();

    public GridTaskFlowContext(GridKernalContext ctx, GridTaskFlow flow,
        GridFlowTaskTransferObject params) {
        this.ctx = ctx;
        this.flow = flow;
        flowParams = params;
    }

    public IgniteInternalFuture<GridFlowTaskTransferObject> start() {
        isStarted.compareAndSet(false, true);

        IgniteInternalFuture rootTaskFut = executeFlowTaskAsync(flow.rootNode(), flowParams);

        rootTaskFut.listen(new IgniteInClosure<IgniteInternalFuture>() {
            @Override public void apply(IgniteInternalFuture future) {
                completeFlow();
            }
        });

        return completeFut;
    }

    private IgniteInternalFuture completeTask(GridFlowElement node, GridFlowTaskTransferObject result) {
        GridCompoundFuture taskAndSubtasksCompleteFut = new GridCompoundFuture();

        if (!node.childElements().isEmpty()) {
            for (IgniteBiTuple<FlowCondition, GridFlowElement> childNodeInfo : (Collection<IgniteBiTuple<FlowCondition, GridFlowElement>>)node.childElements()) {
                if (childNodeInfo.get1().test(result)) {
                    IgniteInternalFuture fut = executeFlowTaskAsync(childNodeInfo.get2(), result);

                    taskAndSubtasksCompleteFut.add(fut);
                }
            }

            taskAndSubtasksCompleteFut.markInitialized();
        }
        else {
            flow.aggregator().accept(result);

            taskAndSubtasksCompleteFut.onDone();
        }

        return taskAndSubtasksCompleteFut;
    }

    private void completeFlow() {
        GridFlowTaskTransferObject res = flow.aggregator().result();

        if (res.successfull())
            completeFut.onDone(res);
        else
            completeFut.onDone(res.exception());
    }

    private <T extends FlowTask<A, R>, A, R> IgniteInternalFuture<GridFlowTaskTransferObject> executeFlowTaskAsync(
        GridFlowElement<T, A, R> storedNode,
        GridFlowTaskTransferObject params
    ) {
        GridFlowTaskAdapter<T, A, R> flowNode = storedNode.node();

        Class<T> taskCls = flowNode.taskClass();

        A args = flowNode.arguments(params);

        ClusterGroup group = flowNode.nodeFilter() == null
            ? ctx.grid().cluster()
            : ctx.grid().cluster().forPredicate(flowNode.nodeFilter());

        ComputeTaskFuture<R> fut = ctx.grid().compute(group).executeAsync(taskCls, args);

        GridFutureAdapter resultFut = new GridFutureAdapter();

        fut.listen(new IgniteInClosure<IgniteFuture<R>>() {
            @Override public void apply(IgniteFuture<R> future) {
                GridFlowTaskTransferObject res;

                try {
                    R taskResult = future.get();

                    res = flowNode.result(taskResult);
                }
                catch (IgniteException e) {
                    res = new GridFlowTaskTransferObject(e);
                }

                IgniteInternalFuture taskCompleteFut = completeTask(storedNode, res);

                taskCompleteFut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                    @Override public void apply(IgniteInternalFuture future) {
                        try {
                            future.get();

                            resultFut.onDone();
                        }
                        catch (Throwable e) {
                            resultFut.onDone(e);
                        }
                    }
                });
            }
        });

        return resultFut;
    }
}