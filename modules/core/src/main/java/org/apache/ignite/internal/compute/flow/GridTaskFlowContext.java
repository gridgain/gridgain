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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeUserUndeclaredException;
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
        assert isStarted.compareAndSet(false, true);

        FlowTaskReducer resultReducer = flow.rootElement().taskAdapter().resultReducer();

        IgniteInternalFuture<GridFlowTaskTransferObject> rootTaskFut =
            executeFlowTaskAsync(flow.rootElement(), flowParams, resultReducer);

        rootTaskFut.listen(new IgniteInClosure<IgniteInternalFuture<GridFlowTaskTransferObject>>() {
            @Override public void apply(IgniteInternalFuture<GridFlowTaskTransferObject> future) {
                try {
                    GridFlowTaskTransferObject res = future.get();

                    if (res.successfull())
                        completeFut.onDone(res);
                    else
                        completeFut.onDone(res.exception());
                }
                catch (IgniteCheckedException e) {
                    completeFut.onDone(e);
                }
            }
        });

        return completeFut;
    }

    private IgniteInternalFuture<GridFlowTaskTransferObject> completeTask(GridFlowElement flowElement, GridFlowTaskTransferObject result, FlowTaskReducer resultReducer) {
        GridCompoundFuture<GridFlowTaskTransferObject, GridFlowTaskTransferObject> taskAndSubtasksCompleteFut =
            new GridCompoundFuture<>(resultReducer);

        if (!flowElement.childElements().isEmpty()) {
            for (IgniteBiTuple<FlowCondition, GridFlowElement> childElementsInfo : (Collection<IgniteBiTuple<FlowCondition, GridFlowElement>>)flowElement.childElements()) {
                if (childElementsInfo.get1().test(result)) {
                    FlowTaskReducer childResultReducer = childElementsInfo.get2().taskAdapter().resultReducer();

                    IgniteInternalFuture<GridFlowTaskTransferObject> fut =
                        executeFlowTaskAsync(childElementsInfo.get2(), result, childResultReducer);

                    fut.listen(new IgniteInClosure<IgniteInternalFuture<GridFlowTaskTransferObject>>() {
                        @Override public void apply(IgniteInternalFuture<GridFlowTaskTransferObject> future) {
                            try {
                                resultReducer.collect(future.get());
                            }
                            catch (IgniteCheckedException e) {
                                resultReducer.collect(new GridFlowTaskTransferObject(e));
                            }
                        }
                    });

                    taskAndSubtasksCompleteFut.add(fut);
                }
            }
        }

        if (taskAndSubtasksCompleteFut.futures().isEmpty()) {
            GridFutureAdapter<GridFlowTaskTransferObject> f0 = new GridFutureAdapter<>();

            taskAndSubtasksCompleteFut.add(f0);

            f0.onDone(result);
        }

        taskAndSubtasksCompleteFut.markInitialized();

        return taskAndSubtasksCompleteFut;
    }

    private void completeFlow(GridFlowTaskTransferObject res) {
        if (res.successfull())
            completeFut.onDone(res);
        else
            completeFut.onDone(res.exception());
    }

    private <T extends FlowTask<A, R>, A, R> IgniteInternalFuture<GridFlowTaskTransferObject> executeFlowTaskAsync(
        GridFlowElement<T, A, R> flowElement,
        GridFlowTaskTransferObject params,
        FlowTaskReducer resultReducer
    ) {
        GridFlowTaskAdapter<T, A, R> taskAdapter = flowElement.taskAdapter();

        Class<T> taskCls = taskAdapter.taskClass();

        A args = taskAdapter.arguments(params);

        ClusterGroup group = taskAdapter.nodeFilter() == null
            ? ctx.grid().cluster()
            : ctx.grid().cluster().forPredicate(taskAdapter.nodeFilter());

        ComputeTaskFuture<R> fut = ctx.grid().compute(group).executeAsync(taskCls, args);

        GridFutureAdapter<GridFlowTaskTransferObject> resultFut = new GridFutureAdapter();

        fut.listen(new IgniteInClosure<IgniteFuture<R>>() {
            @Override public void apply(IgniteFuture<R> future) {
                GridFlowTaskTransferObject res;

                try {
                    R taskResult = future.get();

                    res = taskAdapter.result(taskResult);
                }
                catch (ComputeUserUndeclaredException e) {
                    res = new GridFlowTaskTransferObject(e.getCause());
                }
                catch (IgniteException e) {
                    res = new GridFlowTaskTransferObject(e);
                }

                IgniteInternalFuture<GridFlowTaskTransferObject> taskCompleteFut = completeTask(flowElement, res, resultReducer);

                taskCompleteFut.listen(new IgniteInClosure<IgniteInternalFuture<GridFlowTaskTransferObject>>() {
                    @Override public void apply(IgniteInternalFuture<GridFlowTaskTransferObject> future) {
                        try {
                            resultFut.onDone(future.get());
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
