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
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeUserUndeclaredException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

public class TaskFlowContext {
    private final GridKernalContext ctx;

    private final TaskFlow flow;

    private final FlowTaskTransferObject flowParams;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    private final GridFutureAdapter<FlowTaskTransferObject> completeFut = new GridFutureAdapter<>();

    public TaskFlowContext(GridKernalContext ctx, TaskFlow flow,
        FlowTaskTransferObject params) {
        this.ctx = ctx;
        this.flow = flow;
        flowParams = params;
    }

    public IgniteInternalFuture<FlowTaskTransferObject> start() {
        assert isStarted.compareAndSet(false, true);

        FlowTaskReducer resultReducer = flow.rootElement().reducer();

        IgniteInternalFuture<FlowTaskTransferObject> rootTaskFut =
            executeFlowTaskAsync(flow.rootElement(), flowParams, resultReducer);

        rootTaskFut.listen(new IgniteInClosure<IgniteInternalFuture<FlowTaskTransferObject>>() {
            @Override public void apply(IgniteInternalFuture<FlowTaskTransferObject> future) {
                try {
                    FlowTaskTransferObject res = future.get();

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

    private IgniteInternalFuture<FlowTaskTransferObject> completeTask(TaskFlowElement flowElement, FlowTaskTransferObject result, FlowTaskReducer resultReducer) {
        GridCompoundFuture<FlowTaskTransferObject, FlowTaskTransferObject> taskAndSubtasksCompleteFut =
            new GridCompoundFuture<>(resultReducer);

        if (!flowElement.childElements().isEmpty()) {
            for (IgniteBiTuple<FlowCondition, TaskFlowElement> childElementsInfo : (Collection<IgniteBiTuple<FlowCondition, TaskFlowElement>>)flowElement.childElements()) {
                if (childElementsInfo.get1().test(result)) {
                    FlowTaskReducer childResultReducer = childElementsInfo.get2().reducer();

                    IgniteInternalFuture<FlowTaskTransferObject> fut =
                        executeFlowTaskAsync(childElementsInfo.get2(), result, childResultReducer);

                    fut.listen(new IgniteInClosure<IgniteInternalFuture<FlowTaskTransferObject>>() {
                        @Override public void apply(IgniteInternalFuture<FlowTaskTransferObject> future) {
                            try {
                                resultReducer.collect(future.get());
                            }
                            catch (IgniteCheckedException e) {
                                resultReducer.collect(new FlowTaskTransferObject(e));
                            }
                        }
                    });

                    taskAndSubtasksCompleteFut.add(fut);
                }
            }
        }

        if (taskAndSubtasksCompleteFut.futures().isEmpty()) {
            GridFutureAdapter<FlowTaskTransferObject> f0 = new GridFutureAdapter<>();

            taskAndSubtasksCompleteFut.add(f0);

            f0.onDone(result);
        }

        taskAndSubtasksCompleteFut.markInitialized();

        return taskAndSubtasksCompleteFut;
    }

    private <T extends ComputeTask<A, R>, A, R> IgniteInternalFuture<FlowTaskTransferObject> executeFlowTaskAsync(
        TaskFlowElement<T, A, R> flowElement,
        FlowTaskTransferObject params,
        FlowTaskReducer resultReducer
    ) {
        ComputeTaskFlowAdapter<T, A, R> taskAdapter = flowElement.taskAdapter();

        Class<T> taskCls = taskAdapter.taskClass();

        A args = taskAdapter.arguments(params);

        ClusterGroup group = taskAdapter.nodeFilter() == null
            ? ctx.grid().cluster()
            : ctx.grid().cluster().forPredicate(taskAdapter.nodeFilter());

        ComputeTaskFuture<R> fut = ctx.grid().compute(group).executeAsync(taskCls, args);

        GridFutureAdapter<FlowTaskTransferObject> resultFut = new GridFutureAdapter();

        fut.listen(new IgniteInClosure<IgniteFuture<R>>() {
            @Override public void apply(IgniteFuture<R> future) {
                FlowTaskTransferObject res;

                try {
                    R taskResult = future.get();

                    res = taskAdapter.result(taskResult);
                }
                catch (ComputeUserUndeclaredException e) {
                    res = new FlowTaskTransferObject(e.getCause());
                }
                catch (IgniteException e) {
                    res = new FlowTaskTransferObject(e);
                }

                IgniteInternalFuture<FlowTaskTransferObject> taskCompleteFut = completeTask(flowElement, res, resultReducer);

                taskCompleteFut.listen(new IgniteInClosure<IgniteInternalFuture<FlowTaskTransferObject>>() {
                    @Override public void apply(IgniteInternalFuture<FlowTaskTransferObject> future) {
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
