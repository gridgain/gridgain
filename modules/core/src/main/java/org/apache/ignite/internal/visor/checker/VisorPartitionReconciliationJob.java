/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.checker;

import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.Nullable;

/**
 * Partition reconciliation job.
 * @param <ResultT> Result type.
 */
public class VisorPartitionReconciliationJob<ResultT> extends VisorJob<VisorPartitionReconciliationTaskArg, ResultT> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ComputeTaskFuture<ResultT> fut;

    /** Auto-inject job context. */
    @JobContextResource
    protected transient ComputeJobContext jobCtx;

    /** Task class for execution */
    private final Class<? extends ComputeTask<VisorPartitionReconciliationTaskArg, ResultT>> taskCls;

    /**
     * @param arg Argument.
     * @param debug Debug.
     * @param taskCls Task class for execution.
     */
    VisorPartitionReconciliationJob(
        VisorPartitionReconciliationTaskArg arg,
        boolean debug,
        Class<? extends ComputeTask<VisorPartitionReconciliationTaskArg, ResultT>> taskCls
    ) {
        super(arg, debug);
        this.taskCls = taskCls;
    }

    /** {@inheritDoc} */
    @Override protected ResultT run(@Nullable VisorPartitionReconciliationTaskArg arg) throws IgniteException {
        if (fut == null) {
            fut = ignite.compute().executeAsync(taskCls, arg);

            if (!fut.isDone()) {
                jobCtx.holdcc();

                fut.listen((IgniteInClosure<IgniteFuture<ResultT>>)f -> jobCtx.callcc());

                return null;
            }
        }

        return fut.get();
    }
}
