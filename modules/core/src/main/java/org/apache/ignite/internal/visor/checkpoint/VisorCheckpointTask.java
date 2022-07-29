/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.checkpoint;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.commandline.meta.subcommands.VoidDto;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task for checkpointing operation
 */
@GridInternal
public class VisorCheckpointTask extends VisorMultiNodeTask<VoidDto, VisorCheckpointTaskResult, VisorCheckpointJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VoidDto, VisorCheckpointJobResult> job(VoidDto arg) {
        return new CheckpointingForceJob(debug);
    }

    /**
     * Collect statuses of underlying checkpoint jobs.
     *
     * @param results Job results.
     * @return Collected statuses of underlying checkpoint jobs.
     * @throws IgniteException If failed.
     */
    @Override protected VisorCheckpointTaskResult reduce0(List<ComputeJobResult> results) throws IgniteException {
        VisorCheckpointTaskResult res = new VisorCheckpointTaskResult();

        for (ComputeJobResult result : results) {
            VisorCheckpointJobResult nodeRes = result.getData();
            res.status().put(nodeRes.nodeId(), nodeRes);
        }
        return res;
    }

    /**
     * Compute job that does a local checkpoint on a node
     */
    private static class CheckpointingForceJob extends VisorJob<VoidDto, VisorCheckpointJobResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** */
        private GridFutureAdapter fut;

        /** Checkpoint result. */
        private VisorCheckpointJobResult res;

        /** Checkpoint start time. */
        long startTime;

        /**
         * @param debug Debug.
         */
        public CheckpointingForceJob(boolean debug) {
            super(null, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCheckpointJobResult run(@Nullable VoidDto arg) throws IgniteException {
            if (fut == null) {
                res = new VisorCheckpointJobResult();
                res.nodeId(ignite.localNode().id());

                startTime = U.currentTimeMillis();
                CheckpointProgress progress = ignite.context().cache().context().database().forceCheckpoint("force");
                if (progress == null) {
                    res.durationMillis(U.currentTimeMillis() - startTime);
                    return res;
                }

                fut = progress.futureFor(CheckpointState.FINISHED);
                if (fut.isDone()) {
                    readFutureResult(fut, res);
                    return res;
                }
                else {
                    jobCtx.holdcc();

                    fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                        @Override public void apply(IgniteInternalFuture f) {
                            res.durationMillis(U.currentTimeMillis() - startTime);

                            if (f.isCancelled())
                                res.error(f.error());

                            jobCtx.callcc();
                        }
                    });
                }

                return null;
            }
            readFutureResult(fut, res);

            return res;
        }
    }

    private static void readFutureResult(GridFutureAdapter fut, VisorCheckpointJobResult res) {
        try {
            fut.get();
        }
        catch (IgniteCheckedException e) {
            res.error(e);
        }
    }
}
