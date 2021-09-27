/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.checkpointing;

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
public class CheckpointingForceTask extends VisorMultiNodeTask<VoidDto, CheckpointingForceResult, NodeCheckpointingResult> {

    /** */
    private static final long serialVersionUID = 0L;

    @Override protected VisorJob<VoidDto, NodeCheckpointingResult> job(VoidDto arg) {
        return new CheckpointingForceJob(debug);
    }

    @Nullable @Override
    protected CheckpointingForceResult reduce0(List<ComputeJobResult> results) throws IgniteException {
        CheckpointingForceResult res = new CheckpointingForceResult();

        for (ComputeJobResult result : results) {
            NodeCheckpointingResult nodeRes = result.getData();
            res.status().put(nodeRes.nodeId(), nodeRes);
        }
        return res;
    }

    /** */
    private static class CheckpointingForceJob extends VisorJob<VoidDto, NodeCheckpointingResult> {

        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** */
        private GridFutureAdapter fut;

        private NodeCheckpointingResult res;
        long startTime;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        public CheckpointingForceJob(boolean debug) {
            super(null, debug);
        }

        /** {@inheritDoc} */
        @Override protected NodeCheckpointingResult run(@Nullable VoidDto arg) throws IgniteException {
            if (fut == null) {
                res = new NodeCheckpointingResult();
                res.nodeId(ignite.localNode().id());

                startTime = U.currentTimeMillis();
                CheckpointProgress progress = ignite.context().cache().context().database().forceCheckpoint("force");
                if (progress == null) {
                    res.durationMillis(U.currentTimeMillis() - startTime);
                    return res;
                }

                fut = progress.futureFor(CheckpointState.FINISHED);
                if (fut.isDone())
                    getFuture(fut, res);

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
            getFuture(fut, res);

            return res;
        }
    }

    private static Object getFuture(GridFutureAdapter fut, NodeCheckpointingResult res) {
        try {
            return fut.get();
        }
        catch (IgniteCheckedException e) {
            res.error(e);
        }

        return null;
    }
}
