/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.query;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task to cancel queries on initiator node.
 */
@GridInternal
@GridVisorManagementTask
public class VisorQueryCancelOnInitiatorTask extends VisorOneNodeTask<VisorQueryCancelOnInitiatorTaskArg, Void> {
    /**  */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCancelQueryOnInitiatorJob job(VisorQueryCancelOnInitiatorTaskArg arg) {
        return new VisorCancelQueryOnInitiatorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List<ComputeJobResult> results) throws IgniteException {
        return null;
    }

    /** Job to cancel query on node. */
    private static class VisorCancelQueryOnInitiatorJob extends VisorJob<VisorQueryCancelOnInitiatorTaskArg, Void> {
        /**  */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorCancelQueryOnInitiatorJob(VisorQueryCancelOnInitiatorTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorQueryCancelOnInitiatorTaskArg arg) throws IgniteException {
            new QueryMXBeanImpl(ignite.context()).cancelSQL(arg.getNodeId(), arg.getQueryId());

            return null;
        }
    }
}
