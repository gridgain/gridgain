/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task to cancel scan queries.
 */
@GridInternal
@GridVisorManagementTask
public class VisorScanQueryCancelTask extends VisorOneNodeTask<VisorScanQueryCancelTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorScanQueryCancelJob job(VisorScanQueryCancelTaskArg arg) {
        return new VisorScanQueryCancelJob(arg, debug);
    }

    /**
     * Job to cancel scan queries on node.
     */
    private static class VisorScanQueryCancelJob extends VisorJob<VisorScanQueryCancelTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorScanQueryCancelJob(@Nullable VisorScanQueryCancelTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(@Nullable VisorScanQueryCancelTaskArg arg) throws IgniteException {
            new QueryMXBeanImpl(ignite.context())
                .cancelScan(arg.getOriginNodeId(), arg.getCacheName(), arg.getQueryId());

            return null;
        }
    }
}
