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

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.JobContextResource;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.getQueryHolder;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.removeQueryHolder;

/**
 * Task for collecting next page previously executed SQL or SCAN query.
 */
@GridInternal
public class VisorQueryNextPageTask extends VisorOneNodeTask<VisorQueryNextPageTaskArg, VisorQueryResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryNextPageJob job(VisorQueryNextPageTaskArg arg) {
        return new VisorQueryNextPageJob(arg, debug);
    }

    /**
     * Job for collecting next page previously executed SQL or SCAN query.
     */
    private static class VisorQueryNextPageJob extends VisorJob<VisorQueryNextPageTaskArg, VisorQueryResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Operation future. */
        private volatile IgniteFuture<Void> opFut;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorQueryNextPageJob(VisorQueryNextPageTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorQueryResult run(VisorQueryNextPageTaskArg arg) {
            String qryId = arg.getQueryId();

            VisorQueryHolder holder = getQueryHolder(ignite, qryId);

            if (opFut == null) {
                opFut = holder.readyFuture();

                if (!opFut.isDone()) {
                    jobCtx.holdcc();

                    opFut.listen(f -> jobCtx.callcc());

                    return null;
                }
            }

            assert opFut.isDone() : "Query holder must be completed [queryId=" + qryId + ']';

            if (holder.getErr() != null)
                throw new IgniteException(holder.getErr());

            long start = System.currentTimeMillis();

            Iterator itr = holder.getIterator();

            List<Object[]> nextRows = VisorQueryHolder.isSqlQuery(qryId)
                ? VisorQueryUtils.fetchSqlQueryRows(itr, arg.getPageSize())
                : VisorQueryUtils.fetchScanQueryRows(itr, arg.getPageSize());

            boolean hasMore = itr.hasNext();

            if (hasMore)
                holder.setAccessed(true);
            else
                removeQueryHolder(ignite, qryId);

            return new VisorQueryResult(ignite.localNode().id(), qryId, null, nextRows, hasMore,
                System.currentTimeMillis() - start);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryNextPageJob.class, this);
        }
    }
}
