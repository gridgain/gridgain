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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.Nullable;

/** Clears specified caches. */
@GridInternal
public class ClearCachesTask extends VisorOneNodeTask<ClearCachesTaskArg, ClearCachesTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Override protected VisorJob<ClearCachesTaskArg, ClearCachesTaskResult> job(ClearCachesTaskArg arg) {
        return new ClearCacheJob(arg, debug);
    }

    /** Job clears specified caches. */
    private static class ClearCacheJob extends VisorJob<ClearCachesTaskArg, ClearCachesTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Local Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        private GridCompoundFuture opFut;

        @JobContextResource
        private ComputeJobContext jobCtx;

        /** */
        private ClearCacheJob(ClearCachesTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected ClearCachesTaskResult run(@Nullable ClearCachesTaskArg arg) throws IgniteException {
            if (opFut == null) {
                opFut = new GridCompoundFuture<>();

                for (String cache: arg.caches()) {
                    IgniteCache<?, ?> ignCache = ignite.cache(cache);
                    GridFutureAdapter<?> fut = new GridFutureAdapter<>();
                    ignCache.clearAsync().listen(asyncClearFut -> fut.onDone());
                    opFut.add(fut);
                }

                jobCtx.holdcc();

                opFut.listen(f -> jobCtx.callcc());

                opFut.markInitialized();

                return null;
            }

            assert opFut.isDone();

            return new ClearCachesTaskResult();
        }
    }
}

