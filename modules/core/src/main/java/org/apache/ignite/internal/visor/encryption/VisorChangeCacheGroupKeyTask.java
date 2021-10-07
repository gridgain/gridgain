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

package org.apache.ignite.internal.visor.encryption;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;

/**
 * The task for changing the encryption key of the cache group.
 *
 * @see IgniteEncryption#changeCacheGroupKey(Collection)
 */
@GridInternal
public class VisorChangeCacheGroupKeyTask extends VisorOneNodeTask<VisorCacheGroupEncryptionTaskArg, Void> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorCacheGroupEncryptionTaskArg, Void> job(VisorCacheGroupEncryptionTaskArg arg) {
        return new VisorChangeCacheGroupKeyJob(arg, debug);
    }

    /** The job for changing the encryption key of the cache group. */
    private static class VisorChangeCacheGroupKeyJob extends VisorJob<VisorCacheGroupEncryptionTaskArg, Void> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteFuture opFut;

        /** Job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorChangeCacheGroupKeyJob(VisorCacheGroupEncryptionTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorCacheGroupEncryptionTaskArg taskArg) throws IgniteException {
            if (opFut == null) {
                opFut = ignite.encryption().changeCacheGroupKey(Collections.singleton(taskArg.groupName()));

                if (opFut.isDone())
                    opFut.get();
                else {
                    jobCtx.holdcc();

                    opFut.listen(new IgniteInClosure<IgniteFuture>() {
                        @Override public void apply(IgniteFuture f) {
                            jobCtx.callcc();
                        }
                    });
                }
            }

            opFut.get();

            return null;
        }
    }
}
