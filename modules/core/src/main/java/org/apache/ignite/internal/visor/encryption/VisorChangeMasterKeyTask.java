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
 * The task for changing the master key.
 *
 * @see IgniteEncryption#changeMasterKey(String)
 */
@GridInternal
public class VisorChangeMasterKeyTask extends VisorOneNodeTask<String, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<String, String> job(String arg) {
        return new VisorChangeMasterKeyJob(arg, debug);
    }

    /** The job for changing the master key. */
    private static class VisorChangeMasterKeyJob extends VisorJob<String, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteFuture opFut;

        /** Job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * Create job with specified argument.
         *
         * @param masterKeyName Master key name.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorChangeMasterKeyJob(String masterKeyName, boolean debug) {
            super(masterKeyName, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(String masterKeyName) throws IgniteException {
            if (opFut == null) {
                opFut = ignite.encryption().changeMasterKey(masterKeyName);

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

            return "The master key changed.";
        }
    }
}
