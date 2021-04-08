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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.encryption.VisorReencryptionSuspendTask.VisorReencryptionSuspendResumeJobResult;
import org.jetbrains.annotations.Nullable;

/**
 * Resume re-encryption of the cache group.
 */
@GridInternal
public class VisorReencryptionResumeTask extends VisorCacheGroupEncryptionTask<Boolean> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorCacheGroupEncryptionTaskArg, VisorSingleFieldDto<Boolean>> job(
        VisorCacheGroupEncryptionTaskArg arg) {
        return new VisorReencryptionResumeJob(arg, debug);
    }

    /** The job to resume re-encryption of the cache group. */
    private static class VisorReencryptionResumeJob extends VisorReencryptionBaseJob<Boolean> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorReencryptionResumeJob(@Nullable VisorCacheGroupEncryptionTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorSingleFieldDto<Boolean> run0(CacheGroupContext grp) throws IgniteCheckedException {
            return new VisorReencryptionSuspendResumeJobResult().value(
                ignite.context().encryption().resumeReencryption(grp.groupId()));
        }
    }
}
