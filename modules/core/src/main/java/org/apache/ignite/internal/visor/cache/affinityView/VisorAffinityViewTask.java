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


package org.apache.ignite.internal.visor.cache.affinityView;

import java.util.Collections;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheAffinitySharedManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task for fetching affinity assignment for cache group.
 */
@GridInternal
public class VisorAffinityViewTask extends VisorOneNodeTask<VisorAffinityViewTaskArg, VisorAffinityViewTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Job for fetching affinity assignment from custer
     */
    private static class VisorAffinityViewJob extends VisorJob<VisorAffinityViewTaskArg, VisorAffinityViewTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug flag.
         */
        VisorAffinityViewJob(VisorAffinityViewTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override
        protected VisorAffinityViewTaskResult run(@Nullable VisorAffinityViewTaskArg arg) throws IgniteException {
            if (arg == null)
                throw new IgniteException("VisorAffinityViewTaskArg is null");

            CacheAffinitySharedManager affinitySharedManager = ignite.context()
                                              .cache()
                                              .context()
                                              .affinity();

            AffinityAssignment affAss;

            int gropuId = CU.cacheId(arg.getCacheGrpName());

            if (affinitySharedManager.cacheGroupExists(gropuId))
                affAss = affinitySharedManager.affinity(gropuId).getLatestAffinityAssignment();
            else
                throw new IgniteException("Group " + arg.getCacheGrpName() + " not found");

            VisorAffinityViewTaskArg.Mode mode = arg.getMode();

            switch (mode) {
                case CURRENT:
                    return new VisorAffinityViewTaskResult(affAss.assignment(), Collections.emptySet());

                case IDEAL:
                    return new VisorAffinityViewTaskResult(affAss.idealAssignment(), Collections.emptySet());

                case DIFF:
                    return new VisorAffinityViewTaskResult(Collections.emptyList(), affAss.partitionPrimariesDifferentToIdeal());

                default:
                    throw new IgniteException("Unexpected mode: " + mode);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    protected VisorAffinityViewJob job(VisorAffinityViewTaskArg arg) {
        return new VisorAffinityViewJob(arg, debug);
    }
}
