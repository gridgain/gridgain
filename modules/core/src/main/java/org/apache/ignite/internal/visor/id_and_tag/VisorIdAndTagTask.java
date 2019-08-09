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
package org.apache.ignite.internal.visor.id_and_tag;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.id_and_tag.VisorIdAndTagOperation.CHANGE_TAG;
import static org.apache.ignite.internal.visor.id_and_tag.VisorIdAndTagOperation.VIEW;

/**
 *
 */
@GridInternal
@GridVisorManagementTask
public class VisorIdAndTagTask extends VisorOneNodeTask<VisorIdAndTagTaskArg, VisorIdAndTagTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorIdAndTagTaskArg, VisorIdAndTagTaskResult> job(VisorIdAndTagTaskArg arg) {
        return new VisorIdAndTagJob(arg, debug);
    }

    private static class VisorIdAndTagJob extends VisorJob<VisorIdAndTagTaskArg, VisorIdAndTagTaskResult> {

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        VisorIdAndTagJob(
            @Nullable VisorIdAndTagTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorIdAndTagTaskResult run(@Nullable VisorIdAndTagTaskArg arg) throws IgniteException {
            switch (arg.operation()) {
                case VIEW:
                    return view();

                case CHANGE_TAG:
                    return update(arg.newTag());

                default:
                    return view();
            }
        }

        /** */
        private VisorIdAndTagTaskResult view() {
            IgniteClusterEx cl = ignite.cluster();

            return new VisorIdAndTagTaskResult(VIEW.ordinal(), cl.id(), cl.tag(), null, null);
        }

        /**
         * @param newTag New tag.
         */
        private VisorIdAndTagTaskResult update(String newTag) {
            IgniteClusterEx cl = ignite.cluster();

            boolean success = false;
            String errMsg = null;

            String oldTag = cl.tag();

            try {
                cl.tag(newTag);

                success = true;
            }
            catch (IgniteCheckedException e) {
                errMsg = e.getMessage();
            }

            return new VisorIdAndTagTaskResult(CHANGE_TAG.ordinal(), null, oldTag, Boolean.valueOf(success), errMsg);
        }
    }
}
