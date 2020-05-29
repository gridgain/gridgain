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

package org.apache.ignite.internal.commandline.meta.tasks;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.commandline.cache.CheckIndexInlineSizes;
import org.apache.ignite.internal.commandline.meta.subcommands.MetadataListCommand;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

/**
 * Task for {@link MetadataListCommand} command.
 */
@GridInternal
public class MetadataRemoveTask extends VisorMultiNodeTask<MetadataTypeArgs, MetadataListResult, MetadataListResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<MetadataTypeArgs, MetadataListResult> job(MetadataTypeArgs arg) {
        return new MetadataListJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected MetadataListResult reduce0(List<ComputeJobResult> results) {
        if (results.isEmpty())
            throw new IgniteException("Empty job results");

        if (results.size() > 1)
            throw new IgniteException("Invalid job results: " + results);

        if (results.get(0).getException() != null)
            throw results.get(0).getException();
        else
            return results.get(0).getData();
    }

    /**
     * Job for {@link CheckIndexInlineSizes} command.
     */
    private static class MetadataListJob extends VisorJob<MetadataTypeArgs, MetadataListResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected MetadataListJob(@Nullable MetadataTypeArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected MetadataListResult run(@Nullable MetadataTypeArgs arg) throws IgniteException {
            ignite.context().security().authorize(null, SecurityPermission.ADMIN_OPS);

            assert Objects.nonNull(arg);

            int typeId;

            if (arg.typeId() != null)
                typeId = arg.typeId();
            else
                typeId = ignite.context().cacheObjects().typeId(arg.typeName());

            MetadataListResult res = new MetadataListResult(Collections.singleton(
                ((CacheObjectBinaryProcessorImpl)ignite.context().cacheObjects()).binaryMetadata(typeId)));

            ignite.context().cacheObjects().removeType(typeId);

            return res;
        }
    }
}
