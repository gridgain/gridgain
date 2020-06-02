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

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.commandline.cache.CheckIndexInlineSizes;
import org.apache.ignite.internal.commandline.meta.subcommands.MetadataAbstractSubCommand;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

/**
 * Task for {@link MetadataRemoveTask} commands.
 */
@GridInternal
public class MetadataDropAllThinConnectionsTask extends VisorMultiNodeTask<
    MetadataAbstractSubCommand.VoidDto,
    MetadataAbstractSubCommand.VoidDto,
    MetadataAbstractSubCommand.VoidDto> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<MetadataAbstractSubCommand.VoidDto, MetadataAbstractSubCommand.VoidDto> job(MetadataAbstractSubCommand.VoidDto arg) {
        return new MetadataListJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected MetadataAbstractSubCommand.VoidDto reduce0(List<ComputeJobResult> results) {
        if (results.get(0).getException() != null)
            throw results.get(0).getException();
        else
            return null;
    }

    /**
     * Job for {@link CheckIndexInlineSizes} command.
     */
    private static class MetadataListJob extends VisorJob<
        MetadataAbstractSubCommand.VoidDto,
        MetadataAbstractSubCommand.VoidDto> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected MetadataListJob(@Nullable MetadataAbstractSubCommand.VoidDto arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected MetadataAbstractSubCommand.VoidDto run(@Nullable MetadataAbstractSubCommand.VoidDto arg)
            throws IgniteException {
            ignite.context().security().authorize(null, SecurityPermission.ADMIN_METADATA_OPS);

            ignite.context().sqlListener().closeAllSessions();

            return null;
        }
    }
}
