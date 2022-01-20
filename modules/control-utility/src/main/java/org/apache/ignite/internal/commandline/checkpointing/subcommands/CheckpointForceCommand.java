/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.checkpointing.subcommands;

import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.checkpointing.CheckpointAbstractSubCommand;
import org.apache.ignite.internal.commandline.checkpointing.CheckpointingForceResult;
import org.apache.ignite.internal.commandline.checkpointing.CheckpointingForceTask;
import org.apache.ignite.internal.commandline.checkpointing.CheckpointingSubCommandsList;
import org.apache.ignite.internal.commandline.meta.subcommands.VoidDto;

/**
 * Checkpoint force command descriptor.
 *
 * Starts manual checkpoint process on a whole cluster
 */
public class CheckpointForceCommand extends CheckpointAbstractSubCommand<VoidDto, CheckpointingForceResult> {

    /** {@inheritDoc} */
    @Override protected String taskName() {
        return CheckpointingForceTask.class.getName();
    }

    @Override protected VoidDto parseArguments0(CommandArgIterator argIter) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void printResult(CheckpointingForceResult res, Logger log) {
        if (res.isSuccess())
            log.info("Checkpointing completed successfully on " + res.numberOfSuccessNodes() + " nodes.");
        else
            log.info("Checkpointing completed with errors. Number of failed nodes: " + res.numberOfFailedNodes() + ".");

    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CheckpointingSubCommandsList.FORCE.text();
    }

}
