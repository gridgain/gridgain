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
package org.apache.ignite.internal.commandline;

import java.util.Comparator;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.id_and_tag.ClusterIdAndTagSubcommands;
import org.apache.ignite.internal.visor.id_and_tag.VisorIdAndTagTask;
import org.apache.ignite.internal.visor.id_and_tag.VisorIdAndTagTaskArg;
import org.apache.ignite.internal.visor.id_and_tag.VisorIdAndTagTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.ID_AND_TAG;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.visor.id_and_tag.VisorIdAndTagOperation.VIEW;

/**
 *
 */
public class ClusterIdAndTagCommand implements Command<String> {
    /** */
    private static final String ERR_SUBCOMMAND_RETRIEVING =
        "Expected action for Cluster ID and tag command.";

    /** */
    private static final String ERR_UNKNOWN_SUBCOMMAND = "Unknown action for Cluster ID and tag command: ";

    /** */
    private static final String ERR_EXCESS_ARG = "No additional arguments are expected.";

    /** */
    private static final String ERR_NO_NEW_TAG_PROVIDED = "Please provide new tag.";

    /** */
    private static final String ERR_EMPTY_TAG_PROVIDED = "Please provide non-empty tag.";

    /** */
    private ClusterIdAndTagSubcommands subCmd;

    /** */
    private String newTagArg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            UUID coordinatorId = client.compute().nodes().stream()
                .min(Comparator.comparingLong(GridClientNode::order))
                .map(GridClientNode::nodeId)
                .orElse(null);

            VisorIdAndTagTaskResult res = executeTaskByNameOnNode(
                client,
                VisorIdAndTagTask.class.getName(),
                toVisorArguments(),
                coordinatorId,
                clientCfg
            );

            if (subCmd.operation() == VIEW) {
                logger.info("Cluster ID: " + res.id());
                logger.info("Cluster tag: " + res.tag());
            }
            else {
                if (res.success())
                    logger.info("Cluster tag updated successfully, old tag was: " + res.tag());
                else
                    logger.warning("Error has occurred during tag update: " + res.errorMessage());
            }
        }
        catch (Throwable e) {
            logger.severe("Failed to execute Cluster ID and tag command: ");
            logger.severe(CommandLogger.errorMessage(e));
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String arg() {
        return newTagArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {

    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            subCmd = ClusterIdAndTagSubcommands.VIEW;

            return;
        }

        if (argIter.hasNextSubArg()) {
            String subCmdName = argIter.nextArg(ERR_SUBCOMMAND_RETRIEVING);

            ClusterIdAndTagSubcommands subCmd = subCommand(subCmdName);

            if (subCmd == ClusterIdAndTagSubcommands.VIEW) {
                if (argIter.hasNextSubArg())
                    throw new IllegalArgumentException(ERR_EXCESS_ARG);
            }
            else if (subCmd == ClusterIdAndTagSubcommands.CHANGE_TAG) {
                if (!argIter.hasNextSubArg())
                    throw new IllegalArgumentException(ERR_NO_NEW_TAG_PROVIDED);

                String newTag = argIter.nextArg(ERR_SUBCOMMAND_RETRIEVING);

                if (newTag.isEmpty())
                    throw new IllegalArgumentException(ERR_EMPTY_TAG_PROVIDED);

                newTagArg = newTag;
            }
            else
                throw new IllegalArgumentException(ERR_UNKNOWN_SUBCOMMAND + subCmdName);

            this.subCmd = subCmd;
        }
    }

    /** */
    private ClusterIdAndTagSubcommands subCommand(String cmdName) {
        return ClusterIdAndTagSubcommands.of(cmdName);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return ID_AND_TAG.toCommandName();
    }

    /** */
    private VisorIdAndTagTaskArg toVisorArguments() {
        return new VisorIdAndTagTaskArg(subCmd.operation(), newTagArg);
    }
}
