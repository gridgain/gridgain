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
import org.apache.ignite.internal.visor.id_and_tag.VisorClusterChangeTagTask;
import org.apache.ignite.internal.visor.id_and_tag.VisorIdAndTagTaskArg;
import org.apache.ignite.internal.visor.id_and_tag.VisorClusterChangeTagTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.CLUSTER_CHANGE_TAG;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

/**
 * Command to access cluster ID and tag functionality.
 */
public class ClusterChangeTagCommand implements Command<String> {
    /** */
    private static final String ERR_NO_NEW_TAG_PROVIDED = "Please provide new tag.";

    /** */
    private static final String ARG_NAME = "newTag";

    /** */
    private static final String ERR_EMPTY_TAG_PROVIDED = "Please provide non-empty tag.";

    /** */
    private String newTagArg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            UUID coordinatorId = client.compute().nodes().stream()
                .min(Comparator.comparingLong(GridClientNode::order))
                .map(GridClientNode::nodeId)
                .orElse(null);

            VisorClusterChangeTagTaskResult res = executeTaskByNameOnNode(
                client,
                VisorClusterChangeTagTask.class.getName(),
                toVisorArguments(),
                coordinatorId,
                clientCfg
            );

            if (res.success())
                    logger.info("Cluster tag updated successfully, old tag was: " + res.tag());
                else
                    logger.warning("Error has occurred during tag update: " + res.errorMessage());
        }
        catch (Throwable e) {
            logger.severe("Failed to execute Cluster ID and tag command: ");
            logger.severe(CommandLogger.errorMessage(e));

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String arg() {
        return newTagArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Change cluster tag to new value:", CLUSTER_CHANGE_TAG, "newTag", optional(CMD_AUTO_CONFIRMATION));
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg())
            throw new IllegalArgumentException(ERR_NO_NEW_TAG_PROVIDED);

        String argName = argIter.nextArg(ERR_NO_NEW_TAG_PROVIDED);

        if (!ARG_NAME.equalsIgnoreCase(argName))
            throw new IllegalArgumentException("Expected correct argument");

        String newTag = argIter.nextArg(ERR_NO_NEW_TAG_PROVIDED);

        System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] "  + System.currentTimeMillis() +
            " newTag: " + newTag);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CLUSTER_CHANGE_TAG.toCommandName();
    }

    /** */
    private VisorIdAndTagTaskArg toVisorArguments() {
        return new VisorIdAndTagTaskArg(newTagArg);
    }
}
