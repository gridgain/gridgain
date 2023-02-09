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

import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.visor.id_and_tag.VisorClusterChangeIdTask;
import org.apache.ignite.internal.visor.id_and_tag.VisorClusterChangeIdTaskArg;
import org.apache.ignite.internal.visor.id_and_tag.VisorClusterChangeIdTaskResult;

import java.util.UUID;
import java.util.logging.Logger;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_CLUSTER_ID_AND_TAG_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.internal.commandline.CommandList.CLUSTER_CHANGE_ID;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.util.TopologyUtils.coordinatorId;

/**
 * Command to change cluster ID.
 */
public class ClusterChangeIdCommand extends AbstractCommand<UUID> {
    /** */
    private UUID newIdArg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        if (!isFeatureEnabled(IGNITE_CLUSTER_ID_AND_TAG_FEATURE))
            return null;

        try (GridClient client = Command.startClient(clientCfg)) {
            UUID coordinatorId = coordinatorId(client.compute());

            VisorClusterChangeIdTaskResult res = executeTaskByNameOnNode(
                client,
                VisorClusterChangeIdTask.class.getName(),
                toVisorArguments(),
                coordinatorId,
                clientCfg
            );

            logger.info("Cluster ID changed successfully, previous ID was: " + res.oldId());
        }
        catch (Throwable e) {
            logger.severe("Failed to change cluster ID: ");
            logger.severe(CommandLogger.errorMessage(e));

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public UUID arg() {
        return newIdArg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        if (!isFeatureEnabled(IGNITE_CLUSTER_ID_AND_TAG_FEATURE))
            return;

        Command.usage(logger, "Change cluster ID to new value:", CLUSTER_CHANGE_ID, "newIdValue", optional(CMD_AUTO_CONFIRMATION));
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg())
            throw new IllegalArgumentException("Please provide new ID.");

        newIdArg = argIter.nextUuidArg("cluster ID");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CLUSTER_CHANGE_ID.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will change cluster ID.";
    }

    /** */
    private VisorClusterChangeIdTaskArg toVisorArguments() {
        return new VisorClusterChangeIdTaskArg(newIdArg);
    }
}
