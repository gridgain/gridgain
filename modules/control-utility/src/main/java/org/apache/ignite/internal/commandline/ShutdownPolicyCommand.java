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

package org.apache.ignite.internal.commandline;

import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.shutdown.ShutdownPolicyArgument;
import org.apache.ignite.internal.visor.shutdown.VisorShutdownPolicyTask;
import org.apache.ignite.internal.visor.shutdown.VisorShutdownPolicyTaskArg;
import org.apache.ignite.internal.visor.shutdown.VisorShutdownPolicyTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.SHUTDOWN_POLICY;
import static org.apache.ignite.internal.commandline.util.TopologyUtils.coordinatorId;

/**
 * Command for change or display policy for shutdown.
 */
public class ShutdownPolicyCommand extends AbstractCommand<ShutdownPolicyArgument> {
    /** Arguments. */
    private ShutdownPolicyArgument shutdownPolicyArgument;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            UUID coordinatorId = coordinatorId(client.compute());

            VisorShutdownPolicyTaskResult res = TaskExecutor.executeTaskByNameOnNode(
                client,
                VisorShutdownPolicyTask.class.getName(),
                toTaskArg(shutdownPolicyArgument),
                coordinatorId,
                clientCfg
            );

            logger.info("Cluster shutdown policy is " + res.getShutdown());
        }

        return null;
    }

    /**
     * Convert command argument to task argument.
     *
     * @param arg Shutdown policy command argument.
     * @return Argument for shutdown policy task.
     */
    private static VisorShutdownPolicyTaskArg toTaskArg(ShutdownPolicyArgument arg) {
        return new VisorShutdownPolicyTaskArg(arg.getShutdown());
    }

    /** {@inheritDoc} */
    @Override public ShutdownPolicyArgument arg() {
        return shutdownPolicyArgument;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Set or display shutdown policy:", SHUTDOWN_POLICY,
            CommandLogger.optional(CommandLogger.join("|", ShutdownPolicy.values())));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SHUTDOWN_POLICY.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            this.shutdownPolicyArgument = new ShutdownPolicyArgument.Builder().build();

            return;
        }

        ShutdownPolicyArgument.Builder builderArg = new ShutdownPolicyArgument.Builder();

        builderArg.setShutdownPolicy(ShutdownPolicy.valueOf(argIter.nextArg("Shutdown policy is expected")));

        shutdownPolicyArgument = builderArg.build();
    }
}
