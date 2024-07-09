/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.checkpoint.VisorCheckpointTask;
import org.apache.ignite.internal.visor.checkpoint.VisorCheckpointTaskResult;

import static org.apache.ignite.internal.commandline.Command.usage;
import static org.apache.ignite.internal.commandline.CommandList.CHECKPOINT;

/**
 * Command to run checkpoint on cluster
 */
public class CheckpointCommand extends AbstractCommand<Void> {
    /** */
    private static final String NODE_ID_ARG_NAME = "--node-id";

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        usage(log, "Start checkpointing process:", CHECKPOINT);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CHECKPOINT.toCommandName();
    }

    /** ID of a node to run checkpoint at */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            GridClientCompute compute = client.compute();

            // Try to find connectable server nodes
            Collection<GridClientNode> nodes;

            if (nodeId == null)
                nodes = compute.nodes((n) -> n.connectable() && !n.isClient());
            else {
                nodes = new ArrayList<>();

                nodes.add(compute.node(nodeId));
            }

            if (F.isEmpty(nodes))
                throw new GridClientDisconnectedException("Connectable nodes not found", null);

            VisorCheckpointTaskResult res = compute.projection(nodes).execute(
                VisorCheckpointTask.class.getName(),
                new VisorTaskArgument<>(nodes.stream().map(GridClientNode::nodeId).collect(Collectors.toList()), arg(), false)
            );

            if (res.isSuccess()) {
                String nodeMsgPart = nodes.size() == 1 ? 1 + " node." : res.numberOfSuccessNodes() + " nodes.";

                log.info("Checkpointing completed successfully on " + nodeMsgPart);
            }
            else
                log.info("Checkpointing completed with errors. Number of failed nodes: " + res.numberOfFailedNodes() + ".");

        }
        catch (Throwable e) {
            log.severe("Failed to execute checkpointing command='" + name() + "'");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        nodeId = null;

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("Unexpected error on parsing checkpoint command args");

            if (nextArg.equals(NODE_ID_ARG_NAME))
                nodeId = UUID.fromString(argIter.nextArg("failed to read node ID"));
        }
    }
}
