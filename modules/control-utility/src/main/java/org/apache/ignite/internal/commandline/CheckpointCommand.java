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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.checkpoint.VisorCheckpointTask;
import org.apache.ignite.internal.visor.checkpoint.VisorCheckpointTaskResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.commandline.Command.usage;
import static org.apache.ignite.internal.commandline.CommandList.CHECKPOINT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;

/**
 * Command to run checkpoint on the cluster.
 */
public class CheckpointCommand extends AbstractCommand<Void> {
    /** */
    private static final String NODE_ID_ARG_NAME = "--node-id";

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        usage(log, "Start checkpointing on a node with specified nodeId. " +
            "If nodeId is not specified checkpoint is started on all nodes:",
            CHECKPOINT,
            optional(NODE_ID_ARG_NAME, "<nodeId>"));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CHECKPOINT.toCommandName();
    }

    /** ID of a node to run checkpoint at. If null, checkpoint is run on all nodes. */
    private @Nullable UUID nodeId;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            GridClientCompute compute = client.compute();

            // Try to find connectable server nodes
            Collection<GridClientNode> nodes;

            if (nodeId == null)
                nodes = compute.nodes((n) -> n.connectable() && !n.isClient());
            else {
                GridClientNode node = compute.node(nodeId);

                if (node == null)
                    throw new IllegalArgumentException(collectNodesInfo(compute));

                nodes = Collections.singletonList(node);
            }

            if (F.isEmpty(nodes))
                throw new GridClientDisconnectedException("Connectable nodes not found", null);

            VisorCheckpointTaskResult res = compute.projection(nodes).execute(
                VisorCheckpointTask.class.getName(),
                new VisorTaskArgument<>(nodes.stream().map(GridClientNode::nodeId).collect(Collectors.toList()), arg(), false)
            );

            if (res.isSuccess()) {
                String nodesMsgPart = nodes.size() == 1 ? "1 node." : res.numberOfSuccessNodes() + " nodes.";

                log.info("Checkpointing completed successfully on " + nodesMsgPart);
            }
            else {
                String nodesMsgPart = nodes.size() == 1 ? "1 node." : res.numberOfFailedNodes() + " nodes.";

                log.info("Checkpointing completed with errors on " + nodesMsgPart);
            }

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
                nodeId = UUID.fromString(argIter.nextArg("Failed to read node ID"));
        }
    }

    /**  */
    private String collectNodesInfo(GridClientCompute compute) throws GridClientException {
        SB b = new SB();

        b.a("Node with id=" + nodeId + " is not found in the topology").nl()
            .a("Available cluster nodes:").nl();

        compute.nodes((node) -> node.connectable() && !node.isClient()).forEach(node -> b.a(node.nodeId()).nl());

        return b.toString();
    }
}
