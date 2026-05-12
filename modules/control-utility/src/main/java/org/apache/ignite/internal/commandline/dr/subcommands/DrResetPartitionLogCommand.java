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

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.dr.DrSubCommandsList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.dr.VisorDrResetTreeTask;

import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.RESET_TREES;
import static org.apache.ignite.internal.commandline.util.TopologyUtils.anyConnectableServerNode;

/**
 * The command for resetting partition log state. The task reset LWM to remove existing tree data if exists.
 */
public class DrResetPartitionLogCommand extends AbstractCommand<DrResetPartitionLogCommand.Arguments> {
    /**  */
    public static final String CACHES_ARG = "--caches";

    /** */
    public static final String NODES_ARG = "--nodes";

    /** */
    public static final String PARTITIONS_ARG = "--partitions";

    /**  */
    private DrResetPartitionLogCommand.Arguments arguments;

    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Optional<GridClientNode> firstNodeOpt = anyConnectableServerNode(client.compute());

            if (firstNodeOpt.isPresent()) {
                VisorDrResetTreeTask.VisorDrResetTreeTaskResult res;

                Predicate<? super GridClientNode> nodeFilter = arg().nodeIds == null
                    ? node -> true
                    : node -> arguments.nodeIds.contains(node.consistentId().toString());

                List<UUID> nodes = client.compute().nodes().stream()
                    .filter(node -> !node.isClient())
                    .filter(nodeFilter)
                    .map(GridClientNode::nodeId).collect(Collectors.toList());

                VisorTaskArgument<?> visorArg = new VisorTaskArgument<>(
                    nodes,
                    convertArguments(),
                    false
                );

                res = client.compute()
                    .projection(firstNodeOpt.get())
                    .execute(
                        VisorDrResetTreeTask.class.getName(),
                        visorArg
                    );

                printResult(res, logger);
            }
            else
                logger.warning("No nodes found in topology, command won't be executed.");
        }
        catch (Throwable t) {
            logger.severe("Failed to execute persistence command='" + RESET_TREES + "'");
            logger.severe(CommandLogger.errorMessage(t));

            throw t;
        }

        return null;
    }

    private VisorDrResetTreeTask.VisorDrResetTreeTaskArgs convertArguments() {
        return new VisorDrResetTreeTask.VisorDrResetTreeTaskArgs(arguments.caches, arguments.partitions);
    }

    protected void printResult(VisorDrResetTreeTask.VisorDrResetTreeTaskResult res, Logger log) {
        if (!res.messages().isEmpty())
            log.info(res.messages().stream().collect(Collectors.joining(System.lineSeparator())));
        else
            log.warning("No issues found.");
    }

    /** {@inheritDoc} */
    @Override public DrResetPartitionLogCommand.Arguments arg() {
        return arguments;
    }

    /** {@inheritDoc} */
    @Override public final void printUsage(Logger log) {
        throw new UnsupportedOperationException("printUsage");
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheNames = null;
        Set<Integer> partitions = null;
        List<String> consistentIds = null;

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("");

            switch (nextArg) {
                case CACHES_ARG:
                    cacheNames = CommandArgUtils.validateCachesArgument(argIter.nextCachesSet(nextArg), RESET_TREES.toString());
                    break;

                case PARTITIONS_ARG:
                    Set<String> partitionsArgs = argIter.nextNonEmptyStringSet("set of partitions");
                    Set<Integer> partitions0 = new HashSet<>(partitionsArgs.size());
                    partitionsArgs.forEach(dc -> partitions0.add(Integer.parseInt(dc)));

                    if (partitions0.isEmpty())
                        throw new IllegalArgumentException("Partitions list is empty.");

                    partitions = partitions0;
                    break;

                case NODES_ARG: {
                    Set<String> ids = argIter.nextStringSet(NODES_ARG);

                    if (ids.isEmpty())
                        throw new IllegalArgumentException("Consistent ids list is empty.");

                    consistentIds = new ArrayList<>(ids);

                    break;
                }

                default:
                    throw new IllegalArgumentException("Argument " + nextArg + " is not supported.");
            }

            arguments = new DrResetPartitionLogCommand.Arguments(cacheNames, partitions, consistentIds);
        }
    }

    @Override
    public String name() {
        return DrSubCommandsList.RESET_TREES.text().toUpperCase();
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Caches. */
        private final Set<String> caches;

        /** Partitions. */
        private final Set<Integer> partitions;

        /** Nodes ids to execute task on. */
        private final List<String> nodeIds;

        public Arguments(Set<String> caches, Set<Integer> partitions, List<String> nodeIds) {
            this.caches = caches;
            this.partitions = partitions;
            this.nodeIds = nodeIds;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DrResetPartitionLogCommand.Arguments.class, this);
        }
    }
}
