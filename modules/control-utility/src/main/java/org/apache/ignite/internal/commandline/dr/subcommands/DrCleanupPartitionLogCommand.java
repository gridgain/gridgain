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

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.ArrayList;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.dr.VisorDrCleanupTreeTask;
import org.apache.ignite.internal.visor.dr.VisorDrCleanupTreeTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrCleanupTreeTaskResult;
import org.apache.ignite.internal.visor.dr.VisorDrRebuildTreeOperation;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.CLEANUP_TREES;
import static org.apache.ignite.internal.commandline.util.TopologyUtils.anyConnectableServerNode;

/**
 * Repair partition counters command.
 */
public class DrCleanupPartitionLogCommand extends AbstractCommand<DrCleanupPartitionLogCommand.Arguments> {
    /**  */
    public static final String GROUPS_ARG = "--groups";

    /**  */
    public static final String CACHES_ARG = "--caches";

    /** */
    public static final String NODES_ARG = "--nodes";

    /**  */
    private Arguments arguments;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Optional<GridClientNode> firstNodeOpt = anyConnectableServerNode(client.compute());

            if (firstNodeOpt.isPresent()) {
                VisorDrCleanupTreeTaskResult res;

                if (arguments.nodeIds == null) {
                    res = TaskExecutor.executeTaskByNameOnNode(
                            client,
                            VisorDrCleanupTreeTask.class.getName(),
                            convertArguments(),
                            null, // Use node from clientCfg.
                            clientCfg
                    );
                }
                else {
                    VisorTaskArgument<?> visorArg = new VisorTaskArgument<>(
                            client.compute().nodes().stream().filter(
                                    node -> arguments.nodeIds.contains(node.consistentId().toString())
                            ).map(GridClientNode::nodeId).collect(Collectors.toList()),
                            convertArguments(),
                            false
                    );

                    res = client.compute()
                            .projection(firstNodeOpt.get())
                            .execute(
                                    VisorDrCleanupTreeTask.class.getName(),
                                    visorArg
                            );
                }

                printResult(res, logger);
            }
            else
                logger.warning("No nodes found in topology, command won't be executed.");
        }
        catch (Throwable t) {
            logger.severe("Failed to execute persistence command='" + CLEANUP_TREES + "'");
            logger.severe(CommandLogger.errorMessage(t));

            throw t;
        }

        return null;
    }

    private VisorDrCleanupTreeTaskArgs convertArguments() {
        return new VisorDrCleanupTreeTaskArgs(convertOperation(arguments.op), arguments.caches, arguments.groups);
    }

    private VisorDrRebuildTreeOperation convertOperation(Operation op) {
        switch (op) {
            case STATUS:
                return VisorDrRebuildTreeOperation.STATUS;
            case CANCEL:
                return VisorDrRebuildTreeOperation.CANCEL;
            default:
                return VisorDrRebuildTreeOperation.DEFAULT;
        }
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return arguments;
    }

    /** {@inheritDoc} */
    @Override public final void printUsage(Logger log) {
        throw new UnsupportedOperationException("printUsage");
    }

    protected void printResult(VisorDrCleanupTreeTaskResult res, Logger log) {
        if (!res.messages().isEmpty())
            log.info(res.messages().stream().collect(Collectors.joining(System.lineSeparator())));
        else
            log.warning("No issues found.");
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheGrps = null;
        Set<String> cacheNames = null;
        List<String> consistentIds = null;

        Operation op = Operation.of(argIter.peekNextArg());

        if (op != null)
            argIter.nextArg("");
        else
            op = Operation.DEFAULT;

        switch (op) {
            case STATUS:
            case CANCEL:
                arguments = new Arguments(op);

                return;

            case DEFAULT:
                while (argIter.hasNextSubArg()) {
                    String nextArg = argIter.nextArg("");

                    switch (nextArg) {
                        case CACHES_ARG:
                            cacheNames = CommandArgUtils.validateCachesArgument(argIter.nextCachesSet(nextArg), CLEANUP_TREES.toString());
                            break;

                        case GROUPS_ARG:
                            cacheGrps = CommandArgUtils.validateCachesArgument(argIter.nextCacheGroupsSet(nextArg), CLEANUP_TREES.toString());
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
                }

                arguments = new Arguments(cacheNames, cacheGrps, consistentIds);

                return;
            default:
                throw new IllegalArgumentException("Command is not supported.");

        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CLEANUP_TREES.text().toUpperCase();
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        private Operation op;

        /** Caches. */
        private Set<String> caches;

        /** Cache groups. */
        private Set<String> groups;

        /** Nodes ids to execute task on. */
        private List<String> nodeIds;

        public Arguments(Set<String> caches, Set<String> groups, List<String> nodeIds) {
            this.op = Operation.DEFAULT;
            this.caches = caches;
            this.groups = groups;
            this.nodeIds = nodeIds;
        }

        public Arguments(Operation op) {
            this.op = op;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }
    }

    enum Operation {
        DEFAULT,
        STATUS,
        CANCEL;

        static @Nullable Operation of(String arg) {
            if ("status".equalsIgnoreCase(arg))
                return STATUS;
            if ("cancel".equalsIgnoreCase(arg))
                return CANCEL;

            return null;
        }
    }
}
