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

import java.util.Optional;
import java.util.Set;
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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.dr.VisorDrRebuildTreeOperation;
import org.apache.ignite.internal.visor.dr.VisorDrRebuildTreeTask;
import org.apache.ignite.internal.visor.dr.VisorDrRebuildTreeTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrRebuildTreeTaskResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.REBUILD_TREES;
import static org.apache.ignite.internal.commandline.util.TopologyUtils.anyConnectableServerNode;

/**
 * Repair partition counters command.
 */
public class DrRebuildPartitionLogCommand extends AbstractCommand<DrRebuildPartitionLogCommand.Arguments> {
    /**  */
    public static final String GROUPS_ARG = "--groups";

    /**  */
    public static final String CACHES_ARG = "--caches";

    /**  */
    private Arguments arguments;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Optional<GridClientNode> firstNodeOpt = anyConnectableServerNode(client.compute());

            if (firstNodeOpt.isPresent()) {
                VisorDrRebuildTreeTaskResult res = executeTaskByNameOnNode(client,
                    VisorDrRebuildTreeTask.class.getName(),
                    convertArguments(),
                    null, // Use node from clientCfg.
                    clientCfg
                );

                printResult(res, logger);
            }
            else
                logger.warning("No nodes found in topology, command won't be executed.");
        }
        catch (Throwable t) {
            logger.severe("Failed to execute persistence command='" + REBUILD_TREES + "'");
            logger.severe(CommandLogger.errorMessage(t));

            throw t;
        }

        return null;
    }

    private VisorDrRebuildTreeTaskArgs convertArguments() {
        return new VisorDrRebuildTreeTaskArgs(convertOperation(arguments.op), arguments.caches, arguments.groups);
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

    protected void printResult(VisorDrRebuildTreeTaskResult res, Logger log) {
        if (!res.messages().isEmpty())
            log.info(res.messages().stream().collect(Collectors.joining(System.lineSeparator())));
        else
            log.warning("No issues found.");
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheGrps = null;
        Set<String> cacheNames = null;

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
                            cacheNames = CommandArgUtils.validateCachesArgument(argIter.nextCachesSet(nextArg), REBUILD_TREES.toString());
                            break;

                        case GROUPS_ARG:
                            cacheGrps = CommandArgUtils.validateCachesArgument(argIter.nextCacheGroupsSet(nextArg), REBUILD_TREES.toString());
                            break;

                        default:
                            throw new IllegalArgumentException("Argument " + nextArg + " is not supported.");
                    }
                }

                arguments = new Arguments(cacheNames, cacheGrps);

                return;
            default:
                throw new IllegalArgumentException("Command is not supported.");

        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return REBUILD_TREES.text().toUpperCase();
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

        public Arguments(Set<String> caches, Set<String> groups) {
            this.op = Operation.DEFAULT;
            this.caches = caches;
            this.groups = groups;
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
