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


package org.apache.ignite.internal.commandline.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.visor.cache.affinityView.VisorAffinityViewTask;
import org.apache.ignite.internal.visor.cache.affinityView.VisorAffinityViewTaskArg;
import org.apache.ignite.internal.visor.cache.affinityView.VisorAffinityViewTaskResult;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.AFFINITY;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.CURRENT;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.DIFF;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.GROUP_NAME;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.IDEAL;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.SOURCE_NODE_ID;

/**
 * Command for accessing affinity assignment
 */
public class AffinityViewCommand implements Command<AffinityViewCommand.Arguments> {

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String commandDesc = "Print information about affinity assignment for cache group.";

        Map<String, String> paramsDesc = new HashMap<>();

        paramsDesc.put(CURRENT.toString(), "print current affinity assignment.");
        paramsDesc.put(IDEAL.toString(), "print affinity assignment calculated by affinity function.");
        paramsDesc.put(DIFF.toString(), "print ids of partitions different from ideal assignment.");
        paramsDesc.put(GROUP_NAME.toString(), "group name.");
        paramsDesc.put(SOURCE_NODE_ID.toString(), "affinity source node id.");

        String[] args = new String[3];

        args[0] = or(CURRENT, IDEAL, DIFF);
        args[1] = GROUP_NAME + " cache_group_name";
        args[2] = optional(SOURCE_NODE_ID + " affinity_src_node");

        usageCache(logger, AFFINITY, commandDesc, paramsDesc, args);
    }

    /**
     *
     */
    public class Arguments {
        /** */
        private final AffinityViewCommandArg mode;
        /** */
        private final String cacheGrpName;
        /** */
        private final UUID affinitySrcNodeId;

        /**
         * @param mode Mode.
         * @param cacheGrpName Cache group name.
         * @param affinitySrcNodeId Affinity source node id.
         */
        public Arguments(AffinityViewCommandArg mode, String cacheGrpName, UUID affinitySrcNodeId) {
            this.mode = mode;
            this.cacheGrpName = cacheGrpName;
            this.affinitySrcNodeId = affinitySrcNodeId;
        }
    }

    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        VisorAffinityViewTaskResult res;

        try (GridClient client = Command.startClient(clientCfg)) {
            VisorAffinityViewTaskArg.Mode mode = VisorAffinityViewTaskArg.Mode.fromAVCmdArg(args.mode);

            VisorAffinityViewTaskArg taskArg = new VisorAffinityViewTaskArg(args.cacheGrpName, mode, args.affinitySrcNodeId);

            res = TaskExecutor.executeTask(client, VisorAffinityViewTask.class, taskArg, clientCfg);

            // Null check for mode is made in validateArgs()
            switch (mode) {
                case IDEAL: //fallthrough
                case CURRENT:
                    printAssignment(res.getAssignment(), logger);
                    break;

                case DIFF:
                    printDiff(res.getPrimariesDifferentToIdeal(), logger);
                    break;

                default:
                    throw new IgniteException("Unexpected mode: " + mode);
            }

            logger.info("");
        }

        return res;
    }

    /**
     * Prints affinity assignment.
     * @param assignment Assignment to print.
     * @param logger Logger to use.
     */
    private static void printAssignment(Map<ClusterNode, IgniteBiTuple<char[], char[]>> assignment, Logger logger) {
        List<ClusterNode> nodes = new ArrayList<>(assignment.keySet());

        nodes.sort(Comparator.comparing(node -> node.consistentId().toString()));

        for (ClusterNode node: nodes) {
            logger.info(node.getClass().getSimpleName() + " [id=" + node.id() +
                ", addrs=" + node.addresses() +
                ", order=" + node.order() +
                ", ver=" + node.version() +
                ", isClient=" + node.isClient() +
                ", consistentId=" + node.consistentId() +
                "]");

            IgniteBiTuple<char[], char[]> partitions = assignment.get(node);

            printPartIds("Primary", partitions.get1(), logger);

            printPartIds("Backup", partitions.get2(), logger);
        }
    }

    /**
     * Prints compacted partitions' ids to {@code logger}
     * @param ids to print
     * @param logger to use
     */
    private static void printPartIds(String partType, char[] ids, Logger logger) {
        logger.info(INDENT + partType + " partitions num: " + ids.length);

        if (ids.length != 0) {
            logger.info(INDENT + partType + " partitions ids:");

            List<Integer> partIdsForNodeList = new ArrayList<>();

            for (char c : ids)
                partIdsForNodeList.add((int)c);

            logger.info(DOUBLE_INDENT + GridToStringBuilder.compact(partIdsForNodeList));
        }

        logger.info("");
    }

    /**
     * Prints primary partitions' ids different to ideal assignment.
     * @param diff Set of partitions' ids
     * @param logger Logger to use
     */
    private static void printDiff(Set<Integer> diff, Logger logger) {
        logger.info("Primary partitions different to ideal assignment:");

        if (diff.isEmpty())
            logger.info(INDENT + "Not found.");
        else {
            List<Integer> diffIds = diff.stream()
                                        .sorted()
                                        .collect(Collectors.toList());

            logger.info(INDENT + GridToStringBuilder.compact(diffIds));
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        AffinityViewCommandArg mode = null;
        String cacheGrpName = "";
        UUID affinitySrcNodeId = null;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("");

            AffinityViewCommandArg avCmdArg = CommandArgUtils.of(arg, AffinityViewCommandArg.class);

            if (avCmdArg == null)
                throw new IllegalArgumentException("Argument " + arg + " is unexpected here");

            switch (avCmdArg) {
                case IDEAL: //fallthrough
                case CURRENT: //fallthrough
                case DIFF:
                    mode = avCmdArg;
                    break;

                case GROUP_NAME:
                    cacheGrpName = argIter.nextArg("Group name expected after " + GROUP_NAME);
                    break;

                case SOURCE_NODE_ID:
                    affinitySrcNodeId = UUID.fromString(argIter.nextArg("Node id expected after " + SOURCE_NODE_ID));
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported argument: " + avCmdArg);
            }
        }

        args = new Arguments(mode, cacheGrpName, affinitySrcNodeId);

        validateArgs();
    }

    /**
     * Checks that arguments are set properly
     * @throws IllegalArgumentException if some args not set
     */
    private void validateArgs() throws IllegalArgumentException {
        if (args.cacheGrpName.isEmpty())
            throw new IllegalArgumentException(GROUP_NAME.trimmedArgName() + " not set");
        else if (args.mode == null)
            throw new IllegalArgumentException("Mode not set");
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return AFFINITY.text().toUpperCase();
    }
}
