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
import org.apache.ignite.internal.visor.cache.affinityView.VisorAffinityViewTask;
import org.apache.ignite.internal.visor.cache.affinityView.VisorAffinityViewTaskArg;
import org.apache.ignite.internal.visor.cache.affinityView.VisorAffinityViewTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.AFFINITY;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.CURRENT;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.DIFF;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.GROUP_NAME;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.IDEAL;

/**
 * Command for accessing affinity assignment
 */
public class AffinityViewCommand implements Command<AffinityViewCommand.Arguments> {

    /** {@inheritDoc} */
    @Override
    public void printUsage(Logger logger) {
        String commandDesc = "Print information about affinity assignment for cache group.";

        Map<String, String> paramsDesc = new HashMap<>();

        paramsDesc.put(CURRENT.toString(), "print current affinity assignment.");
        paramsDesc.put(IDEAL.toString(), "print affinity assignment calculated by affinity function.");
        paramsDesc.put(DIFF.toString(), "print ids of partitions different from ideal assignment.");
        paramsDesc.put(GROUP_NAME.toString(), "group name.");

        String[] args = new String[2];

        args[0] = or(CURRENT, IDEAL, DIFF);
        args[1] = GROUP_NAME + " cache_group_name";

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

        /**
         * @param mode Mode.
         * @param cacheGrpName Cache group name.
         */
        public Arguments(AffinityViewCommandArg mode, String cacheGrpName) {
            this.mode = mode;
            this.cacheGrpName = cacheGrpName;
        }
    }

    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override
    public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        VisorAffinityViewTaskResult res;

        try (GridClient client = Command.startClient(clientCfg)) {
            VisorAffinityViewTaskArg.Mode mode = VisorAffinityViewTaskArg.Mode.fromAVCmdArg(args.mode);

            VisorAffinityViewTaskArg taskArg = new VisorAffinityViewTaskArg(args.cacheGrpName, mode);

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
    private void printAssignment(List<List<ClusterNode>> assignment, Logger logger) {
        Map<ClusterNode, List<Integer>> nodeMap = new HashMap<>();

        for (int partNum = 0; partNum < assignment.size(); partNum++) {
            for (ClusterNode node: assignment.get(partNum)) {
                if (!nodeMap.containsKey(node)) {
                    ArrayList<Integer> initList = new ArrayList<>();

                    initList.add(partNum);

                    nodeMap.put(node, initList);

                    continue;
                }

                nodeMap.get(node).add(partNum);
            }
        }

        List<ClusterNode> nodes = new ArrayList<>(nodeMap.keySet());

        nodes.sort(Comparator.comparing(node -> node.consistentId().toString()));

        for (ClusterNode node: nodes) {
            logger.info(node.consistentId() + ":");

            List<Integer> partIdsForNode = nodeMap.get(node);

            logger.info(INDENT + "Partitions num: " + partIdsForNode.size());

            logger.info(INDENT + "Partitions ids:");
            String ids = DOUBLE_INDENT + partIdsForNode.stream()
                                                       .map(partNum -> Integer.toString(partNum))
                                                       .collect(Collectors.joining("; "));
            logger.info(ids);

            logger.info("");
        }
    }

    /**
     * Prints primary partitions' ids different to ideal assignment.
     * @param diff Set of partitions' ids
     * @param logger Logger to use
     */
    private void printDiff(Set<Integer> diff, Logger logger) {
        logger.info("Primary partitions different to ideal assignment:");

        if (diff.isEmpty())
            logger.info(INDENT + "Not found.");
        else {
            String ids = INDENT + diff.stream()
                                      .sorted()
                                      .map(partNum -> Integer.toString(partNum))
                                      .collect(Collectors.joining("; "));
            logger.info(ids);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void parseArguments(CommandArgIterator argIter) {
        AffinityViewCommandArg mode = null;
        String cacheGrpName = "";

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

                default:
                    throw new IllegalArgumentException("Unsupported argument: " + avCmdArg);
            }
        }

        args = new Arguments(mode, cacheGrpName);

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
    @Override
    public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return AFFINITY.text().toUpperCase();
    }
}
