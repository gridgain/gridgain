package org.apache.ignite.internal.commandline.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.internal.visor.cache.affinityView.AffinityViewerTaskResult;
import org.apache.ignite.internal.visor.cache.affinityView.VisorAffinityViewTask;
import org.apache.ignite.internal.visor.cache.affinityView.VisorAffinityViewTaskArg;

import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.AFFINITY;
import static org.apache.ignite.internal.commandline.cache.argument.AffinityViewCommandArg.GROUP_NAME;

/**
 * Command for accessing affinity assignment
 */
public class AffinityViewCommand implements Command<AffinityViewCommand.Arguments> {

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
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorAffinityViewTaskArg.Mode mode = VisorAffinityViewTaskArg.Mode.fromAVCmdArg(args.mode);

            VisorAffinityViewTaskArg taskArg = new VisorAffinityViewTaskArg(args.cacheGrpName, mode);

            AffinityViewerTaskResult res = TaskExecutor.executeTask(client, VisorAffinityViewTask.class, taskArg, clientCfg);

            // Null check for mode is made in validateArgs()
            switch (mode) {
                case IDEAL: //fallthrough
                case CURRENT:
                    printAssignment(res.getAssignment(), logger);
                    break;

                case DIFF:
                    printDiff(logger);
                    break;

                default:
                    throw new IgniteException("Unexpected mode: " + mode);
            }

            logger.info("TODO: add output");
        }
        return null;
    }

    private void printAssignment(List<List<ClusterNode>> assignment, Logger logger) {
        Map<ClusterNode, List<Integer>> nodeMap = new HashMap<>();

        //TODO: use explicit counter

        for (int partNum = 1; partNum < assignment.size() + 1; partNum++) {
            for (ClusterNode node: assignment.get(partNum - 1)) {
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

            logger.info(INDENT + "Partitions num:");
            logger.info(INDENT + INDENT + partIdsForNode.size());

            logger.info(INDENT + "Partitions ids:");
            //TODO: replace INDENT + INDENT after rebase to master
            String sb = INDENT + INDENT + partIdsForNode.stream()
                                               .map(partNum -> Integer.toString(partNum))
                                               .collect(Collectors.joining("; "));
            logger.info(sb);

            logger.info("");
        }

        //TODO: complete
        logger.info("");
    }

    //TODO: implement
    private void printDiff(Logger logger) {

    }

    /** {@inheritDoc} */
    @Override
    public void printUsage(Logger logger) {
        //TODO: implement

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

        validateArgs(args);
    }

    /**
     * Checks that arguments are set properly
     * @param args Arguments to check
     * @throws IllegalArgumentException if some args not set
     */
    private void validateArgs(Arguments args) throws IllegalArgumentException {
        if (args.cacheGrpName.isEmpty())
            throw new IllegalArgumentException(GROUP_NAME.trimmedArgName() + " not set");
        else if (args.mode == null)
            throw new IllegalArgumentException("Mode not set");

    }
}
