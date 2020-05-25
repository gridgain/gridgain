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

import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.rebalance.RebalanceStatusArgument;
import org.apache.ignite.internal.commandline.rebalance.RebalanceStatusOption;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.baseline.VisorBaselineNode;
import org.apache.ignite.internal.visor.rebalance.VisorRebalanceStatusGroupView;
import org.apache.ignite.internal.visor.rebalance.VisorRebalanceStatusTask;
import org.apache.ignite.internal.visor.rebalance.VisorRebalanceStatusTaskArg;
import org.apache.ignite.internal.visor.rebalance.VisorRebalanceStatusTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.REBALANCE_STATUS;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

/**
 * Command shows cluster wide rebalance status.
 */
public class RebalanceStatusCommand implements Command<RebalanceStatusArgument> {
    /** Arguments. */
    private RebalanceStatusArgument rebalanceStatusArgument;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            UUID coordinatorId = client.compute()
                //Only non client node can be coordinator.
                .nodes(node -> !node.isClient())
                .stream()
                .min(Comparator.comparingLong(GridClientNode::order))
                .map(GridClientNode::nodeId)
                .orElse(null);

            VisorRebalanceStatusTaskResult res = executeTaskByNameOnNode(
                client,
                VisorRebalanceStatusTask.class.getName(),
                toTaskArg(rebalanceStatusArgument),
                coordinatorId,
                clientCfg
            );

            if (res.isRebalanceComplited())
                logger.info("Rebalance completed.");
            else {
                logger.info("Rebalance is progressing.");
                logger.info("Nodes that are balancing now:");

                for (VisorBaselineNode node : res.getRebNodes())
                    logger.info(node.toString());
            }

            logger.info("Count of nodes that can leave cluster without losing data: " + res.getReplicatedNodeCount());

            if (rebalanceStatusArgument.isRebCacheView()) {
                if (F.isEmpty(res.getGroups()))
                    logger.info("No one cache found.");
                else {
                    logger.info("Status by chaches.");

                    for (Map.Entry<VisorRebalanceStatusGroupView, Integer> entry : res.getGroups().entrySet()) {
                        logger.info(entry.getKey().toString());
                        logger.info("Count of node can leave cluster for this group: " + entry.getValue());
                    }
                }
            }
        }

        return null;
    }

    /**
     * Convert command argument to task argument.
     *
     * @param arg Rebalance status command argument.
     * @return Argument for rebalance status task.
     */
    private static VisorRebalanceStatusTaskArg toTaskArg(RebalanceStatusArgument arg) {
        return new VisorRebalanceStatusTaskArg(arg.isRebCacheView());
    }

    /** {@inheritDoc} */
    @Override public RebalanceStatusArgument arg() {
        return rebalanceStatusArgument;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Print rebalance status:", REBALANCE_STATUS);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return REBALANCE_STATUS.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            this.rebalanceStatusArgument = new RebalanceStatusArgument.Builder().build();

            return;
        }

        RebalanceStatusArgument.Builder builderArg = new RebalanceStatusArgument.Builder();

        RebalanceStatusOption cmd = of(argIter.nextArg("Expected baseline action"));

        switch (cmd) {
            case CACHES_VIEW: {
                builderArg.setRebCacheView(true);

                break;
            }
            default:
                throw new IgniteException(cmd.toString());
        }

        rebalanceStatusArgument = builderArg.build();
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static RebalanceStatusOption of(String text) {
        for (RebalanceStatusOption cmd : RebalanceStatusOption.values()) {
            if (cmd.name().equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }
}
