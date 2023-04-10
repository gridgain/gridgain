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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.ResetLostPartitionsCommandArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionGroup;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionNode;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionPartition;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTask;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskResult;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTask;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskArg;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskResult;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;

import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.RESET_LOST_PARTITIONS;
import static org.apache.ignite.internal.commandline.cache.argument.ResetLostPartitionsCommandArg.ALL_CACHES_MODE;

/**
 * Command for reset lost partition state.
 */
public class ResetLostPartitions extends AbstractCommand<Set<String>> {
    private static String CACHES = "cacheName1,...,cacheNameN";

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String description = "Reset the state of lost partitions for the specified or all affected caches.";

        usageCache(logger, RESET_LOST_PARTITIONS, description, null, or(CACHES, ALL_CACHES_MODE));
    }

    /**
     * Command argument. Caches which lost partitions should be reset.
     */
    private Set<String> caches;

    /** {@inheritDoc} */
    @Override public Set<String> arg() {
        return caches;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        if (caches == null)
            getAffectedCaches(clientCfg, logger);

        CacheResetLostPartitionsTaskArg taskArg = new CacheResetLostPartitionsTaskArg(caches);
        try (GridClient client = Command.startClient(clientCfg)) {
            CacheResetLostPartitionsTaskResult res =
                executeTaskByNameOnNode(client, CacheResetLostPartitionsTask.class.getName(), taskArg, null, clientCfg);

            res.print(System.out);

            return res;
        }
    }

    /** Get a list of caches with LOST partitions in case if ALL MODE is specified. */
    private void getAffectedCaches(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        logger.info("ALL mode is activated.");
        logger.info("Looking for the caches with LOST partitions first...");

        caches = new HashSet<>();
        CacheDistributionTaskArg taskArg = new CacheDistributionTaskArg(null, null);
        try (GridClient client = Command.startClient(clientCfg)) {
            CacheDistributionTaskResult res = executeTaskByNameOnNode(client, CacheDistributionTask.class.getName(), taskArg, BROADCAST_UUID, clientCfg);
            for (CacheDistributionNode node : res.jobResults()) {
                for (CacheDistributionGroup group : node.getGroups()) {
                    for (CacheDistributionPartition partition : group.getPartitions()) {
                        if (partition.getState() == GridDhtPartitionState.LOST) {
                            caches.add(group.getGroupName());
                            break;
                        }
                    }
                }
            }
        }

        if (!caches.isEmpty())
            logger.info("The following caches have LOST partitions: " + CommandLogger.join(",", caches) + ".");
        else
            logger.info("No caches with LOST partition has been found found.");
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String nextArg = argIter.nextArg("Expected either [" + ALL_CACHES_MODE + "] or [" + CACHES + "]");

        ResetLostPartitionsCommandArg arg = CommandArgUtils.of(nextArg, ResetLostPartitionsCommandArg.class);

        if (arg == ALL_CACHES_MODE)
            caches = null;
        else {
            caches = new HashSet<>();
            for (String cacheName : nextArg.split(","))
                caches.add(cacheName.trim());
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return RESET_LOST_PARTITIONS.text().toUpperCase();
    }
}
