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

package org.apache.ignite.internal.commandline.dr;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.dr.subcommands.DrCacheCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrCheckPartitionCountersCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrCleanupPartitionLogCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrFSTCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrNodeCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrRebuildPartitionLogCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrRepairPartitionCountersCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrStateCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrTopologyCommand;

import static org.apache.ignite.internal.commandline.Command.usage;
import static org.apache.ignite.internal.commandline.CommandList.DATA_CENTER_REPLICATION;
import static org.apache.ignite.internal.commandline.CommandLogger.join;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.CACHE;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.CHECK;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.CLEANUP_TREES;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.FULL_STATE_TRANSFER;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.HELP;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.NODE;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.PAUSE;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.REBUILD_TREES;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.REPAIR;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.RESUME;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.STATE;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.TOPOLOGY;
import static org.apache.ignite.internal.commandline.dr.subcommands.DrFSTCommand.ParseStart.CACHES_PARAM;
import static org.apache.ignite.internal.commandline.dr.subcommands.DrFSTCommand.ParseStart.DATA_CENTERS;
import static org.apache.ignite.internal.commandline.dr.subcommands.DrFSTCommand.ParseStart.SENDER_GROUP;
import static org.apache.ignite.internal.commandline.dr.subcommands.DrFSTCommand.ParseStart.SNAPSHOT_ID;
import static org.apache.ignite.internal.commandline.dr.subcommands.DrFSTCommand.ParseStart.SYNC_MODE;

/** */
public class DrCommand extends AbstractCommand<Object> {
    /** */
    private Command<?> delegate;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        usage(log, "Print data center replication command help:",
            DATA_CENTER_REPLICATION,
            HELP.toString()
        );

        usage(log, "Print state of data center replication:",
            DATA_CENTER_REPLICATION,
            STATE.toString(),
            optional(DrStateCommand.VERBOSE_PARAM)
        );

        usage(log, "Print topology of the cluster with the data center replication related details:",
            DATA_CENTER_REPLICATION,
            TOPOLOGY.toString(),
            optional(DrTopologyCommand.SENDER_HUBS_PARAM),
            optional(DrTopologyCommand.RECEIVER_HUBS_PARAM),
            optional(DrTopologyCommand.DATA_NODES_PARAM),
            optional(DrTopologyCommand.OTHER_NODES_PARAM)
        );

        usage(log, "Print node specific data center replication related details and clear node's DR store:",
            DATA_CENTER_REPLICATION,
            NODE.toString(),
            "<nodeId>",
            optional(DrNodeCommand.CONFIG_PARAM),
            optional(DrNodeCommand.METRICS_PARAM),
            optional(DrNodeCommand.CLEAR_STORE_PARAM),
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Print cache specific data center replication related details about caches and maybe change replication state on them:",
            DATA_CENTER_REPLICATION,
            CACHE.toString(),
            "<regExp>",
            optional(DrCacheCommand.CONFIG_PARAM),
            optional(DrCacheCommand.METRICS_PARAM),
            optional(DrCacheCommand.CACHE_FILTER_PARAM, join("|", DrCacheCommand.CacheFilter.values())),
            optional(DrCacheCommand.SENDER_GROUP_PARAM, "<groupName>|" + join("|", DrCacheCommand.SenderGroup.values())),
            optional(DrCacheCommand.ACTION_PARAM, DrCacheCommand.Action.START.argName() + "|" + DrCacheCommand.Action.STOP.argName()),
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Deprecated command to start full state transfer:",
            DATA_CENTER_REPLICATION,
            CACHE.toString(),
            "<regExp> --action full-state-transfer",
            optional(SYNC_MODE),
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Execute full state transfer, transfer all caches if not specified in params and " +
                "data center is configured:",
            DATA_CENTER_REPLICATION,
            FULL_STATE_TRANSFER.toString(),
            optional(DrFSTCommand.Action.START.argName()),
            optional(SNAPSHOT_ID, "<snapshotId>"),
            optional(CACHES_PARAM, "<cacheName1, ...>"),
            optional(SENDER_GROUP, "<groupName>|ALL|DEFAULT|NONE"),
            optional(DATA_CENTERS, "<dcId, ...>"),
            optional(SYNC_MODE),
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Cancel active full state transfer by id:",
            DATA_CENTER_REPLICATION,
            FULL_STATE_TRANSFER.toString(),
            DrFSTCommand.Action.CANCEL.argName(),
            "<fullStateTransferUID>",
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Print list of active full state transfers:",
            DATA_CENTER_REPLICATION,
            FULL_STATE_TRANSFER.toString(),
            DrFSTCommand.Action.LIST.argName()
        );

        usage(log, "Stop data center replication on all caches in cluster:",
            DATA_CENTER_REPLICATION,
            PAUSE.toString(),
            "<remoteDataCenterId>",
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Start data center replication on all caches in cluster:",
            DATA_CENTER_REPLICATION,
            RESUME.toString(),
            "<remoteDataCenterId>",
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Start partition counters check for selected caches:",
                DATA_CENTER_REPLICATION,
                CHECK.toString(),
                optional(DrCheckPartitionCountersCommand.CACHES_PARAM, "cacheName1,cacheName2,...,cacheNameN"),
                optional(DrCheckPartitionCountersCommand.CHECK_FIRST_PARAM),
                optional(DrCheckPartitionCountersCommand.SCAN_UNTIL_FIRST_ERROR),
                optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Start partition counters repairing for selected caches:",
                DATA_CENTER_REPLICATION,
                REPAIR.toString(),
                optional(DrRepairPartitionCountersCommand.CACHES_PARAM, "cacheName1,cacheName2,...,cacheNameN"),
                optional(DrRepairPartitionCountersCommand.BATCH_SIZE),
                optional(DrRepairPartitionCountersCommand.KEEP_BINARY),
                optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Schedule/run maintenance task for rebuilding DR tries:",
            DATA_CENTER_REPLICATION,
            REBUILD_TREES.toString(),
            optional(DrRebuildPartitionLogCommand.CACHES_ARG, "cacheName1,cacheName2,...,cacheNameN"),
            optional(DrRebuildPartitionLogCommand.GROUPS_ARG, "groupName1,groupName2,...,groupNameN"),
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Schedule/run maintenance task for cleanup DR tries:",
            DATA_CENTER_REPLICATION,
            CLEANUP_TREES.toString(),
            optional(DrCleanupPartitionLogCommand.CACHES_ARG, "cacheName1,cacheName2,...,cacheNameN"),
            optional(DrCleanupPartitionLogCommand.GROUPS_ARG, "groupName1,groupName2,...,groupNameN"),
            optional(DrCleanupPartitionLogCommand.NODES_ARG, "consistentId0,consistentId1,...,consistentIdN"),
            optional(CMD_AUTO_CONFIRMATION)
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DATA_CENTER_REPLICATION.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        DrSubCommandsList subcommand = DrSubCommandsList.parse(argIter.nextArg("Expected dr action."));

        if (subcommand == null)
            throw new IllegalArgumentException("Expected correct dr action.");

        delegate = subcommand.command();

        delegate.parseArguments(argIter);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return delegate != null ? delegate.confirmationPrompt() : null;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        return delegate.execute(clientCfg, log);
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return delegate.arg();
    }
}
