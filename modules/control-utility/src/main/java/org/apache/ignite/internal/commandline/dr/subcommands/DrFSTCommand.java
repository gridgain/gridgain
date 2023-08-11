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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.dr.VisorDrCacheFSTTaskResult;
import org.apache.ignite.internal.visor.dr.VisorDrCacheTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrCacheTaskResult;
import org.apache.ignite.internal.visor.dr.VisorDrFSTCmdArgs;
import org.apache.ignite.lang.IgniteUuid;
import static org.apache.ignite.internal.IgniteFeatures.NEW_DR_FST_COMMANDS;
import static org.apache.ignite.internal.client.util.GridClientUtils.applyFilter;
import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.CHECK;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.FULL_STATE_TRANSFER;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;

/** */
public class DrFSTCommand
    extends DrAbstractRemoteSubCommand<VisorDrFSTCmdArgs, VisorDrCacheFSTTaskResult, DrFSTCommand.DrFSTArguments> {
    public static final String INC_TRANSFER_WITHOUT_DC_ERR = "Incremental state transfer is possible only if you specify " +
        "single configured remote data center id";

    public static final String COMPATIBILITY_WARN = "Some nodes in cluster doesn't support extended dr fst commands. " +
        "Only \"--dr full-state-transfer\" available for cluster in rolling upgrade state.";

    /**
     * Container for command arguments.
     */
    static class DrFSTArguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrFSTCmdArgs> {
        /** Legacy params support. */
        private DrCacheCommand.DrCacheArguments legacyArgs;

        /** Legacy params. */
        private boolean legacyMode;

        /** Command action. */
        private Action action;

        /** Action dependent params. */
        private ActionParams params;

        /** */
        public DrFSTArguments(DrCacheCommand.DrCacheArguments legacyArgs) {
            A.notNull(legacyArgs, "compatibilityArgs");

            this.legacyArgs = legacyArgs;

            legacyMode = true;
        }

        /** */
        public DrFSTArguments(
            Action action,
            ActionParams params
        ) {
            this.action = action;
            this.params = params;
            legacyMode = false;
        }

        /** {@inheritDoc} */
        @Override public VisorDrFSTCmdArgs toVisorArgs() {
            switch (action) {
                case LIST:
                    return new VisorDrFSTCmdArgs(action.ordinal(), null);

                case START: {
                    StartParams params0 = (StartParams)params;
                    return new VisorDrFSTCmdArgs(action.ordinal(), params0.caches(), params0.snapshotId(),
                        params0.dcIds(), params0.senderGroup() == null ? VisorDrCacheTaskArgs.SENDER_GROUP_NAMED :
                        params0.senderGroup().ordinal(), params0.senderGroupName(), params0.isSyncMode());
                }

                case CANCEL:
                    CancelParams params0 = (CancelParams)params;
                    return new VisorDrFSTCmdArgs(action.ordinal(), params0.operationId());

                default:
                    throw new IllegalArgumentException("Action [" + action.action() + "] not supported.");
            }
        }

        /**
         * @return Legacy params.
         */
        public boolean legacyMode() {
            return legacyMode;
        }

        /**
         * @return Legacy params support.
         */
        public DrCacheCommand.DrCacheArguments legacyArgs() {
            return legacyArgs;
        }

        /**
         * @return Command action.
         */
        public Action action() {
            return action;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DrFSTCommand.Arguments.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return arg().legacyMode() || arg().action() == Action.START ?
            "Warning: this command will execute full state transfer for all caches. This migth take a long time." : null;
    }

    /** {@inheritDoc} */
    @Override public DrFSTArguments parseArguments0(CommandArgIterator argIter) {
        if (!argIter.hasNextArg() || argIter.peekNextArg().equals(CMD_AUTO_CONFIRMATION)) {
            // for compatibility support only.
            DrCacheCommand.DrCacheArguments compatibilityArgs = new DrCacheCommand.DrCacheArguments(
                ".*",
                Pattern.compile(".*"),
                false,
                false,
                DrCacheCommand.CacheFilter.SENDING,
                DrCacheCommand.SenderGroup.ALL,
                null,
                DrCacheCommand.Action.FULL_STATE_TRANSFER,
                (byte)0,
                false
            );

            return new DrFSTArguments(compatibilityArgs);
        }

        List<Action> actions = Arrays.asList(Action.values());

        String actionStr = argIter.nextArg("One of possible actions: " + actions + " is required.");

        Action action = Action.parse(actionStr);

        if (action == null)
            throw new IllegalArgumentException("Action [" + actionStr + "] not supported.");

        ActionParams params = action.parseAction(argIter);

        return new DrFSTArguments(action, params);
    }

    /** {@inheritDoc} */
    @Override protected VisorDrCacheFSTTaskResult execute0(
        GridClientConfiguration clientCfg,
        GridClient client
    ) throws Exception {
        GridClientCompute compute = client.compute();

        if (!allNodesSupports(compute.nodes(), NEW_DR_FST_COMMANDS) || arg().legacyMode()) {
            if (arg().legacyMode()) {
                VisorDrCacheTaskResult res = DrCacheCommand.execute0(client, arg().legacyArgs());

                String completionMessage = "";

                if (res.getCacheNames().isEmpty())
                    completionMessage = "No suitable caches found for transfer.";
                else if (res.getResultMessages().isEmpty())
                    completionMessage = "Full state transfer command completed successfully for caches " + res.getCacheNames();

                return new VisorDrCacheFSTTaskResult(res.getDataCenterId(), completionMessage);
            } else
                throw new IgniteException(COMPATIBILITY_WARN);
        }

        Collection<GridClientNode> connectableNodes = compute.nodes(GridClientNode::connectable);

        connectableNodes = applyFilter(connectableNodes, p -> p.supports(NEW_DR_FST_COMMANDS));

        if (F.isEmpty(connectableNodes))
            throw new GridClientDisconnectedException("Connectable nodes not found", null);

        GridClientNode node = compute.balancer().balancedNode(connectableNodes);

        return compute.projection(node).execute(
            visorTaskName(),
            new VisorTaskArgument<>(node.nodeId(), arg().toVisorArgs(), false)
        );
    }

    /** Check feature support. */
    private boolean allNodesSupports(Collection<GridClientNode> nodes, IgniteFeatures feature) {
        for (GridClientNode node : nodes) {
            if (!node.supports(feature))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrCacheFSTTaskResult res, Logger log) {
        printUnrecognizedNodesMessage(log, false);

        log.info("Data Center ID: " + res.dataCenterId());

        log.info(DELIM);

        log.info(res.resultMessage());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return FULL_STATE_TRANSFER.text().toUpperCase();
    }

    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrProcessFSTTask";
    }

    /** FST actions. */
    public enum Action {
        /** Start FST. */
        START("start", new ParseStart()),

        /** Cancel FST. */
        CANCEL("cancel", new ParseCancel()),

        /** List active transfers. */
        LIST("list", new ParseNone());

        /** String representation. */
        private final String action;

        /** */
        private final ParseAction parseAction;

        /** */
        Action(String item, ParseAction parseAction) {
            action = item;
            this.parseAction = parseAction;
        }

        /** */
        public String action() {
            return action;
        }

        /** */
        public static Action parse(String item) {
            for (Action action : values()) {
                if (action.action.equalsIgnoreCase(item))
                    return action;
            }

            return null;
        }

        /**
         * @return Parse action params.
         */
        public ActionParams parseAction(CommandArgIterator argIter) {
            return parseAction.parse(argIter);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return action;
        }
    }

    /** */
    private static interface ActionParams {
    }

    /** */
    private static class CancelParams implements ActionParams {
        /** */
        private IgniteUuid operationId;

        /** */
        CancelParams(IgniteUuid operationId) {
            this.operationId = operationId;
        }

        /**
         * @return Operation id.
         */
        public IgniteUuid operationId() {
            return operationId;
        }
    }

    /** */
    private static class StartParams implements ActionParams {
        /** */
        private long snapshotId;

        /** */
        private Set<String> caches;

        /** */
        private DrCacheCommand.SenderGroup senderGroup;

        /** */
        private String senderGroupName;

        /** */
        private Set<Byte> dcIds;

        /** */
        private boolean syncMode;

        /** */
        StartParams() {
            // No op.
        }

        /** */
        StartParams(
            Long snapshotId,
            Set<String> caches,
            DrCacheCommand.SenderGroup senderGroup,
            String senderGroupName,
            Set<Byte> dcIds,
            boolean syncMode
        ) {
            if (snapshotId != null && (dcIds.size() != 1))
                throw new IllegalArgumentException(INC_TRANSFER_WITHOUT_DC_ERR);

            this.snapshotId = snapshotId == null ? -1 : snapshotId;
            this.caches = caches;
            this.senderGroup = senderGroup;
            this.senderGroupName = senderGroupName;
            this.dcIds = dcIds;
            this.syncMode = syncMode;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Snapshot id.
         */
        public long snapshotId() {
            return snapshotId;
        }

        /**
         * @return Sender group.
         */
        public DrCacheCommand.SenderGroup senderGroup() {
            return senderGroup;
        }

        /**
         * @return Sender group name.
         */
        public String senderGroupName() {
            return senderGroupName;
        }

        /**
         * @return Dc ids.
         */
        public Set<Byte> dcIds() {
            return dcIds;
        }

        /**
         * @return Sync mode.
         */
        public boolean isSyncMode() {
            return syncMode;
        }
    }

    /** */
    private static interface ParseAction<T extends ActionParams> {
        /** Parse further params. */
        T parse(CommandArgIterator argIter);
    }

    /** */
    private static class ParseNone implements ParseAction<ActionParams> {
        /** {@inheritDoc} */
        @Override public ActionParams parse(CommandArgIterator argIter) {
            if (argIter.hasNextArg() && !argIter.peekNextArg().equals(CMD_AUTO_CONFIRMATION))
                throw new IllegalArgumentException("Unexpected params: " + argIter.peekNextArg());

            return null;
        }
    }

    /** */
    private static class ParseCancel implements ParseAction<CancelParams> {
        /** {@inheritDoc} */
        @Override public CancelParams parse(CommandArgIterator argIter) {
            String operationId = argIter.nextArg("Expected full state transfer ID.");

            return new CancelParams(IgniteUuid.fromString(operationId));
        }
    }

    /** */
    public static class ParseStart implements ParseAction<StartParams> {
        /** Snapshot id. */
        public static final String SNAPSHOT_ID = "--snapshot";

        /** Caches parameter. */
        public static final String CACHES_PARAM = "--caches";

        /** Sender group. */
        public static final String SENDER_GROUP = "--sender-group";

        /** Data center id`s. */
        public static final String DATA_CENTERS = "--data-centers";

        public static final String SYNC_MODE = "--sync";

        /** {@inheritDoc} */
        @Override public StartParams parse(CommandArgIterator argIter) {
            Set<String> caches = null;
            Long snapshotId = null;
            DrCacheCommand.SenderGroup sndGrp = DrCacheCommand.SenderGroup.ALL;
            String sndGrpName = null;
            Set<Byte> dcIds = null;
            boolean syncMode = false;

            while (argIter.hasNextSubArg()) {
                String nextArg = argIter.nextArg("");

                switch (nextArg.toLowerCase(Locale.ENGLISH)) {
                    case SNAPSHOT_ID:
                        snapshotId = Long.parseLong(argIter.nextArg("Snapshot identificator expected."));
                        break;

                    case CACHES_PARAM:
                        if (!argIter.hasNextSubArg())
                            throw new IllegalArgumentException(
                                "Set of cache names for '" + nextArg + "' parameter expected.");

                        caches = argIter.parseStringSet(argIter.nextArg(""));

                        if (F.constainsStringIgnoreCase(caches, UTILITY_CACHE_NAME)) {
                            throw new IllegalArgumentException(
                                CHECK + " not allowed for `" + UTILITY_CACHE_NAME + "` cache."
                            );
                        }
                        break;

                    case SENDER_GROUP:
                        argIter.nextArg(null);

                        String arg = argIter.nextArg(SENDER_GROUP + " parameter value required.");

                        sndGrp = DrCacheCommand.SenderGroup.parse(arg);

                        if (sndGrp == null)
                            sndGrpName = arg;

                        break;

                    case DATA_CENTERS:
                        if (!argIter.hasNextSubArg())
                            throw new IllegalArgumentException(
                                "Set of datacenter id`s for '" + nextArg + "' parameter expected.");

                        Set<String> dcIdsStr = argIter.parseStringSet(argIter.nextArg(""));

                        dcIds = new HashSet<>();

                        Set<Byte> dcIds0 = dcIds;

                        dcIdsStr.forEach(dc -> dcIds0.add(Byte.parseByte(dc)));

                        break;

                    case SYNC_MODE:
                        syncMode = true;

                        break;

                    default:
                        throw new IllegalArgumentException("Argument " + nextArg + " is not supported.");
                }
            }

            return new StartParams(snapshotId, caches, sndGrp, sndGrpName, dcIds, syncMode);
        }
    }
}
