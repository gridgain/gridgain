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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.util.VisorIllegalStateException;
import org.apache.ignite.internal.visor.verify.IndexIntegrityCheckIssue;
import org.apache.ignite.internal.visor.verify.IndexValidationIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesPartitionResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskArg;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.OP_NODE_ID;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.IdleVerify.CLUSTER_NOT_IN_READ_ONLY_WARN_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_CRC;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;

/**
 * Validate indexes command.
 */
public class CacheValidateIndexes implements Command<CacheValidateIndexes.Arguments> {
    /** Number of arguments. */
    private static final int NUMBER_OF_ARGUMENTS = 5;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String description = "Validate consistency between primary and secondary indexes for all or specified caches on " +
            "an idle cluster and print out the differences, if any. The following is checked by the validation process: " +
            "1) All the key-value entries that are referenced from a primary index has to be reachable from secondary " +
            "SQL indexes as well if any. 2) All the key-value entries that are referenced from a primary index has to " +
            "be reachable. A reference from the primary index shouldn't go in nowhere. 3) All the key-value entries " +
            "that are referenced from secondary SQL indexes have to be reachable from the primary index as well." +
            "The validation could be done on all cluster or on specified node only. For speed up process could be used " +
            CHECK_FIRST + " or " + CHECK_THROUGH + " options.";

        Map<String, String> paramsDesc = U.newLinkedHashMap(16);

        paramsDesc.put(CHECK_FIRST + " N", "validate only the first N keys");
        paramsDesc.put(CHECK_THROUGH + " K", "validate every Kth key");
        paramsDesc.put(CHECK_CRC.argName(), "check the CRC-sum of pages in index partition stored on disk before " +
            "index validation. Works only with enabled read-only mode.");

        Set<String> args = U.newLinkedHashSet(16);

        args.add(optional("cacheName1,...,cacheNameN"));
        args.add(OP_NODE_ID);
        args.add(optional(or(CHECK_FIRST + " N", CHECK_THROUGH + " K")));
        args.add(optional(CHECK_CRC));

        usageCache(logger, VALIDATE_INDEXES, description, paramsDesc, args.toArray(new String[0]));
    }

    /**
     * Container for command arguments.
     */
    public class Arguments {
         /** Caches. */
        private Set<String> caches;

        /** Node id. */
        private UUID nodeId;

        /** Max number of entries to be checked. */
        private int checkFirst = -1;

        /** Number of entries to check through. */
        private int checkThrough = -1;

        /** Check crc sums flag. */
        private boolean checkCrc;

        /** */
        public Arguments(Set<String> caches, UUID nodeId, int checkFirst, int checkThrough, boolean checkCrc) {
            this.caches = caches;
            this.nodeId = nodeId;
            this.checkFirst = checkFirst;
            this.checkThrough = checkThrough;
            this.checkCrc = checkCrc;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Max number of entries to be checked.
         */
        public int checkFirst() {
            return checkFirst;
        }

        /**
         * @return Number of entries to check through.
         */
        public int checkThrough() {
            return checkThrough;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Check crc sums flag.
         */
        public boolean checkCrc() {
            return checkCrc;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }
    }

    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        VisorValidateIndexesTaskArg taskArg = new VisorValidateIndexesTaskArg(
            args.caches(),
            args.nodeId() != null ? Collections.singleton(args.nodeId()) : null,
            args.checkFirst(),
            args.checkThrough(),
            args.checkCrc()
        );

        try (GridClient client = Command.startClient(clientCfg)) {
            GridClientClusterState state = client.state();

            if (args.checkCrc() && !state.readOnly()) {
                throw new VisorIllegalStateException(
                    "Cluster isn't in read-only mode. " + VALIDATE_INDEXES + " with " + CHECK_CRC +
                        " not allowed without enabled read-only mode."
                );
            }

            if (!state.readOnly())
                logger.warning(CLUSTER_NOT_IN_READ_ONLY_WARN_MESSAGE);

            VisorValidateIndexesTaskResult taskRes = executeTaskByNameOnNode(
                client, "org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask", taskArg, null, clientCfg);

            boolean errors = CommandLogger.printErrors(taskRes.exceptions(), "Index validation failed on nodes:", logger);

            for (Map.Entry<UUID, VisorValidateIndexesJobResult> nodeEntry : taskRes.results().entrySet()) {
                if (!nodeEntry.getValue().hasIssues())
                    continue;

                errors = true;

                logger.info("Index issues found on node " + nodeEntry.getKey() + ":");

                Collection<IndexIntegrityCheckIssue> integrityCheckFailures = nodeEntry.getValue().integrityCheckFailures();

                if (!integrityCheckFailures.isEmpty()) {
                    for (IndexIntegrityCheckIssue is : integrityCheckFailures)
                        logger.info(INDENT + is);
                }

                Map<PartitionKey, ValidateIndexesPartitionResult> partRes = nodeEntry.getValue().partitionResult();

                for (Map.Entry<PartitionKey, ValidateIndexesPartitionResult> e : partRes.entrySet()) {
                    ValidateIndexesPartitionResult res = e.getValue();

                    if (!res.issues().isEmpty()) {
                        logger.info(INDENT + CommandLogger.join(" ", e.getKey(), e.getValue()));

                        for (IndexValidationIssue is : res.issues())
                            logger.info(DOUBLE_INDENT + is);
                    }
                }

                Map<String, ValidateIndexesPartitionResult> idxRes = nodeEntry.getValue().indexResult();

                for (Map.Entry<String, ValidateIndexesPartitionResult> e : idxRes.entrySet()) {
                    ValidateIndexesPartitionResult res = e.getValue();

                    if (!res.issues().isEmpty()) {
                        logger.info(INDENT + CommandLogger.join(" ", "SQL Index", e.getKey(), e.getValue()));

                        for (IndexValidationIssue is : res.issues())
                            logger.info(DOUBLE_INDENT + is);
                    }
                }
            }

            if (!errors)
                logger.severe("no issues found.");
            else
                logger.severe("issues found (listed above).");

            logger.info("");

            if (errors && !state.readOnly()) {
                logger.warning(CLUSTER_NOT_IN_READ_ONLY_WARN_MESSAGE);

                logger.warning("");
            }

            return taskRes;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        int checkFirst = -1;
        int checkThrough = -1;
        UUID nodeId = null;
        Set<String> caches = null;
        boolean checkCrc = false;

        int argsCnt = 0;

        while (argIter.hasNextSubArg() && argsCnt++ < NUMBER_OF_ARGUMENTS) {
            String nextArg = argIter.nextArg("");

            ValidateIndexesCommandArg arg = CommandArgUtils.of(nextArg, ValidateIndexesCommandArg.class);

            if (arg == CHECK_FIRST || arg == CHECK_THROUGH) {
                if (!argIter.hasNextSubArg())
                    throw new IllegalArgumentException("Numeric value for '" + nextArg + "' parameter expected.");

                int numVal;

                String numStr = argIter.nextArg("");

                try {
                    numVal = Integer.parseInt(numStr);
                }
                catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                        "Not numeric value was passed for '" + nextArg + "' parameter: " + numStr
                    );
                }

                if (numVal <= 0)
                    throw new IllegalArgumentException("Value for '" + nextArg + "' property should be positive.");

                if (arg == CHECK_FIRST)
                    checkFirst = numVal;
                else
                    checkThrough = numVal;

                continue;
            }

            if (arg == CHECK_CRC) {
                checkCrc = true;

                continue;
            }

            try {
                nodeId = UUID.fromString(nextArg);

                continue;
            }
            catch (IllegalArgumentException ignored) {
                //No-op.
            }

            caches = argIter.parseStringSet(nextArg);

            if (F.constainsStringIgnoreCase(caches, UTILITY_CACHE_NAME)) {
                throw new IllegalArgumentException(
                    VALIDATE_INDEXES + " not allowed for `" + UTILITY_CACHE_NAME + "` cache."
                );
            }
        }

        args = new Arguments(caches, nodeId, checkFirst, checkThrough, checkCrc);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return VALIDATE_INDEXES.text().toUpperCase();
    }
}
