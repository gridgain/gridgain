/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.cache;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTask;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;

import static java.lang.String.format;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.PARTITION_RECONCILIATION;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.BATCH_SIZE;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.CONSOLE;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.FIX_ALG;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.FIX_MODE;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.RECHECK_ATTEMPTS;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.LOAD_FACTOR;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.VERBOSE;

/**
 * Partition reconciliation command.
 */
public class PartitionReconciliation implements Command<PartitionReconciliation.Arguments> {
    /** */
    public static final int DEFAULT_BATCH_SIZE = 1000;
    /** */
    public static final int DEFAULT_RECHECK_ATTEMPTS = 2;

    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        String CACHES = "cacheName1,...,cacheNameN";

        String desc = "Verify whether there are inconsistent entries for the specified caches " +
            "and print out the differences if any. Fix inconsistency if " + FIX_MODE + "argument is presented. " +
            "When no parameters are specified, " +
            "all user caches are verified. Cache filtering options configure the set of caches that will be " +
            "processed by " + PARTITION_RECONCILIATION + " command. If cache names are specified, in form of regular " +
            "expressions, only matching caches will be verified.";

        Map<String, String> paramsDesc = new HashMap<>();

        paramsDesc.put(FIX_MODE.toString(),
            "If present, fix all inconsistent data.");
        paramsDesc.put(LOAD_FACTOR.toString(),
            "Percent of system loading between 0 and 1.");
        paramsDesc.put(BATCH_SIZE.toString(),
            "Amount of keys to retrieve within one job.");
        paramsDesc.put(RECHECK_ATTEMPTS.toString(),
            "Amount of potentially inconsistent keys recheck attempts.");
        paramsDesc.put(VERBOSE.toString(),
            "Print data to result with sensitive information: keys and values.");
        paramsDesc.put(FIX_ALG.toString(),
            "Specifies which repair algorithm to use for doubtful keys.");
        paramsDesc.put(CONSOLE.toString(),
            "Specifies whether to print result to console or file.");

        usageCache(
            log,
            PARTITION_RECONCILIATION,
            desc,
            paramsDesc,
            optional(FIX_MODE), optional(LOAD_FACTOR), optional(BATCH_SIZE), optional(RECHECK_ATTEMPTS),
            optional(VERBOSE), optional(FIX_ALG), optional(CONSOLE), optional(CACHES));
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PARTITION_RECONCILIATION.text().toUpperCase();
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            partitionReconciliationCheck(client, clientCfg, log);
        }

        return null;
    }

    /**
     * Prepare arguments, execute partition reconciliation task, print logs and optionally fix inconsistency.
     *
     * @param client Client node to run initial task.
     * @param clientCfg Client configuration.
     * @param log Logger.
     * @throws GridClientException If failed.
     */
    private void partitionReconciliationCheck(
        GridClient client,
        GridClientConfiguration clientCfg,
        Logger log
    ) throws GridClientException {
        VisorPartitionReconciliationTaskArg taskArg = new VisorPartitionReconciliationTaskArg(
            args.caches,
            args.fixMode,
            args.verbose,
            args.console,
            args.loadFactor,
            args.batchSize,
            args.recheckAttempts,
            args.repairAlg
        );

        ReconciliationResult res =
            executeTask(client, VisorPartitionReconciliationTask.class, taskArg, clientCfg);

        if(args.console)
            print(res, log::info);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheNames = null;
        boolean fixMode = false;
        boolean verbose = false;
        boolean console = false;
        double loadFactor = 1;
        int batchSize = DEFAULT_BATCH_SIZE;
        int recheckAttempts = DEFAULT_RECHECK_ATTEMPTS;
        RepairAlgorithm repairAlg = RepairAlgorithm.defaultValue();

        int partReconciliationArgsCnt = 8;

        while (argIter.hasNextSubArg() && partReconciliationArgsCnt-- > 0) {
            String nextArg = argIter.nextArg("");

            PartitionReconciliationCommandArg arg =
                CommandArgUtils.of(nextArg, PartitionReconciliationCommandArg.class);

            if (arg == null) {
                cacheNames = argIter.parseStringSet(nextArg);

                validateRegexps(cacheNames);
            }
            else {
                switch (arg) {
                    case FIX_MODE:
                        fixMode = true;

                        break;

                    case FIX_ALG:
                        // TODO: 20.11.19 Use proper message here.
                        // TODO: 20.11.19 Validate value.
                        repairAlg = RepairAlgorithm.valueOf(argIter.nextArg(""));

                        break;

                    case VERBOSE:
                        verbose = true;

                        break;

                    case CONSOLE:
                        console = true;

                        break;

                    case LOAD_FACTOR:
                        // TODO: 20.11.19 Use proper message here. 
                        String loadFactorStr = argIter.nextArg("The cache filter should be specified. The following " +
                            "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                        // TODO: 20.11.19 Validate value. 
                        loadFactor = Double.valueOf(loadFactorStr);

                        break;

                    case BATCH_SIZE:
                        // TODO: 20.11.19 Use proper message here. 
                        String batchSizeStr = argIter.nextArg("The cache filter should be specified. The following " +
                            "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                        // TODO: 20.11.19 Validate value. 
                        batchSize = Integer.valueOf(batchSizeStr);

                        break;

                    case RECHECK_ATTEMPTS:
                        // TODO: 20.11.19 Use proper message here.
                        String recheckAttemptsStr = argIter.nextArg("The cache filter should be specified. The following " +
                            "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                        // TODO: 20.11.19 Validate value.
                        recheckAttempts = Integer.valueOf(recheckAttemptsStr);

                        break;
                }
            }
        }

        args = new Arguments(cacheNames, fixMode, verbose, console, loadFactor, batchSize, recheckAttempts, repairAlg);
    }

    // TODO: 20.11.19 Idle verify has exactly same method.

    /**
     * @param str To validate that given name is valid regexp.
     */
    private void validateRegexps(Set<String> str) {
        str.forEach(s -> {
            try {
                Pattern.compile(s);
            }
            catch (PatternSyntaxException e) {
                throw new IgniteException(format("Invalid cache name regexp '%s': %s", s, e.getMessage()));
            }
        });
    }

    /**
     * @return String with meta information about current run of partition_reconciliation: used arguments, params, etc.
     */
    private String prepareHeaderMeta() {
        SB options = new SB("partition_reconciliation task was executed with the following args: ");

        options
            .a("caches=[")
            .a(args.caches() == null ? "" : String.join(", ", args.caches()))
            .a("], fix-mode=[" + args.fixMode)
            .a("], verbose=[" + args.verbose)
            .a("], load-factor=[" + args.loadFactor)
            .a("], batch-size=[" + args.batchSize)
            .a("], recheck-attempts=[" + args.recheckAttempts)
            .a("], fix-alg=[" + args.repairAlg + "]")
            .a("\n");

        return options.toString();
    }

    /**
     * @return String with folder of results and their locations.
     */
    private String prepareResultFolders(
        Map<UUID, String> nodeIdsToFolders,
        Map<UUID, String> nodesIdsToConsistenceIdsMap
    ) {
        SB out = new SB("partition_reconciliation task prepared result where line is " +
            "- <nodeConsistentId>, <nodeId> : <folder> \n");

        for (Map.Entry<UUID, String> entry : nodeIdsToFolders.entrySet()) {
            String consId = nodesIdsToConsistenceIdsMap.get(entry.getKey());

            out
                .a(consId + " " + entry.getKey() + " : " + entry.getValue())
                .a("\n");
        }

        return out.toString();
    }

    /**
     * Print partition reconciliation output.
     * @param res Partition reconciliation result.
     * @param printer Printer.
     */
    private void print(ReconciliationResult res, Consumer<String> printer) {
        PartitionReconciliationResult reconciliationRes = res.partitionReconciliationResult();

        printer.accept(prepareHeaderMeta());

        printer.accept(prepareResultFolders(res.nodeIdToFolder(), reconciliationRes.nodesIdsToConsistenseIdsMap()));

        reconciliationRes.print(printer, args.verbose);
    }

    /**
     * Container for command arguments.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    protected static class Arguments {
        /** */
        private Set<String> caches;

        /** */
        private boolean fixMode;

        /** */
        private boolean verbose;

        /** */
        private boolean console;

        /** */
        private double loadFactor;

        /** */
        private int batchSize;

        /** */
        private int recheckAttempts;

        /** */
        private RepairAlgorithm repairAlg;

        /**
         * Constructor.
         *
         * @param caches Caches.
         * @param fixMode Fix inconsistency if {@code True}.
         * @param verbose Print key and value to result log if {@code True}.
         * @param loadFactor Percent of system loading.
         * @param batchSize Batch size.
         * @param recheckAttempts Amount of recheck attempts.
         */
        public Arguments(Set<String> caches, boolean fixMode, boolean verbose, boolean console,
            double loadFactor,
            int batchSize, int recheckAttempts, RepairAlgorithm repairAlg) {
            this.caches = caches;
            this.fixMode = fixMode;
            this.verbose = verbose;
            this.console = console;
            this.loadFactor = loadFactor;
            this.batchSize = batchSize;
            this.recheckAttempts = recheckAttempts;
            this.repairAlg = repairAlg;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Fix mode.
         */
        public boolean fixMode() {
            return fixMode;
        }

        /**
         * @return Percent of system loading.
         */
        public double loadFactor() {
            return loadFactor;
        }

        /**
         * @return Batch size.
         */
        public int batchSize() {
            return batchSize;
        }

        /**
         * @return Recheck attempts.
         */
        public int recheckAttempts() {
            return recheckAttempts;
        }

        /**
         * @return Verbose.
         */
        public boolean verbose() {
            return verbose;
        }

        /**
         * @return Print to console.
         */
        public boolean console() {
            return console;
        }

        /**
         * @return Repair alg.
         */
        public RepairAlgorithm repairAlg() {
            return repairAlg;
        }
    }
}
