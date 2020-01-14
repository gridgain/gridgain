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
import java.util.List;
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
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTask;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;

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
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.RECHECK_DELAY;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.VERBOSE;

/**
 * Partition reconciliation command.
 */
public class PartitionReconciliation implements Command<PartitionReconciliation.Arguments> {
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
            "If present, fix all inconsistent data. Default value is " + FIX_MODE.defaultValue() + '.');

        paramsDesc.put(LOAD_FACTOR.toString(),
            "Percent of system loading between 0 (exclusive) and 1 (inclusive). Default value is " +
                LOAD_FACTOR.defaultValue() + '.');

        paramsDesc.put(BATCH_SIZE.toString(),
            "Amount of keys to retrieve within one job. Default value is " + BATCH_SIZE.defaultValue() + '.');

        paramsDesc.put(RECHECK_ATTEMPTS.toString(),
            "Amount of potentially inconsistent keys recheck attempts. Value between 1 and 5 should be used." +
                " Default value is " + RECHECK_ATTEMPTS.defaultValue() + '.');

        paramsDesc.put(VERBOSE.toString(),
            "Print data to result with sensitive information: keys and values." +
                " Default value is " + VERBOSE.defaultValue() + '.');

        paramsDesc.put(FIX_ALG.toString(),
            "Specifies which repair algorithm to use for doubtful keys. The following values can be used: "
                + Arrays.toString(RepairAlgorithm.values()) +  " Default value is " + FIX_ALG.defaultValue() + '.');

        paramsDesc.put(CONSOLE.toString(),
            "Specifies whether to print result to console or file. Default value is " + CONSOLE.defaultValue() + '.');

        // RECHECK_DELAY arg intentionally skipped.

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
            return partitionReconciliationCheck(client, clientCfg, log);
        }
        catch (Throwable e) {
            log.severe("Failed to execute partition reconciliation command " + CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /**
     * Prepare arguments, execute partition reconciliation task, print logs and optionally fix inconsistency.
     *
     * @param client Client node to run initial task.
     * @param clientCfg Client configuration.
     * @param log Logger.
     * @return Result of operation.
     * @throws GridClientException If failed.
     */
    private ReconciliationResult partitionReconciliationCheck(
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
            args.repairAlg,
            args.recheckDelay
        );

        ReconciliationResult res =
            executeTask(client, VisorPartitionReconciliationTask.class, taskArg, clientCfg);

        print(res, log::info);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheNames = null;
        boolean fixMode = (boolean) FIX_MODE.defaultValue();
        boolean verbose = (boolean) VERBOSE.defaultValue();
        boolean console = (boolean) CONSOLE.defaultValue();
        double loadFactor = (double) LOAD_FACTOR.defaultValue();
        int batchSize = (int) BATCH_SIZE.defaultValue();
        int recheckAttempts = (int) RECHECK_ATTEMPTS.defaultValue();
        RepairAlgorithm repairAlg = (RepairAlgorithm) FIX_ALG.defaultValue();
        int recheckDelay = (int) RECHECK_DELAY.defaultValue();

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
                String strVal;

                switch (arg) {
                    case FIX_MODE:
                        fixMode = true;

                        break;

                    case FIX_ALG:
                        strVal = argIter.nextArg(
                            "The repair algorithm should be specified. The following " +
                                "values can be used: " + Arrays.toString(RepairAlgorithm.values()) + '.');

                        try {
                            repairAlg = RepairAlgorithm.valueOf(strVal);
                        }
                        catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                "Invalid repair algorithm: " + strVal + ". The following " +
                                "values can be used: " + Arrays.toString(RepairAlgorithm.values()) + '.');
                        }

                        break;

                    case VERBOSE:
                        verbose = true;

                        break;

                    case CONSOLE:
                        console = true;

                        break;

                    case LOAD_FACTOR:
                        strVal = argIter.nextArg("The load factor should be specified.");

                        try {
                            loadFactor = Double.parseDouble(strVal);
                        }
                        catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid load factor: " + strVal +
                                ". Double value between 0 (exclusive) and 1 (inclusive) should be used.");
                        }

                        if (loadFactor <= 0 || loadFactor > 1) {
                            throw new IllegalArgumentException("Invalid load factor: " + strVal +
                                ". Double value between 0 (exclusive) and 1 (inclusive) should be used.");
                        }

                        break;

                    case BATCH_SIZE:
                        strVal = argIter.nextArg("The batch size should be specified.");

                        try {
                            batchSize = Integer.parseInt(strVal);
                        }
                        catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid batch size: " + strVal +
                                ". Int value greater than zero should be used.");
                        }

                        if (batchSize <= 0) {
                            throw new IllegalArgumentException("Invalid batch size: " + strVal +
                                ". Int value greater than zero should be used.");
                        }

                        break;

                    case RECHECK_ATTEMPTS:
                        strVal = argIter.nextArg("The recheck attempts should be specified.");

                        try {
                            recheckAttempts = Integer.parseInt(strVal);
                        }
                        catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid recheck attempts: " + strVal +
                                ". Int value between 1 and 5 should be used.");
                        }

                        if (recheckAttempts < 1 || recheckAttempts > 5) {
                            throw new IllegalArgumentException("Invalid recheck attempts: " + strVal +
                                ". Int value between 1 and 5 should be used.");
                        }

                        break;

                    case RECHECK_DELAY:
                        strVal = argIter.nextArg("The recheck delay should be specified.");

                        try {
                            recheckDelay = Integer.valueOf(strVal);
                        }
                        catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid recheck delay: " + strVal +
                                ". Int value between 0 and 100 should be used.");
                        }

                        if (recheckDelay < 0 || recheckDelay > 100) {
                            throw new IllegalArgumentException("Invalid recheck delay: " + strVal +
                                ". Int value between 0 and 100 should be used.");
                        }

                        break;
                }
            }
        }

        args = new Arguments(cacheNames, fixMode, verbose, console, loadFactor, batchSize, recheckAttempts, repairAlg,
            recheckDelay);
    }

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
            .a("], recheck-delay=[" + args.recheckDelay + "]")
            .a(System.lineSeparator());

        if (args.verbose) {
            options
                .a("WARNING: Please be aware that sensitive data will be printed to the console and output file(s).")
                .a(System.lineSeparator());
        }

        return options.toString();
    }

    /**
     * @param nodeIdsToFolders Mapping node id to the directory to be used for inconsistency report.
     * @param nodesIdsToConsistenceIdsMap Mapping node id to consistent id.
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
                .a(consId + " " + entry.getKey() + " : " + (entry.getValue() == null ?
                    "All keys on this node are consistent: report wasn't generated." : entry.getValue()))
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

        printer.accept(prepareErrors(res.errors()));

        printer.accept(prepareResultFolders(res.nodeIdToFolder(), reconciliationRes.nodesIdsToConsistenseIdsMap()));

        reconciliationRes.print(printer, args.verbose);
    }

    /**
     * Returns string representation of the given list of errors.
     *
     * @param errors List of errors.
     * @return String representation of the given list of errors.
     */
    private String prepareErrors(List<String> errors) {
        SB errorMsg = new SB();

        if(!errors.isEmpty()) {
            errorMsg
                .a("The following errors occurred during the execution of partition reconciliation:")
                .a(System.lineSeparator());

            for (int i = 0; i < errors.size(); i++)
                errorMsg.a(i + 1).a(". ").a(errors.get(i)).a(System.lineSeparator());
        }

        return errorMsg.toString();
    }

    /**
     * Container for command arguments.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    protected static class Arguments {
        /** List of caches to be checked. */
        private final Set<String> caches;

        /** Flag indicates that an inconsistency should be fixed in accordance with RepairAlgorithm parameter. */
        private final boolean fixMode;

        /** Flag indicates that the result should include sensitive information such as key and value. */
        private final boolean verbose;

        /** Flag indicates that the result is printed to the console. */
        private final boolean console;

        /** Percent of system loading between 0 and 1. */
        private final double loadFactor;

        /** Amount of keys to checked at one time. */
        private final int batchSize;

        /** Amount of potentially inconsistent keys recheck attempts. */
        private final int recheckAttempts;

        /** Partition reconciliation repair algorithm to be used. */
        private final RepairAlgorithm repairAlg;

        /** Recheck delay in seconds. */
        private int recheckDelay;

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
            int batchSize, int recheckAttempts, RepairAlgorithm repairAlg, int recheckDelay) {
            this.caches = caches;
            this.fixMode = fixMode;
            this.verbose = verbose;
            this.console = console;
            this.loadFactor = loadFactor;
            this.batchSize = batchSize;
            this.recheckAttempts = recheckAttempts;
            this.repairAlg = repairAlg;
            this.recheckDelay = recheckDelay;
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

        /**
         * @return Recheck delay.
         */
        public int recheckDelay() {
            return recheckDelay;
        }
    }
}
