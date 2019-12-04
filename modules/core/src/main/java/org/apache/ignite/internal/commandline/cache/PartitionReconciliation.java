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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.FIX_MODE;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.RECHECK_ATTEMPTS;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.THROTTLING_INTERVAL_MILLIS;

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

        String desc = "Verify counters of primary and backup partitions for the specified caches/cache " +
            "and print out the differences, if any and/or fix inconsistency if " + FIX_MODE + "argument is presented." +
            " When no parameters are specified, " +
            "all user caches are verified. Cache filtering options configure the set of caches that will be " +
            "processed by " + PARTITION_RECONCILIATION + " command. If cache names are specified, in form of regular " +
            "expressions, only matching caches will be verified.";

        Map<String, String> paramsDesc = new HashMap<>();

        paramsDesc.put(FIX_MODE.toString(),
            "If present, backup data update from Primary partition for all inconsistent data.");
        paramsDesc.put(THROTTLING_INTERVAL_MILLIS.toString(),
            "Interval in milliseconds between running partition reconciliation jobs.");
        paramsDesc.put(BATCH_SIZE.toString(),
            "Amount of keys to retrieve within one job.");
        paramsDesc.put(RECHECK_ATTEMPTS.toString(),
            "Amount of potentially inconsistent keys recheck attempts.");

        usageCache(
            log,
            PARTITION_RECONCILIATION,
            desc,
            paramsDesc,
            optional(FIX_MODE), optional(THROTTLING_INTERVAL_MILLIS), optional(BATCH_SIZE), optional(RECHECK_ATTEMPTS),
            optional(CACHES));
        // TODO: 20.11.19 It might have sense to add params similar to IdleVerify command: exclude_caches and similar.
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
            args.throttlingIntervalMillis,
            args.batchSize,
            args.recheckAttempts
        );

        PartitionReconciliationResult res =
            executeTask(client, VisorPartitionReconciliationTask.class, taskArg, clientCfg);

        print(res, log::info, args.outputFile);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheNames = null;
        boolean fixMode = false;
        boolean verbose = false;
        int throttlingIntervalMillis = -1;
        int batchSize = DEFAULT_BATCH_SIZE;
        int recheckAttempts = DEFAULT_RECHECK_ATTEMPTS;
        String outputFile = null;

        int partReconciliationArgsCnt = 7;

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

                    case VERBOSE:
                        verbose = true;

                        break;

                    case THROTTLING_INTERVAL_MILLIS:
                        // TODO: 20.11.19 Use proper message here. 
                        String throttlingIntervalMillisStr = argIter.nextArg("The cache filter should be specified. The following " +
                            "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                        // TODO: 20.11.19 Validate value. 
                        throttlingIntervalMillis = Integer.valueOf(throttlingIntervalMillisStr);

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

                    case OUTPUT_FILE:
                        // TODO: 29.11.19 Propper message and optionality.
                        outputFile = argIter.nextArg("");

                        break;
                }
            }
        }

        args = new Arguments(cacheNames, fixMode, verbose, throttlingIntervalMillis, batchSize, recheckAttempts, outputFile);
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

    private String prepareHeaderMeta() {
        SB options = new SB("partition_reconciliation task was executed with the following args: ");

        options
            .a("caches=[")
            .a(args.caches() == null ? "" : String.join(", ", args.caches()))
            .a("], fix-mode=[" + args.fixMode)
            .a("], verbose=[" + args.verbose)
            .a("], throttling-interval-millis=[" + args.throttlingIntervalMillis)
            .a("], batch-size=[" + args.batchSize)
            .a("], recheck-attempts=[" + args.recheckAttempts)
            .a("], file=[" + args.outputFile + "]")
            .a("\n");

        return options.toString();
    }

    private void print(PartitionReconciliationResult res, Consumer<String> printer, String outputFile) {
        printer.accept(prepareHeaderMeta());

        if (args.outputFile != null) {
            File f = new File(outputFile);

            try (PrintWriter pw = new PrintWriter(f)) {
                ((Consumer<String>)(pw::write)).accept(prepareHeaderMeta());

                if (res != null)
                    res.print(pw::write);

                pw.flush();
            }
            catch (FileNotFoundException e) {
                printer.accept("Unable to write report to file " + f.getAbsolutePath() + " " + e.getMessage() + "\n");

                if (res != null)
                    res.print(printer);
            }
        }
        else {
            if (res != null)
                res.print(printer);
        }
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
        private int throttlingIntervalMillis;

        /** */
        private int batchSize;

        /** */
        private int recheckAttempts;

        /** */
        private String outputFile;

        /**
         * Constructor.
         *
         * @param caches Caches.
         * @param fixMode Fix inconsistency if {@code True}.
         * @param verbose Print key and value to result log if {@code True}.
         * @param throttlingIntervalMillis Throttling interval millis.
         * @param batchSize Batch size.
         * @param recheckAttempts Amount of recheck attempts.
         * @param outputFile File to write output report to.
         */
        public Arguments(Set<String> caches, boolean fixMode, boolean verbose, int throttlingIntervalMillis,
            int batchSize, int recheckAttempts, String outputFile) {
            this.caches = caches;
            this.fixMode = fixMode;
            this.verbose = verbose;
            this.throttlingIntervalMillis = throttlingIntervalMillis;
            this.batchSize = batchSize;
            this.recheckAttempts = recheckAttempts;
            this.outputFile = outputFile;
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
         * @return Throttling interval millis.
         */
        public int throttlingIntervalMillis() {
            return throttlingIntervalMillis;
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
    }
}
