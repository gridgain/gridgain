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

import java.util.Collection;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.dr.VisorDrRepairPartitionCountersJobResult;
import org.apache.ignite.internal.visor.dr.VisorDrRepairPartitionCountersTaskArg;
import org.apache.ignite.internal.visor.dr.VisorDrRepairPartitionCountersTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.REPAIR;

/**
 * Repair partition counters command.
 */
public class DrRepairPartitionCountersCommand extends DrAbstractRemoteSubCommand<VisorDrRepairPartitionCountersTaskArg,
        VisorDrRepairPartitionCountersTaskResult, DrRepairPartitionCountersCommand.Arguments> {
    /** Caches parameter. */
    public static final String CACHES_PARAM = "--caches";

    /** Batch size parameter. */
    public static final String BATCH_SIZE = "--batch-size";

    /** Keep binary parameter. */
    public static final String KEEP_BINARY = "--keep-binary";

    /**
     * Container for command arguments.
     */
    public static class Arguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrRepairPartitionCountersTaskArg> {
         /** Caches. */
        private final Set<String> caches;

        /** Caches. */
        private final int batchSize;

        /** Caches. */
        private final boolean keepBinary;

        public Arguments(Set<String> caches, int batchSize, boolean keepBinary) {
            this.caches = caches;
            this.batchSize = batchSize;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }

        @Override public VisorDrRepairPartitionCountersTaskArg toVisorArgs() {
            return new VisorDrRepairPartitionCountersTaskArg(caches, batchSize, keepBinary);
        }
    }

    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.apache.ignite.internal.visor.dr.VisorDrRepairPartitionCountersTask";
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrRepairPartitionCountersTaskResult res, Logger log) {
        boolean errors = CommandLogger.printErrors(res.exceptions(), "Dr partition repair task failed on nodes:", log);

        for (Entry<UUID, Collection<VisorDrRepairPartitionCountersJobResult>> nodeEntry : res.results().entrySet()) {
            Collection<VisorDrRepairPartitionCountersJobResult> cacheMetrics = nodeEntry.getValue();

            boolean errorWasPrinted = false;

            for (VisorDrRepairPartitionCountersJobResult cacheMetric : cacheMetrics) {
                if (!cacheMetric.hasIssues())
                    continue;

                errors = true;

                if (!errorWasPrinted) {
                    log.info(INDENT + "Issues found on node " + nodeEntry.getKey() + ":");
                    errorWasPrinted = true;
                }

                log.info(DOUBLE_INDENT + cacheMetric);
            }
        }

        if (!errors)
            log.severe("no issues found.");
        else
            log.severe("issues found (listed above).");

        log.info("");
    }

    /** {@inheritDoc} */
    @Override protected Arguments parseArguments0(CommandArgIterator argIter) {
        Set<String> caches = null;
        boolean keepBinary = false;
        int batchSize = 100;

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("");

            switch (nextArg.toLowerCase(Locale.ENGLISH)) {
                case CACHES_PARAM:
                    caches = CommandArgUtils.validateCachesArgument(argIter.nextCachesSet(CACHES_PARAM), REPAIR.toString());
                    break;
                case BATCH_SIZE:
                    batchSize = readBatchSizeParam(argIter, nextArg);
                    break;
                case KEEP_BINARY:
                    keepBinary = true;
                    break;
                default:
                    throw new IllegalArgumentException("Argument " + nextArg + " is not supported.");
            }
        }

        return new Arguments(caches, batchSize, keepBinary);
    }

    private int readBatchSizeParam(CommandArgIterator argIter, String nextArg) {
        if (!argIter.hasNextSubArg())
            throw new IllegalArgumentException(
                    "Numeric value for '" + nextArg + "' parameter expected.");

        int numVal;

        String numStr = argIter.nextArg("");

        try {
            numVal = Integer.parseInt(numStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Not numeric value was passed for '" + nextArg + "' parameter: " + numStr
            );
        }

        if (numVal <= 0)
            throw new IllegalArgumentException(
                    "Value for '" + nextArg + "' property should be positive.");

        return numVal;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return REPAIR.text().toUpperCase();
    }
}
