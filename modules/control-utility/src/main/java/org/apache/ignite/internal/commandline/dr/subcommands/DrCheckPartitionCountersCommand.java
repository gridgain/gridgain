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
import org.apache.ignite.internal.visor.dr.VisorDrCheckPartitionCountersJobResult;
import org.apache.ignite.internal.visor.dr.VisorDrCheckPartitionCountersTaskArg;
import org.apache.ignite.internal.visor.dr.VisorDrCheckPartitionCountersTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.CHECK;

/**
 * DR check partition counters command.
 */
public class DrCheckPartitionCountersCommand extends
    DrAbstractRemoteSubCommand<VisorDrCheckPartitionCountersTaskArg, VisorDrCheckPartitionCountersTaskResult, DrCheckPartitionCountersCommand.Arguments> {
    /** Check first N entries parameter. */
    public static final String CHECK_FIRST_PARAM = "--check-first";

    /** Caches parameter. */
    public static final String CACHES_PARAM = "--caches";

    /** Scan until first error flag parameter. */
    public static final String SCAN_UNTIL_FIRST_ERROR = "--scan-until-first-error";

    /**
     * Container for command arguments.
     */
    public class Arguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrCheckPartitionCountersTaskArg> {
         /** Caches. */
        private final Set<String> caches;

        /** Max number of entries to be checked. */
        private final int checkFirst;

        /** Scan until first broken counter is found. */
        private final boolean scanUntilFirstError;

        /**
         * Constructor.
         *
         * @param caches Caches.
         * @param checkFirst Max number of entries to be checked.
         * @param scanUntilFirstError Scan until first broken counter is found flag.
         */
        public Arguments(
            Set<String> caches,
            int checkFirst,
            boolean scanUntilFirstError
        ) {
            this.caches = caches;
            this.checkFirst = checkFirst;
            this.scanUntilFirstError = scanUntilFirstError;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }

        @Override public VisorDrCheckPartitionCountersTaskArg toVisorArgs() {
            return new VisorDrCheckPartitionCountersTaskArg(caches, checkFirst, scanUntilFirstError);
        }
    }

    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.apache.ignite.internal.visor.dr.VisorDrCheckPartitionCountersTask";
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrCheckPartitionCountersTaskResult res, Logger log) {
        boolean errors = CommandLogger.printErrors(res.exceptions(), "Check partition counters failed on nodes:", log);

        for (Entry<UUID, Collection<VisorDrCheckPartitionCountersJobResult>> nodeEntry : res.results().entrySet()) {
            Collection<VisorDrCheckPartitionCountersJobResult> cacheMetrics = nodeEntry.getValue();

            boolean errorWasPrinted = false;

            for (VisorDrCheckPartitionCountersJobResult cacheMetric : cacheMetrics) {
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
        int checkFirst = -1;
        boolean scanUntilFirstError = false;
        Set<String> caches = null;

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("");

            switch (nextArg.toLowerCase(Locale.ENGLISH)) {
                case CHECK_FIRST_PARAM:
                    checkFirst = readCheckFirstParam(argIter, nextArg);
                    break;
                case SCAN_UNTIL_FIRST_ERROR:
                    scanUntilFirstError = true;
                    break;
                case CACHES_PARAM:
                    caches = CommandArgUtils.validateCachesArgument(argIter.nextCachesSet(CACHES_PARAM), CHECK.toString());
                    break;
                default:
                    throw new IllegalArgumentException("Argument " + nextArg + " is not supported.");
            }
        }

        return new Arguments(caches, checkFirst, scanUntilFirstError);
    }

    private int readCheckFirstParam(CommandArgIterator argIter, String nextArg) {
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
        return CHECK.text().toUpperCase();
    }
}
