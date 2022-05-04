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

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.CHECK;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;

import java.util.Collection;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.dr.VisorDrCheckPartitionCountersJobResult;
import org.apache.ignite.internal.visor.dr.VisorDrCheckPartitionCountersTaskArg;
import org.apache.ignite.internal.visor.dr.VisorDrCheckPartitionCountersTaskResult;

/**
 * DR check partition counters command.
 */
public class DrCheckPartitionCountersCommand extends DrAbstractRemoteSubCommand<VisorDrCheckPartitionCountersTaskArg, VisorDrCheckPartitionCountersTaskResult, DrCheckPartitionCountersCommand.Arguments> {
    /** Check first N entries parameter. */
    public static final String CHECK_FIRST_PARAM = "--check-first";

    /** Caches parameter. */
    public static final String CACHES_PARAM = "--caches";

    /**
     * Container for command arguments.
     */
    public class Arguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrCheckPartitionCountersTaskArg> {
         /** Caches. */
        private final Set<String> caches;

        /** Max number of entries to be checked. */
        private final int checkFirst;

        /**
         * Constructor.
         *
         * @param caches Caches.
         * @param checkFirst Max number of entries to be checked.
         */
        public Arguments(
            Set<String> caches,
            int checkFirst
        ) {
            this.caches = caches;
            this.checkFirst = checkFirst;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }

        @Override public VisorDrCheckPartitionCountersTaskArg toVisorArgs() {
            return new VisorDrCheckPartitionCountersTaskArg(caches, checkFirst);
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
        Set<String> caches = null;

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("");

            switch (nextArg.toLowerCase(Locale.ENGLISH)) {
                case CHECK_FIRST_PARAM:
                    checkFirst = readCheckFirstParam(argIter, nextArg);
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
                default:
                    throw new IllegalArgumentException("Argument " + nextArg + " is not supported.");
            }
        }

        if (checkFirst == -1)
            throw new IllegalArgumentException(CHECK_FIRST_PARAM + " argument expected.");

        return new Arguments(caches, checkFirst);
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
