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

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.dr.DrSubCommandsList;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.dr.VisorDrCacheTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrCacheTaskResult;

import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;

/** */
public class DrCacheCommand extends
    DrAbstractSubCommand<VisorDrCacheTaskArgs, VisorDrCacheTaskResult, DrCacheCommand.DrCacheArguments>
{
    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrCacheTask";
    }

    /** {@inheritDoc} */
    @Override public DrCacheArguments parseArguments0(CommandArgIterator argIter) {
        String regex = argIter.nextArg("Cache name regex expected.");

        try {
            Pattern.compile(regex);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Cache name regex is not valid.", e);
        }

        boolean config = false;
        boolean metrics = false;
        CacheFilter cacheFilter = CacheFilter.ALL;
        SenderGroup senderGroup = SenderGroup.ALL;
        String senderGroupName = null;
        Action action = null;

        String nextArg;

        //noinspection LabeledStatement
        args_loop: while ((nextArg = argIter.peekNextArg()) != null) {
            switch (nextArg.toLowerCase(Locale.ENGLISH)) {
                case "--config":
                    argIter.nextArg(null);
                    config = true;

                    break;

                case "--metrics":
                    argIter.nextArg(null);
                    metrics = true;

                    break;

                case "--cache-filter": {
                    argIter.nextArg(null);

                    String errorMsg = "--cache-filter parameter value required.";

                    String cacheFilterStr = argIter.nextArg(errorMsg);
                    cacheFilter = CacheFilter.valueOf(cacheFilterStr.toUpperCase(Locale.ENGLISH));

                    if (cacheFilter == null)
                        throw new IllegalArgumentException(errorMsg);

                    break;
                }

                case "--sender-group": {
                    argIter.nextArg(null);

                    String arg = argIter.nextArg("--sender-group parameter value required.");

                    senderGroup = SenderGroup.parse(arg);

                    if (senderGroup == null)
                        senderGroupName = arg;

                    break;
                }

                case "--action": {
                    argIter.nextArg(null);

                    String errorMsg = "--action parameter value required.";

                    action = Action.parse(argIter.nextArg(errorMsg));

                    if (action == null)
                        throw new IllegalArgumentException(errorMsg);

                    break;
                }

                default:
                    //noinspection BreakStatementWithLabel
                    break args_loop;
            }
        }

        return new DrCacheArguments(regex, config, metrics, cacheFilter, senderGroup, senderGroupName, action, (byte)0);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (arg().action != null)
            return "Warning: this command will change data center replication state for selected caches.";

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrCacheTaskResult res, Logger log) {
        log.info("Data Center ID: " + res.getDataCenterId());

        log.info(DELIM);

        if (res.getDataCenterId() == 0) {
            log.info("Data Replication state: is not configured.");

            return;
        }

        List<String> cacheNames = res.getCacheNames();
        if (cacheNames.isEmpty()) {
            log.info("No matching caches found");

            return;
        }

        log.info(String.format("%d matching cache(s): %s", cacheNames.size(), cacheNames));

        for (String cacheName : cacheNames) {
            List<T2<String, Object>> cacheSndCfg = res.getSenderConfig().get(cacheName);

            printList(log, cacheSndCfg, String.format(
                "Sender configuration for cache \"%s\":",
                cacheName
            ));

            List<T2<String, Object>> cacheRcvCfg = res.getReceiverConfig().get(cacheName);

            printList(log, cacheRcvCfg, String.format(
                "Receiver configuration for cache \"%s\":",
                cacheName
            ));
        }

        for (String cacheName : cacheNames) {
            List<T2<String, Object>> cacheSndMetrics = res.getSenderMetrics().get(cacheName);

            printList(log, cacheSndMetrics, String.format(
                "Sender metrics for cache \"%s\":",
                cacheName
            ));

            List<T2<String, Object>> cacheRcvMetrics = res.getReceiverMetrics().get(cacheName);

            printList(log, cacheRcvMetrics, String.format(
                "Receiver metrics for cache \"%s\":",
                cacheName
            ));
        }

        for (String msg : res.getResultMessages())
            log.info(msg);
    }

    /** */
    private static void printList(Logger log, List<T2<String, Object>> cfg, String s) {
        if (cfg != null && !cfg.isEmpty()) {
            log.info(s);

            for (T2<String, Object> t2 : cfg)
                log.info(String.format(INDENT + "%s=%s", t2.toArray()));
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DrSubCommandsList.CACHE.text();
    }

    /** */
    public enum CacheFilter {
        ALL,
        SENDING,
        RECEIVING,
        PAUSED,
        ERROR
    }

    /** */
    public enum SenderGroup {
        ALL,
        DEFAULT,
        NONE;

        public static SenderGroup parse(String text) {
            try {
                return valueOf(text.toUpperCase(Locale.ENGLISH));
            }
            catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    /** */
    public enum Action {
        STOP("stop"),
        START("start"),
        FULL_STATE_TRANSFER("full-state-transfer");

        private final String text;

        Action(String text) {
            this.text = text;
        }

        public String text() {
            return text;
        }

        public static Action parse(String text) {
            for (Action action : values()) {
                if (action.text.equalsIgnoreCase(text))
                    return action;
            }

            return null;
        }
    }

    /** */
    public static class DrCacheArguments implements DrAbstractSubCommand.Arguments<VisorDrCacheTaskArgs> {
        private final String regex;
        private final boolean config;
        private final boolean metrics;
        private final CacheFilter filter;
        private final SenderGroup senderGroup;
        private final String senderGroupName;
        private final Action action;
        private final byte remoteDataCenterId;

        public DrCacheArguments(
            String regex,
            boolean config,
            boolean metrics,
            CacheFilter filter,
            SenderGroup senderGroup,
            String senderGroupName,
            Action action,
            byte remoteDataCenterId
        ) {
            this.regex = regex;
            this.config = config;
            this.metrics = metrics;
            this.filter = filter;
            this.senderGroup = senderGroup;
            this.senderGroupName = senderGroupName;
            this.action = action;
            this.remoteDataCenterId = remoteDataCenterId;
        }

        /** {@inheritDoc} */
        @Override public VisorDrCacheTaskArgs toVisorArgs() {
            return new VisorDrCacheTaskArgs(
                regex,
                config,
                metrics,
                filter.ordinal(),
                senderGroup == null ? VisorDrCacheTaskArgs.SENDER_GROUP_NAMED : senderGroup.ordinal(),
                senderGroupName,
                action == null ? VisorDrCacheTaskArgs.ACTION_NONE : action.ordinal(),
                remoteDataCenterId
            );
        }
    }
}
