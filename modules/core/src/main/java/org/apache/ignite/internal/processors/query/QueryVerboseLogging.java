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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class QueryVerboseLogging {

    /** Query span descriptors */
    protected static final ConcurrentMap<String, TargetQueryDescriptor> queries = new ConcurrentLinkedHashMap<>();

    /** Settings */
    private static Settings cfg;

    /** Logger */
    private static volatile IgniteLogger log;

    /** Local node id */
    private static volatile UUID locNodeId;

    public static void init(GridKernalContext ctx) {
        refreshCfg();
        if (cfg.isEnabled()) {
            log = ctx.log("QVL");
            locNodeId = ctx.localNodeId();
        }
    }

    static void refreshCfg() {
        cfg = new Settings();
    }

    public static void register(GridRunningQueryInfo queryInfo) {
        register(
                queryInfo.label(),
                queryInfo.query(),
                queryInfo.globalQueryId(),
                queryInfo.queryType(),
                queryInfo.lazy(),
                queryInfo.distributedJoins(),
                queryInfo.enforceJoinOrder()
        );
    }

    public static void register(@Nullable String label,
                                @NotNull String qryString,
                                @NotNull String globalQryId,
                                @Nullable GridCacheQueryType qryType,
                                boolean lazy,
                                boolean distrJoins,
                                boolean enforceJoinOrder
    ) {
        if (cfg.isEnabled()
                && label != null
                && cfg.pattern().matcher(label).matches()) {

            cleanup();

            TargetQueryDescriptor previous = queries.put(globalQryId, new TargetQueryDescriptor(
                    label, qryString, globalQryId, qryType, lazy, distrJoins, enforceJoinOrder
            ));

            if (previous != null) {
                previous.log("Registration push out").finish();
            }
        }
    }

    /** Flag of running cleanup */
    private static volatile boolean cleanupInProcess = false;

    /** Check if we exceeded max size of queries map and make cleanup */
    private static void cleanup() {
        if (!cleanupInProcess && queries.size() > cfg.maxSize()) {
            synchronized (QueryVerboseLogging.class) {
                cleanupInProcess = true;

                // Try to finish too old first
                if (queries.size() > cfg.maxSize()) {
                    Iterator<Map.Entry<String, TargetQueryDescriptor>> iter = queries.entrySet().iterator();

                    while (iter.hasNext()) {
                        try {
                            TargetQueryDescriptor descriptor = iter.next().getValue();

                            if (descriptor != null && descriptor.isTooOld()) {
                                descriptor.log("Force finish by TTL").finish();
                                iter.remove();
                            }
                        } catch (Exception e) {
                            logMsg("Failed to advance completion iterator: " + e.getMessage());
                        }
                    }
                }

                // If we still have too many entries perform force cleanup of first half of the map
                // in order to prevent heap leak at all costs
                int size = queries.size();
                if (size > cfg.maxSize()) {
                    logMsg("Force finish due to too many entries: " + size);

                    Iterator<Map.Entry<String, TargetQueryDescriptor>> iter = queries.entrySet().iterator();
                    int deletionsAmount = size / 2;
                    int deletions = 0;

                    while (iter.hasNext() && deletions < deletionsAmount) {
                        try {
                            TargetQueryDescriptor descriptor = iter.next().getValue();

                            descriptor.log("Force finish by size").finish();
                            iter.remove();
                            deletions++;

                        } catch (Exception e) {
                            logMsg("Failed to advance completion iterator: " + e.getMessage());
                        }
                    }
                }

                cleanupInProcess = false;
            }
        }
    }

    public static void ifPresent(String globalQueryId, Consumer<TargetQueryDescriptor> action) {
        if (cfg.isEnabled()) {
            TargetQueryDescriptor descriptor = queries.get(globalQueryId);
            if (descriptor != null) {
                action.accept(descriptor);
            }
        }
    }

    public static void logLocalSpan(@Nullable Long queryId, Supplier<String> messageSupplier) {
        logLocalSpan(VerbosityLevel.INFO, queryId, messageSupplier);
    }

    public static void logLocalSpan(@NotNull VerbosityLevel lvl, @Nullable Long queryId, Supplier<String> messageSupplier) {
        logSpan(lvl, locNodeId, queryId, messageSupplier);
    }

    public static void logSpan(@NotNull UUID nodeId, @Nullable Long queryId, Supplier<String> messageSupplier) {
        logSpan(VerbosityLevel.INFO, nodeId, queryId, messageSupplier);
    }

    public static void logSpan(@NotNull VerbosityLevel lvl, @Nullable UUID nodeId, @Nullable Long queryId, Supplier<String> messageSupplier) {
        if (queryId != null && nodeId != null)
            logSpan(lvl, QueryUtils.globalQueryId(nodeId, queryId), messageSupplier);
    }

    public static void logSpan(@NotNull VerbosityLevel lvl, String globalQueryId, Supplier<String> messageSupplier) {
        ifPresent(globalQueryId, d -> {
            if (cfg.verbosity().allows(lvl))
                d.log(messageSupplier.get());
            else
                d.touch();
        });
    }

    public static void finish(@Nullable Long queryId, Supplier<String> messageSupplier) {
        finish(locNodeId, queryId, messageSupplier);
    }

    public static void finish(@Nullable UUID nodeId, @Nullable Long queryId, Supplier<String> messageSupplier) {
        if (queryId != null && nodeId != null)
            finish(QueryUtils.globalQueryId(nodeId, queryId), messageSupplier);
    }

    private static void finish(String globalQueryId, Supplier<String> messageSupplier) {
        if (cfg.isEnabled()) {
            TargetQueryDescriptor descriptor = queries.remove(globalQueryId);
            if (descriptor != null) {
                descriptor.log(messageSupplier.get()).finish();
            }
        }
    }

    private static void logMsg(String message) {
        if (log == null || !log.isInfoEnabled()) {
            System.out.println(Instant.now().atOffset(ZoneOffset.UTC).format(U.LONG_DATE_FMT) + " [QVL] " + message);
        } else
            log.info(message);
    }

    public static String id(ClusterNode node) {
        return node != null ? U.id8(node.id()) : "-";
    }

    /** Query logging descriptor */
    public static class TargetQueryDescriptor {

        /** Span time format */
        private static final DateTimeFormatter TIME_FORMAT = new DateTimeFormatterBuilder()
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 2)
                .optionalStart()
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendFraction(MICRO_OF_SECOND, 4, 4, true)
                .toFormatter()
                .withResolverStyle(ResolverStyle.STRICT);

        /** Start timestamp */
        private final long startTs = System.currentTimeMillis();

        /** Report content */
        private final StringBuilder report = new StringBuilder();

        /** Completion flag */
        private volatile long lastTouchTs = System.currentTimeMillis();

        public TargetQueryDescriptor(@Nullable String label,
                                     @NotNull String query,
                                     @NotNull String globalQueryId,
                                     @Nullable GridCacheQueryType queryType,
                                     boolean lazy,
                                     boolean distributedJoins,
                                     boolean enforceJoinOrder
        ) {
            report.append(" start=").append(Instant.now()
                            .atOffset(ZoneOffset.UTC)
                            .format(DateTimeFormatter.ISO_TIME)).append(" ")
                    .append("label=").append(label).append(", ")
                    .append("id=").append(globalQueryId).append(", ")
                    .append("type=").append(queryType).append(", ")
                    .append("lazy=").append(lazy).append(", ")
                    .append("dj=").append(distributedJoins).append(", ")
                    .append("ejo=").append(enforceJoinOrder).append(", ")
                    .append("qry={").append(query.replace("\n", " ")).append('}')
                    .append("\n");
        }

        private TargetQueryDescriptor log(String span) {
            Instant now = Instant.now();
            report
                    .append("  ")
                    .append(now.atOffset(ZoneOffset.UTC).format(TIME_FORMAT)).append(' ')
                    .append('[').append(Thread.currentThread().getName()).append("] ");

            if (locNodeId != null)
                report.append('[').append(U.id8(locNodeId)).append("] ");

            report.append(span).append("\n");

            return touch();
        }

        public TargetQueryDescriptor touch() {
            lastTouchTs = System.currentTimeMillis();
            return this;
        }

        public boolean isTooOld() {
            return (System.currentTimeMillis() - lastTouchTs) > cfg.ttl();
        }

        public void finish() {
            long duration = System.currentTimeMillis() - startTs;

            if (cfg.minDuration() < duration) {
                log("Finish [duration=" + duration + "]");
                QueryVerboseLogging.logMsg(toString());
            }
        }

        @Override
        public String toString() {
            return report.toString();
        }
    }

    /** Verbosity level for span messages */
    public enum VerbosityLevel {
        INFO,
        DEBUG,
        TRACE;

        public static VerbosityLevel get() {
            String lvlStr = IgniteSystemProperties.getString("QVL_VERBOSITY", VerbosityLevel.INFO.name());
            for (VerbosityLevel v : values()) {
                if (v.name().equalsIgnoreCase(lvlStr))
                    return v;
            }

            return VerbosityLevel.INFO;
        }

        public boolean allows(VerbosityLevel lvl) {
            return this == lvl || ordinal() > lvl.ordinal();
        }
    }

    /** Settings holder. Created as a separate class for testing purpose */
    static class Settings {

        /** Property */
        static final String QVL_ENABLED_PROP = "QVL_ENABLED";

        /** Property */
        static final String QVL_FORCE_FINISH_MAX_SIZE_PROP = "QVL_FORCE_FINISH_MAX_SIZE";

        /** Property */
        static final String QVL_FORCE_FINISH_TTL_PROP = "QVL_FORCE_FINISH_TTL";

        /** Property */
        static final String QVL_TARGET_QUERY_MIN_DURATION_PROP = "QVL_MIN_DURATION";

        /** Property */
        static final String QVL_TARGET_QUERY_LABEL_PROP = "QVL_LABEL_PATTERN";

        /** Global flag */
        private final boolean isEnabled = IgniteSystemProperties.getBoolean(QVL_ENABLED_PROP, true);

        /** Force finish threshold for queries map */
        private final int maxSize = IgniteSystemProperties.getInteger(QVL_FORCE_FINISH_MAX_SIZE_PROP, 128);

        /** Force finish threshold for descriptor TTL */
        private final long ttl = IgniteSystemProperties.getLong(QVL_FORCE_FINISH_TTL_PROP, 30_000);

        /** Force finish threshold for descriptor TTL */
        private final long minDuration = IgniteSystemProperties.getLong(QVL_TARGET_QUERY_MIN_DURATION_PROP, 60_000);

        /** Verbosity level */
        private final VerbosityLevel verbosity = VerbosityLevel.get();

        /** Inclusion pattern */
        private final Pattern pattern;

        private Settings() {
            this.pattern = resolvePattern();
        }

        public boolean isEnabled() {
            return isEnabled && pattern != null;
        }

        public int maxSize() {
            return maxSize;
        }

        public long ttl() {
            return ttl;
        }

        public long minDuration() {
            return minDuration;
        }

        public VerbosityLevel verbosity() {
            return verbosity;
        }

        public Pattern pattern() {
            return pattern;
        }

        private Pattern resolvePattern() {
            String pattern = IgniteSystemProperties.getString(QVL_TARGET_QUERY_LABEL_PROP, null);

            if (pattern == null) return null;

            try {
                return Pattern.compile(pattern);

            } catch (Exception e) {
                logMsg("Failed to create query pattern from: '" + pattern + "'");
                return null;
            }
        }
    }
}
