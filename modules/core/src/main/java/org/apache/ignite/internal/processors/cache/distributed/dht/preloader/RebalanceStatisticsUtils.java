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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collector.of;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * Utility class for rebalance statistics.
 */
class RebalanceStatisticsUtils {
    /** DTF for print into statistic output. */
    private static final DateTimeFormatter REBALANCE_STATISTICS_DTF = ofPattern("YYYY-MM-dd HH:mm:ss,SSS");

    /** Text for successful or not rebalances. */
    private static final String SUCCESSFUL_OR_NOT_REBALANCE_TEXT = "including successful and not rebalances";

    /** Text successful rebalance. */
    private static final String SUCCESSFUL_REBALANCE_TEXT = "successful rebalance";

    /**
     * Private constructor.
     */
    private RebalanceStatisticsUtils() {
        throw new RuntimeException("don't create");
    }

    /** Rebalance future statistics. */
    static class RebalanceFutureStatistics {
        /** Start rebalance time in mills. */
        private final long startTime = currentTimeMillis();

        /** End rebalance time in mills. */
        private volatile long endTime = startTime;

        /** First key - topic id. Second key - supplier node. */
        private final Map<Integer, Map<ClusterNode, RebalanceMessageStatistics>> msgStats = new ConcurrentHashMap<>();

        /** Is print rebalance statistics. */
        private final boolean printRebalanceStatistics = isPrintRebalanceStatistics();

        /**
         * Add new message statistics.
         * Requires invoke before send demand message.
         * This method required for {@code addReceivePartitionStatistics}.
         * This method add new message statistics if
         * {@link #printRebalanceStatistics} == true.
         *
         * @param topicId Topic id, require not null.
         * @param supplierNode Supplier node, require not null.
         * @see RebalanceMessageStatistics
         * @see #addReceivePartitionStatistics(Integer, ClusterNode, GridDhtPartitionSupplyMessage)
         */
        public void addMessageStatistics(final Integer topicId, final ClusterNode supplierNode) {
            assert nonNull(topicId);
            assert nonNull(supplierNode);

            if (!printRebalanceStatistics)
                return;

            msgStats.computeIfAbsent(topicId, integer -> new ConcurrentHashMap<>())
                .put(supplierNode, new RebalanceMessageStatistics(currentTimeMillis()));
        }

        /**
         * Add new statistics by receive message with partitions from supplier
         * node. Require invoke {@code addMessageStatistics} before send
         * demand message. This method add new message statistics if
         * {@link #printRebalanceStatistics} == true.
         *
         * @param topicId Topic id, require not null.
         * @param supplierNode Supplier node, require not null.
         * @param supplyMsg Supply message, require not null.
         * @see ReceivePartitionStatistics
         * @see #addMessageStatistics(Integer, ClusterNode)
         */
        public void addReceivePartitionStatistics(
            final Integer topicId,
            final ClusterNode supplierNode,
            final GridDhtPartitionSupplyMessage supplyMsg
        ) {
            assert nonNull(topicId);
            assert nonNull(supplierNode);
            assert nonNull(supplyMsg);

            if (!printRebalanceStatistics)
                return;

            List<PartitionStatistics> partStats = supplyMsg.infos().entrySet().stream()
                .map(entry -> new PartitionStatistics(entry.getKey(), entry.getValue().infos().size()))
                .collect(toList());

            msgStats.get(topicId).get(supplierNode).receivePartStats
                .add(new ReceivePartitionStatistics(currentTimeMillis(), supplyMsg.messageSize(), partStats));
        }

        /**
         * Clear statistics.
         */
        public void clear() {
            msgStats.clear();
        }

        /**
         * Set end rebalance time in mills.
         *
         * @param endTime End rebalance time in mills.
         */
        public void endTime(final long endTime) {
            this.endTime = endTime;
        }
    }

    /** Rebalance messages statistics. */
    static class RebalanceMessageStatistics {
        /** Time send demand message in mills. */
        private final long sndMsgTime;

        /** Statistics by received partitions. */
        private final Collection<ReceivePartitionStatistics> receivePartStats = new ConcurrentLinkedQueue<>();

        /**
         * Constructor.
         *
         * @param sndMsgTime time send demand message.
         */
        public RebalanceMessageStatistics(final long sndMsgTime) {
            this.sndMsgTime = sndMsgTime;
        }
    }

    /** Receive partition statistics. */
    static class ReceivePartitionStatistics {
        /** Time receive message(on demand message) with partition in mills. */
        private final long rcvMsgTime;

        /** Size receive message in bytes. */
        private final long msgSize;

        /** Received partitions. */
        private final List<PartitionStatistics> parts;

        /**
         * Constructor.
         *
         * @param rcvMsgTime time receive message in mills.
         * @param msgSize message size in bytes.
         * @param parts received partitions, require not null.
         */
        public ReceivePartitionStatistics(
            final long rcvMsgTime,
            final long msgSize,
            final List<PartitionStatistics> parts
        ) {
            assert nonNull(parts);

            this.rcvMsgTime = rcvMsgTime;
            this.msgSize = msgSize;
            this.parts = parts;
        }
    }

    /** Received partition info. */
    static class PartitionStatistics {
        /** Partition id. */
        private final int id;

        /** Count entries in partition. */
        private final int entryCount;

        /**
         * Constructor.
         *
         * @param id partition id.
         * @param entryCount count entries in partitions.
         */
        public PartitionStatistics(final int id, final int entryCount) {
            this.id = id;
            this.entryCount = entryCount;
        }
    }

    /**
     * Return is enable print statistics or not by
     * {@link IgniteSystemProperties#IGNITE_QUIET},
     * {@link IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS}.
     *
     * @return Is enable print statistics.
     */
    public static boolean isPrintRebalanceStatistics() {
        return !getBoolean(IGNITE_QUIET, true) && getBoolean(IGNITE_WRITE_REBALANCE_STATISTICS, false);
    }

    /**
     * Return is enable print partitions distribution by
     * {@link IgniteSystemProperties#IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS}.
     *
     * @return Is enable print partitions distribution.
     */
    public static boolean isPrintPartitionsDistribution() {
        return getBoolean(IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, false);
    }

    /**
     * Return rebalance statistics. It is recommended to call this method if
     * {@link #isPrintRebalanceStatistics()} == true.
     * <p/>
     * Flag {@code finish} means finished or not rebalance. <br/>
     * If {@code finish} == true then expected {@code rebFutrs} contains
     * success or not {@code RebalanceFuture} per cache group, else expected
     * {@code rebFutrs} contains only one success {@code RebalanceFuture}
     * for one cache group. <br/>
     * If {@code finish} == true then print total statistics.
     * <p/>
     * Partition distribution only for last success rebalance, per cache group.
     *
     * @param finish Is finish rebalance.
     * @param rebFutrs Involved in rebalance.
     * @return Rebalance statistics string.
     * @see RebalanceFuture RebalanceFuture
     */
    public static String rebalanceStatistics(
        final boolean finish,
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs
    ) {
        assert nonNull(rebFutrs);

        if (!isPrintRebalanceStatistics())
            return "";

        AtomicInteger nodeCnt = new AtomicInteger();

        Map<ClusterNode, Integer> nodeAliases = toRebalanceFutureStream(rebFutrs)
            .flatMap(future -> future.stat.msgStats.entrySet().stream())
            .flatMap(entry -> entry.getValue().keySet().stream())
            .distinct()
            .collect(toMap(identity(), node -> nodeCnt.getAndIncrement()));

        StringJoiner joiner = new StringJoiner(" ");

        if (finish)
            totalRebalanceStatistics(rebFutrs, nodeAliases, joiner);

        cacheGroupsRebalanceStatistics(rebFutrs, nodeAliases, finish, joiner);
        aliasesRebalanceStatistics("p - partitions, e - entries, b - bytes, d - duration", nodeAliases, joiner);
        partitionsDistributionRebalanceStatistics(rebFutrs, nodeAliases, nodeCnt, joiner);

        return joiner.toString();
    }

    /**
     * Write total statistics for rebalance.
     *
     * @param rebFutrs Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void totalRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(rebFutrs);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        long minStartTime = minStartTime(toRebalanceFutureStream(rebFutrs));
        long maxEndTime = maxEndTime(toRebalanceFutureStream(rebFutrs));

        Map<Integer, List<RebalanceMessageStatistics>> topicStat =
            toTopicStatistics(toRebalanceFutureStream(rebFutrs));

        Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat =
            toSupplierStatistics(toRebalanceFutureStream(rebFutrs));

        joiner.add(format("Total information (%s):", SUCCESSFUL_OR_NOT_REBALANCE_TEXT))
            .add(format("Time [%s]", toStartEndDuration(minStartTime, maxEndTime)));

        topicRebalanceStatistics(topicStat, joiner);
        supplierRebalanceStatistics(supplierStat, nodeAliases, joiner);
    }

    /**
     * Write rebalance statistics per cache group.
     * <p/>
     * If {@code finish} == true then add {@link #SUCCESSFUL_OR_NOT_REBALANCE_TEXT} else add {@link
     * #SUCCESSFUL_REBALANCE_TEXT} into header.
     *
     * @param rebFutrs Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     * @param finish Is finish rebalance.
     */
    private static void cacheGroupsRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs,
        final Map<ClusterNode, Integer> nodeAliases,
        final boolean finish,
        final StringJoiner joiner
    ) {
        assert nonNull(rebFutrs);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        joiner.add(format(
            "Information per cache group (%s):",
            finish ? SUCCESSFUL_OR_NOT_REBALANCE_TEXT : SUCCESSFUL_REBALANCE_TEXT
        ));

        rebFutrs.forEach((context, futures) -> {
            long minStartTime = minStartTime(futures.stream());
            long maxEndTime = maxEndTime(futures.stream());

            Map<Integer, List<RebalanceMessageStatistics>> topicStat = toTopicStatistics(futures.stream());
            Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat = toSupplierStatistics(futures.stream());

            joiner.add(format(
                "[id=%s, name=%s, %s]",
                context.groupId(),
                context.cacheOrGroupName(),
                toStartEndDuration(minStartTime, maxEndTime)
            ));

            topicRebalanceStatistics(topicStat, joiner);
            supplierRebalanceStatistics(supplierStat, nodeAliases, joiner);
        });
    }

    /**
     * Write partitions distribution per cache group. Only for last success rebalance.
     * Works if {@link #isPrintPartitionsDistribution()} set true.
     *
     * @param rebFutrs Participating in successful and not rebalances, require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param nodeCnt For adding new nodes into {@code nodeAliases}, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void partitionsDistributionRebalanceStatistics(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs,
        final Map<ClusterNode, Integer> nodeAliases,
        final AtomicInteger nodeCnt,
        final StringJoiner joiner
    ) {
        assert nonNull(rebFutrs);
        assert nonNull(nodeAliases);
        assert nonNull(nodeCnt);
        assert nonNull(joiner);

        if (!isPrintPartitionsDistribution())
            return;

        joiner.add(format("Partitions distribution per cache group (%s):", SUCCESSFUL_REBALANCE_TEXT));

        Comparator<RebalanceFuture> startTimeCmp = comparingLong(fut -> fut.stat.startTime);

        rebFutrs.forEach((context, futures) -> {
            joiner.add(format("[id=%s, name=%s]", context.groupId(), context.cacheOrGroupName()));

            RebalanceFuture lastSuccessFuture = futures.stream()
                .filter(GridFutureAdapter::isDone)
                .filter(RebalanceStatisticsUtils::result)
                .sorted(startTimeCmp.reversed())
                .findFirst()
                .orElse(null);

            if (isNull(lastSuccessFuture))
                return;

            AffinityAssignment affinity = context.affinity().cachedAffinity(lastSuccessFuture.topologyVersion());

            Map<ClusterNode, Set<Integer>> primaryParts = affinity.nodes().stream()
                .collect(toMap(identity(), node -> affinity.primaryPartitions(node.id())));

            Stream<Map.Entry<ClusterNode, RebalanceMessageStatistics>> msgStatPerSupplierStream =
                lastSuccessFuture.stat.msgStats.entrySet().stream()
                    .flatMap(entry -> entry.getValue().entrySet().stream());

            Stream<T2<ClusterNode, PartitionStatistics>> partsStatPerSupplierStream = msgStatPerSupplierStream
                .flatMap(entry -> entry.getValue().receivePartStats.stream()
                    .flatMap(rps -> rps.parts.stream())
                    .map(ps -> new T2<>(entry.getKey(), ps))
                );

            partsStatPerSupplierStream
                .sorted(comparingInt(t2 -> t2.get2().id))
                .forEach(t2 -> {
                    ClusterNode supplierNode = t2.get1();
                    int partId = t2.get2().id;

                    String nodes = affinity.get(partId).stream()
                        .peek(node -> nodeAliases.computeIfAbsent(node, node1 -> nodeCnt.getAndIncrement()))
                        .sorted(comparingInt(nodeAliases::get))
                        .map(node -> "[" + nodeAliases.get(node) +
                            (primaryParts.get(node).contains(partId) ? ",pr" : ",bu") +
                            (node.equals(supplierNode) ? ",su" : "") + "]"
                        )
                        .collect(joining(","));

                    joiner.add(partId + " = " + nodes);
                });
        });

        aliasesRebalanceStatistics("pr - primary, bu - backup, su - supplier node", nodeAliases, joiner);
    }

    /**
     * Write statistics per topic.
     *
     * @param topicStat Statistics by topics (in successful and not rebalances), require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void topicRebalanceStatistics(
        final Map<Integer, List<RebalanceMessageStatistics>> topicStat,
        final StringJoiner joiner
    ) {
        assert nonNull(topicStat);
        assert nonNull(joiner);

        joiner.add("Topic statistics:");

        topicStat.forEach((topicId, msgStats) -> {
            long partCnt = sum(msgStats, rps -> rps.parts.size());
            long byteSum = sum(msgStats, rps -> rps.msgSize);
            long entryCount = sum(msgStats, rps -> rps.parts.stream().mapToLong(ps -> ps.entryCount).sum());

            joiner.add(format("[id=%s, %s]", topicId, toPartitionsEntriesBytes(partCnt, entryCount, byteSum)));
        });
    }

    /**
     * Write stattistics per supplier node.
     *
     * @param supplierStat Statistics by supplier (in successful and not rebalances), require not null.
     * @param nodeAliases For print nodeId=1 instead long string, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void supplierRebalanceStatistics(
        final Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(supplierStat);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        joiner.add("Supplier statistics:");

        supplierStat.forEach((supplierNode, msgStats) -> {
            long partCnt = sum(msgStats, rps -> rps.parts.size());
            long byteSum = sum(msgStats, rps -> rps.msgSize);
            long entryCount = sum(msgStats, rps -> rps.parts.stream().mapToLong(ps -> ps.entryCount).sum());

            long durationSum = msgStats.stream()
                .flatMapToLong(msgStat -> msgStat.receivePartStats.stream()
                    .mapToLong(rps -> rps.rcvMsgTime - msgStat.sndMsgTime)
                )
                .sum();

            joiner.add(format(
                "[nodeId=%s, %s, d=%s ms]",
                nodeAliases.get(supplierNode),
                toPartitionsEntriesBytes(partCnt, entryCount, byteSum),
                durationSum
            ));
        });
    }

    /**
     * Write statistics aliases, for reducing output string.
     *
     * @param nodeAliases for print nodeId=1 instead long string, require not null.
     * @param abbreviations Abbreviations ex. b - bytes, require not null.
     * @param joiner For write statistics, require not null.
     */
    private static void aliasesRebalanceStatistics(
        final String abbreviations,
        final Map<ClusterNode, Integer> nodeAliases,
        final StringJoiner joiner
    ) {
        assert nonNull(abbreviations);
        assert nonNull(nodeAliases);
        assert nonNull(joiner);

        String nodes = nodeAliases.entrySet().stream()
            .sorted(comparingInt(Map.Entry::getValue))
            .map(entry -> format("[%s=%s,%s]", entry.getValue(), entry.getKey().id(), entry.getKey().consistentId()))
            .collect(joining(", "));

        joiner.add("Aliases: " + abbreviations + ", nodeId mapping (nodeId=id,consistentId) " + nodes);
    }

    /**
     * Return the result of a rebalance future with wrapping {@link IgniteCheckedException} into {@link
     * IgniteException}.
     *
     * @param future Rebalance future, require not null.
     * @return Result future.
     */
    public static boolean result(final IgniteInternalFuture<Boolean> future) {
        assert nonNull(future);

        try {
            return future.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Return the local date-time of time in mills.
     *
     * @param time Time in mills.
     * @return The local date-time.
     */
    private static LocalDateTime toLocalDateTime(final long time) {
        return new Date(time).toInstant().atZone(systemDefault()).toLocalDateTime();
    }

    /**
     * Return min {@link RebalanceFutureStatistics#startTime} in stream rebalance future's.
     *
     * @param stream Stream rebalance future, require not null.
     * @return Min start time.
     */
    private static long minStartTime(final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.mapToLong(value -> value.stat.startTime).min().orElse(0);
    }

    /**
     * Return max {@link RebalanceFutureStatistics#endTime} in stream rebalance future's.
     *
     * @param stream Stream rebalance future's, require not null.
     * @return Max end time.
     */
    private static long maxEndTime(final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.mapToLong(value -> value.stat.endTime).max().orElse(0);
    }

    /**
     * Return stream rebalance future's of each cache groups.
     *
     * @param rebFutrs Rebalance future's by cache groups, require not null.
     * @return Stream rebalance future's.
     */
    private static Stream<RebalanceFuture> toRebalanceFutureStream(
        final Map<CacheGroupContext, Collection<RebalanceFuture>> rebFutrs
    ) {
        assert nonNull(rebFutrs);

        return rebFutrs.entrySet().stream().flatMap(entry -> entry.getValue().stream());
    }

    /**
     * Returns statistics grouped by topic number.
     *
     * @param stream Stream rebalance future's, require not null.
     * @return Statistic by topics.
     */
    private static Map<Integer, List<RebalanceMessageStatistics>> toTopicStatistics(
        final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.flatMap(future -> future.stat.msgStats.entrySet().stream())
            .collect(groupingBy(
                Map.Entry::getKey,
                mapping(
                    entry -> entry.getValue().values(),
                    of(
                        ArrayList::new,
                        Collection::addAll,
                        (ms1, ms2) -> {
                            ms1.addAll(ms2);
                            return ms1;
                        }
                    )
                )
            ));
    }

    /**
     * Returns statistics grouped by supplier node.
     *
     * @param stream Stream rebalance future's, require not null.
     * @return Statistic by supplier.
     */
    private static Map<ClusterNode, List<RebalanceMessageStatistics>> toSupplierStatistics(
        final Stream<RebalanceFuture> stream
    ) {
        assert nonNull(stream);

        return stream.flatMap(future -> future.stat.msgStats.entrySet().stream())
            .flatMap(entry -> entry.getValue().entrySet().stream())
            .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toList())));
    }

    /**
     * Return a formatted string to display the rebalance time. Format "start=%s, end=%s, d=%s ms" where start, end use
     * {@code REBALANCE_STATISTICS_DTF} and d is the duration between the start and end in mills. Example:
     * "start=2019-08-01 13:15:10,100, end=2019-08-01 13:15:10,200, d=100 ms".
     *
     * @param start Start time in ms.
     * @param end End time in ms.
     * @return Formatted string of rebalance time.
     * @see #REBALANCE_STATISTICS_DTF
     */
    private static String toStartEndDuration(final long start, final long end) {
        return format(
            "start=%s, end=%s, d=%s ms",
            REBALANCE_STATISTICS_DTF.format(toLocalDateTime(start)),
            REBALANCE_STATISTICS_DTF.format(toLocalDateTime(end)),
            end - start
        );
    }

    /**
     * Return sum of long values that extracted from message statistics.
     *
     * @param msgStats Message statistics, require not null.
     * @param longExtractor Long extractor, require not null.
     * @return Sum of long values.
     */
    private static long sum(
        final List<RebalanceMessageStatistics> msgStats,
        final ToLongFunction<? super ReceivePartitionStatistics> longExtractor
    ) {
        assert nonNull(msgStats);
        assert nonNull(longExtractor);

        return msgStats.stream()
            .flatMap(msgStat -> msgStat.receivePartStats.stream())
            .mapToLong(longExtractor)
            .sum();
    }

    /**
     * Return a formatted string to display the received rebalance partitions. Format "p=%s, e=%s, b=%s". Example: "p=1,
     * e=10, b=1024".
     *
     * @param parts Count received partitions.
     * @param entries Count received entries.
     * @param bytes Sum received bytes.
     * @return Formatted string of received rebalance partitions.
     */
    private static String toPartitionsEntriesBytes(final long parts, final long entries, final long bytes) {
        return format("p=%s, e=%s, b=%s", parts, entries, bytes);
    }
}
