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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUpdateVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
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
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;

/**
 * Thread pool for requesting partitions from other nodes and populating local cache.
 */
public class GridDhtPartitionDemander {
    /** DTF for print into statistic output */
    private static final DateTimeFormatter REBALANCE_STATISTICS_DTF = ofPattern("YYYY-MM-dd HH:mm:ss,SSS");

    /** Text for successful or not rebalances */
    private static final String SUCCESSFUL_OR_NOT_REBALANCE_TEXT = "including successful and not rebalances";

    /** Text successful rebalance */
    private static final String SUCCESSFUL_REBALANCE_TEXT = "successful rebalance";

    /** */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final IgniteLogger log;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo> preloadPred;

    /** Future for preload mode {@link CacheRebalanceMode#SYNC}. */
    @GridToStringInclude
    private final GridFutureAdapter syncFut = new GridFutureAdapter();

    /** Rebalance future. */
    @GridToStringInclude
    private volatile RebalanceFuture rebalanceFut;

    /** Last timeout object. */
    private AtomicReference<GridTimeoutObject> lastTimeoutObj = new AtomicReference<>();

    /** Last exchange future. */
    private volatile GridDhtPartitionsExchangeFuture lastExchangeFut;

    /** Cached rebalance topics. */
    private final Map<Integer, Object> rebalanceTopics;

    /** Futures involved in the last rebalance. For statistics. */
    @GridToStringExclude
    private final List<RebalanceFuture> lastStatFutures = new CopyOnWriteArrayList<>();

    /**
     * @param grp Ccahe group.
     */
    public GridDhtPartitionDemander(CacheGroupContext grp) {
        assert grp != null;

        this.grp = grp;

        ctx = grp.shared();

        log = ctx.logger(getClass());

        boolean enabled = grp.rebalanceEnabled() && !ctx.kernalContext().clientNode();

        rebalanceFut = new RebalanceFuture(); //Dummy.

        if (!enabled) {
            // Calling onDone() immediately since preloading is disabled.
            rebalanceFut.onDone(true);
            syncFut.onDone();
        }

        Map<Integer, Object> tops = new HashMap<>();

        for (int idx = 0; idx < grp.shared().kernalContext().config().getRebalanceThreadPoolSize(); idx++)
            tops.put(idx, GridCachePartitionExchangeManager.rebalanceTopic(idx));

        rebalanceTopics = tops;
    }

    /**
     * Start.
     */
    void start() {
        // No-op.
    }

    /**
     * Stop.
     */
    void stop() {
        try {
            rebalanceFut.cancel();
        }
        catch (Exception ignored) {
            rebalanceFut.onDone(false);
        }

        lastExchangeFut = null;

        lastTimeoutObj.set(null);

        syncFut.onDone();
    }

    /**
     * @return Future for {@link CacheRebalanceMode#SYNC} mode.
     */
    IgniteInternalFuture<?> syncFuture() {
        return syncFut;
    }

    /**
     * @return Rebalance future.
     */
    IgniteInternalFuture<Boolean> rebalanceFuture() {
        return rebalanceFut;
    }

    /**
     * Sets preload predicate for demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /**
     * @return Rebalance future.
     */
    IgniteInternalFuture<Boolean> forceRebalance() {
        GridTimeoutObject obj = lastTimeoutObj.getAndSet(null);

        if (obj != null)
            ctx.time().removeTimeoutObject(obj);

        final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing rebalance event for future: " + exchFut);

            final GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

            exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    IgniteInternalFuture<Boolean> fut0 = ctx.exchange().forceRebalance(exchFut.exchangeId());

                    fut0.listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                        @Override public void apply(IgniteInternalFuture<Boolean> future) {
                            try {
                                fut.onDone(future.get());
                            }
                            catch (Exception e) {
                                fut.onDone(e);
                            }
                        }
                    });
                }
            });

            return fut;
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring force rebalance request (no topology event happened yet).");

        return new GridFinishedFuture<>(true);
    }

    /**
     * @param fut Future.
     * @return {@code True} if rebalance topology version changed by exchange thread or force
     * reassing exchange occurs, see {@link RebalanceReassignExchangeTask} for details.
     */
    private boolean topologyChanged(RebalanceFuture fut) {
        return !ctx.exchange().rebalanceTopologyVersion().equals(fut.topVer) ||
            fut != rebalanceFut; // Same topology, but dummy exchange forced because of missing partitions.
    }

    /**
     * Sets last exchange future.
     *
     * @param lastFut Last future to set.
     */
    void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {
        lastExchangeFut = lastFut;
    }

    /**
     * @return Collection of supplier nodes. Value {@code empty} means rebalance already finished.
     */
    Collection<UUID> remainingNodes() {
        return rebalanceFut.remainingNodes();
    }

    /**
     * This method initiates new rebalance process from given {@code assignments} by creating new rebalance
     * future based on them. Cancels previous rebalance future and sends rebalance started event.
     * In case of delayed rebalance method schedules the new one with configured delay based on {@code lastExchangeFut}.
     *
     * @param assignments Assignments to process.
     * @param force {@code True} if preload request by {@link ForceRebalanceExchangeTask}.
     * @param rebalanceId Rebalance id generated from exchange thread.
     * @param next Runnable responsible for cache rebalancing chain.
     * @param forcedRebFut External future for forced rebalance.
     * @return Rebalancing runnable.
     */
    Runnable addAssignments(
        final GridDhtPreloaderAssignments assignments,
        boolean force,
        long rebalanceId,
        final Runnable next,
        @Nullable final GridCompoundFuture<Boolean, Boolean> forcedRebFut
    ) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assignments);

        assert force == (forcedRebFut != null);

        long delay = grp.config().getRebalanceDelay();

        if ((delay == 0 || force) && assignments != null) {
            final RebalanceFuture oldFut = rebalanceFut;

            final RebalanceFuture fut = new RebalanceFuture(grp, assignments, log, rebalanceId);

            if (!grp.localWalEnabled())
                fut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                    @Override public void applyx(IgniteInternalFuture<Boolean> future) throws IgniteCheckedException {
                        if (future.get())
                            ctx.walState().onGroupRebalanceFinished(grp.groupId(), assignments.topologyVersion());
                    }
                });

            if (!oldFut.isInitial())
                oldFut.cancel();
            else
                fut.listen(f -> oldFut.onDone(f.result()));

            if (forcedRebFut != null)
                forcedRebFut.add(fut);

            rebalanceFut = fut;

            for (final GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.clearRebalanceCounters();

                    for (GridDhtPartitionDemandMessage msg : assignments.values()) {
                        for (Integer partId : msg.partitions().fullSet())
                            metrics.onRebalancingKeysCountEstimateReceived(grp.topology().globalPartSizes().get(partId));

                        CachePartitionPartialCountersMap histMap = msg.partitions().historicalMap();

                        for (int i = 0; i < histMap.size(); i++) {
                            long from = histMap.initialUpdateCounterAt(i);
                            long to = histMap.updateCounterAt(i);

                            metrics.onRebalancingKeysCountEstimateReceived(to - from);
                        }
                    }

                    metrics.startRebalance(0);
                }
            }

            fut.sendRebalanceStartedEvent();

            if (assignments.cancelled()) { // Pending exchange.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to cancelled assignments.");

                fut.onDone(false);

                fut.sendRebalanceFinishedEvent();

                return null;
            }

            if (assignments.isEmpty()) { // Nothing to rebalance.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to empty assignments.");

                fut.onDone(true);

                ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();

                fut.sendRebalanceFinishedEvent();

                return null;
            }

            return () -> {
                fut.listen(f -> {
                    fut.stat.endTime = currentTimeMillis();

                    lastStatFutures.add(fut);

                    try {
                        if (f.get()) { //Rebalance success finish
                            printRebalanceStatistics(false);

                            if (nonNull(next)) //Has next rebalance
                                next.run();
                            else
                                printRebalanceStatistics(true);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug(e.getMessage());
                    }
                });

                requestPartitions(fut, assignments);
            };
        }
        else if (delay > 0) {
            for (GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.startRebalance(delay);
                }
            }

            GridTimeoutObject obj = lastTimeoutObj.get();

            if (obj != null)
                ctx.time().removeTimeoutObject(obj);

            final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

            assert exchFut != null : "Delaying rebalance process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> f) {
                            ctx.exchange().forceRebalance(exchFut.exchangeId());
                        }
                    });
                }
            };

            lastTimeoutObj.set(obj);

            ctx.time().addTimeoutObject(obj);
        }

        return null;
    }

    /**
     * Asynchronously sends initial demand messages formed from {@code assignments} and initiates supply-demand processes.
     *
     * For each node participating in rebalance process method distributes set of partitions for that node to several stripes (topics).
     * It means that each stripe containing a subset of partitions can be processed in parallel.
     * The number of stripes are controlled by {@link IgniteConfiguration#getRebalanceThreadPoolSize()} property.
     *
     * Partitions that can be rebalanced using only WAL are called historical, others are called full.
     *
     * Before sending messages, method awaits partitions clearing for full partitions.
     *
     * @param fut Rebalance future.
     * @param assignments Assignments.
     */
    private void requestPartitions(final RebalanceFuture fut, GridDhtPreloaderAssignments assignments) {
        assert fut != null;

        if (topologyChanged(fut)) {
            fut.cancel();

            return;
        }

        if (!ctx.kernalContext().grid().isRebalanceEnabled()) {
            if (log.isTraceEnabled())
                log.trace("Cancel partition demand because rebalance disabled on current node.");

            fut.cancel();

            return;
        }

        final CacheConfiguration cfg = grp.config();

        int locStripes = ctx.gridConfig().getRebalanceThreadPoolSize();

        for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assignments.entrySet()) {
            final ClusterNode node = e.getKey();

            GridDhtPartitionDemandMessage d = e.getValue();

            int rmtStripes = Optional.ofNullable((Integer) node.attribute(IgniteNodeAttributes.ATTR_REBALANCE_POOL_SIZE))
                .orElse(1);

            int rmtTotalStripes = rmtStripes <= locStripes ? rmtStripes : locStripes;

            int stripes = rmtTotalStripes;

            final IgniteDhtDemandedPartitionsMap parts;
            synchronized (fut) { // Synchronized to prevent consistency issues in case of parallel cancellation.
                if (fut.isDone())
                    break;

                parts = fut.remaining.get(node.id());

                U.log(log, "Prepared rebalancing [grp=" + grp.cacheOrGroupName()
                    + ", mode=" + cfg.getRebalanceMode() + ", supplier=" + node.id() + ", partitionsCount=" + parts.size()
                    + ", topVer=" + fut.topologyVersion() + ", localParallelism=" + locStripes
                    + ", rmtParallelism=" + rmtStripes + ", parallelism=" + rmtTotalStripes + "]");
            }

            final List<IgniteDhtDemandedPartitionsMap> stripePartitions = new ArrayList<>(stripes);
            for (int i = 0; i < stripes; i++)
                stripePartitions.add(new IgniteDhtDemandedPartitionsMap());

            // Reserve one stripe for historical partitions.
            if (parts.hasHistorical()) {
                stripePartitions.set(stripes - 1, new IgniteDhtDemandedPartitionsMap(parts.historicalMap(), null));

                if (stripes > 1)
                    stripes--;
            }

            // Distribute full partitions across other stripes.
            Iterator<Integer> it = parts.fullSet().iterator();
            for (int i = 0; it.hasNext(); i++)
                stripePartitions.get(i % stripes).addFull(it.next());

            for (int stripe = 0; stripe < rmtTotalStripes; stripe++) {
                if (!stripePartitions.get(stripe).isEmpty()) {
                    // Create copy of demand message with new striped partitions map.
                    final GridDhtPartitionDemandMessage demandMsg = d.withNewPartitionsMap(stripePartitions.get(stripe));

                    demandMsg.topic(rebalanceTopics.get(stripe));
                    demandMsg.rebalanceId(fut.rebalanceId);
                    demandMsg.timeout(grp.preloader().timeout());

                    final int topicId = stripe;

                    IgniteInternalFuture<?> clearAllFuture = clearFullPartitions(fut, demandMsg.partitions().fullSet());

                    // Start rebalancing after clearing full partitions is finished.
                    clearAllFuture.listen(f -> ctx.kernalContext().closure().runLocalSafe(() -> {
                        if (fut.isDone())
                            return;

                        try {
                            fut.stat.msgStats
                                .computeIfAbsent(topicId, integer -> new ConcurrentHashMap<>())
                                .put(node, new RebalanceMessageStatistics(currentTimeMillis()));

                            ctx.io().sendOrderedMessage(node, rebalanceTopics.get(topicId),
                                demandMsg.convertIfNeeded(node.version()), grp.ioPolicy(), demandMsg.timeout());

                            // Cleanup required in case partitions demanded in parallel with cancellation.
                            synchronized (fut) {
                                if (fut.isDone())
                                    fut.cleanupRemoteContexts(node.id());
                            }

                            if (log.isInfoEnabled())
                                log.info("Started rebalance routine [" + grp.cacheOrGroupName() +
                                    ", topVer=" + fut.topologyVersion() +
                                    ", supplier=" + node.id() + ", topic=" + topicId +
                                    ", fullPartitions=" + S.compact(stripePartitions.get(topicId).fullSet()) +
                                    ", histPartitions=" + S.compact(stripePartitions.get(topicId).historicalSet()) + "]");
                        }
                        catch (IgniteCheckedException e1) {
                            ClusterTopologyCheckedException cause = e1.getCause(ClusterTopologyCheckedException.class);

                            if (cause != null)
                                log.warning("Failed to send initial demand request to node. " + e1.getMessage());
                            else
                                log.error("Failed to send initial demand request to node.", e1);

                            fut.cancel();
                        }
                        catch (Throwable th) {
                            log.error("Runtime error caught during initial demand request sending.", th);

                            fut.cancel();
                        }
                    }, true));
                }
            }
        }
    }

    /**
     * Creates future which will be completed when all {@code fullPartitions} are cleared.
     *
     * @param fut Rebalance future.
     * @param fullPartitions Set of full partitions need to be cleared.
     * @return Future which will be completed when given partitions are cleared.
     */
    private IgniteInternalFuture<?> clearFullPartitions(RebalanceFuture fut, Set<Integer> fullPartitions) {
        final GridFutureAdapter clearAllFuture = new GridFutureAdapter();

        if (fullPartitions.isEmpty()) {
            clearAllFuture.onDone();

            return clearAllFuture;
        }

        for (GridCacheContext cctx : grp.caches()) {
            if (cctx.statisticsEnabled()) {
                final CacheMetricsImpl metrics = cctx.cache().metrics0();

                metrics.rebalanceClearingPartitions(fullPartitions.size());
            }
        }

        final AtomicInteger clearingPartitions = new AtomicInteger(fullPartitions.size());

        for (int partId : fullPartitions) {
            if (fut.isDone()) {
                clearAllFuture.onDone();

                return clearAllFuture;
            }

            GridDhtLocalPartition part = grp.topology().localPartition(partId);

            if (part != null && part.state() == MOVING) {
                part.onClearFinished(f -> {
                    if (!fut.isDone()) {
                        // Cancel rebalance if partition clearing was failed.
                        if (f.error() != null) {
                            for (GridCacheContext cctx : grp.caches()) {
                                if (cctx.statisticsEnabled()) {
                                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                                    metrics.rebalanceClearingPartitions(0);
                                }
                            }

                            log.error("Unable to await partition clearing " + part, f.error());

                            fut.cancel();

                            clearAllFuture.onDone(f.error());
                        }
                        else {
                            int remaining = clearingPartitions.decrementAndGet();

                            for (GridCacheContext cctx : grp.caches()) {
                                if (cctx.statisticsEnabled()) {
                                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                                    metrics.rebalanceClearingPartitions(remaining);
                                }
                            }

                            if (log.isDebugEnabled())
                                log.debug("Partition is ready for rebalance [grp=" + grp.cacheOrGroupName()
                                    + ", p=" + part.id() + ", remaining=" + remaining + "]");

                            if (remaining == 0)
                                clearAllFuture.onDone();
                        }
                    }
                    else
                        clearAllFuture.onDone();
                });
            }
            else {
                int remaining = clearingPartitions.decrementAndGet();

                for (GridCacheContext cctx : grp.caches()) {
                    if (cctx.statisticsEnabled()) {
                        final CacheMetricsImpl metrics = cctx.cache().metrics0();

                        metrics.rebalanceClearingPartitions(remaining);
                    }
                }

                if (remaining == 0)
                    clearAllFuture.onDone();
            }
        }

        return clearAllFuture;
    }

    /**
     * Handles supply message from {@code nodeId} with specified {@code topicId}.
     *
     * Supply message contains entries to populate rebalancing partitions.
     *
     * There is a cyclic process:
     * Populate rebalancing partitions with entries from Supply message.
     * If not all partitions specified in {@link #rebalanceFut} were rebalanced or marked as missed
     * send new Demand message to request next batch of entries.
     *
     * @param topicId Topic id.
     * @param nodeId Node id.
     * @param supplyMsg Supply message.
     */
    public void handleSupplyMessage(
        int topicId,
        final UUID nodeId,
        final GridDhtPartitionSupplyMessage supplyMsg
    ) {
        AffinityTopologyVersion topVer = supplyMsg.topologyVersion();

        final RebalanceFuture fut = rebalanceFut;

        try {
            fut.cancelLock.readLock().lock();

            ClusterNode node = ctx.node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Supply message ignored (supplier has left cluster) [" + demandRoutineInfo(topicId, nodeId, supplyMsg) + "]");

                return;
            }

            // Topology already changed (for the future that supply message based on).
            if (topologyChanged(fut) || !fut.isActual(supplyMsg.rebalanceId())) {
                if (log.isDebugEnabled())
                    log.debug("Supply message ignored (topology changed) [" + demandRoutineInfo(topicId, nodeId, supplyMsg) + "]");

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Received supply message [" + demandRoutineInfo(topicId, nodeId, supplyMsg) + "]");

            // Check whether there were error during supply message unmarshalling process.
            if (supplyMsg.classError() != null) {
                U.warn(log, "Rebalancing from node cancelled [" + demandRoutineInfo(topicId, nodeId, supplyMsg) + "]" +
                    ". Supply message couldn't be unmarshalled: " + supplyMsg.classError());

                fut.cancel(nodeId);

                return;
            }

            // Check whether there were error during supplying process.
            if (supplyMsg.error() != null) {
                U.warn(log, "Rebalancing from node cancelled [" + demandRoutineInfo(topicId, nodeId, supplyMsg) + "]" +
                    "]. Supplier has failed with error: " + supplyMsg.error());

                fut.cancel(nodeId);

                return;
            }

            final GridDhtPartitionTopology top = grp.topology();

            if (grp.sharedGroup()) {
                for (GridCacheContext cctx : grp.caches()) {
                    if (cctx.statisticsEnabled()) {
                        long keysCnt = supplyMsg.keysForCache(cctx.cacheId());

                        if (keysCnt != -1)
                            cctx.cache().metrics0().onRebalancingKeysCountEstimateReceived(keysCnt);

                        // Can not be calculated per cache.
                        cctx.cache().metrics0().onRebalanceBatchReceived(supplyMsg.messageSize());
                    }
                }
            }
            else {
                GridCacheContext cctx = grp.singleCacheContext();

                if (cctx.statisticsEnabled()) {
                    if (supplyMsg.estimatedKeysCount() != -1)
                        cctx.cache().metrics0().onRebalancingKeysCountEstimateReceived(supplyMsg.estimatedKeysCount());

                    cctx.cache().metrics0().onRebalanceBatchReceived(supplyMsg.messageSize());
                }
            }

            try {
                if (isPrintRebalanceStatistics()) {
                    List<PartitionStatistics> partStats = supplyMsg.infos().entrySet().stream()
                        .map(entry -> new PartitionStatistics(entry.getKey(), entry.getValue().infos().size()))
                        .collect(toList());

                    fut.stat.msgStats.get(topicId).get(ctx.node(nodeId)).receivePartStats
                        .add(new ReceivePartitionStatistics(currentTimeMillis(), supplyMsg.messageSize(), partStats));
                }

                AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

                // Preload.
                for (Map.Entry<Integer, CacheEntryInfoCollection> e : supplyMsg.infos().entrySet()) {
                    int p = e.getKey();

                    if (aff.get(p).contains(ctx.localNode())) {
                        GridDhtLocalPartition part;

                        try {
                            part = top.localPartition(p, topVer, true);
                        }
                        catch (GridDhtInvalidPartitionException err) {
                            assert !topVer.equals(top.lastTopologyChangeVersion());

                            if (log.isDebugEnabled()) {
                                log.debug("Failed to get partition for rebalancing [" +
                                    "grp=" + grp.cacheOrGroupName() +
                                    ", err=" + err +
                                    ", p=" + p +
                                    ", topVer=" + topVer +
                                    ", lastTopVer=" + top.lastTopologyChangeVersion() + ']');
                            }

                            continue;
                        }

                        assert part != null;

                        boolean last = supplyMsg.last().containsKey(p);

                        if (part.state() == MOVING) {
                            boolean reserved = part.reserve();

                            assert reserved : "Failed to reserve partition [igniteInstanceName=" +
                                ctx.igniteInstanceName() + ", grp=" + grp.cacheOrGroupName() + ", part=" + part + ']';

                            part.lock();

                            part.beforeApplyBatch(last);

                            try {
                                Iterator<GridCacheEntryInfo> infos = e.getValue().infos().iterator();

                                if (grp.mvccEnabled())
                                    mvccPreloadEntries(topVer, node, p, infos);
                                else
                                    preloadEntries(topVer, node, p, infos);

                                // If message was last for this partition,
                                // then we take ownership.
                                if (last) {
                                    if (ctx.kernalContext().txDr().shouldApplyUpdateCounterOnRebalance())
                                        grp.offheap().onPartitionInitialCounterUpdated(p, supplyMsg.last().get(p) - 1, 1);

                                    fut.partitionDone(nodeId, p, true);

                                    if (log.isDebugEnabled())
                                        log.debug("Finished rebalancing partition: " +
                                            "[" + demandRoutineInfo(topicId, nodeId, supplyMsg) + ", p=" + p + "]");
                                }
                            }
                            finally {
                                part.unlock();
                                part.release();
                            }
                        }
                        else {
                            if (last)
                                fut.partitionDone(nodeId, p, false);

                            if (log.isDebugEnabled())
                                log.debug("Skipping rebalancing partition (state is not MOVING): " +
                                    "[" + demandRoutineInfo(topicId, nodeId, supplyMsg) + ", p=" + p + "]");
                        }
                    }
                    else {
                        fut.partitionDone(nodeId, p, false);

                        if (log.isDebugEnabled())
                            log.debug("Skipping rebalancing partition (affinity changed): " +
                                "[" + demandRoutineInfo(topicId, nodeId, supplyMsg) + ", p=" + p + "]");
                    }
                }

                // Only request partitions based on latest topology version.
                for (Integer miss : supplyMsg.missed()) {
                    if (aff.get(miss).contains(ctx.localNode()))
                        fut.partitionMissed(nodeId, miss);
                }

                for (Integer miss : supplyMsg.missed())
                    fut.partitionDone(nodeId, miss, false);

                GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                    supplyMsg.rebalanceId(),
                    supplyMsg.topologyVersion(),
                    grp.groupId());

                d.timeout(grp.preloader().timeout());

                d.topic(rebalanceTopics.get(topicId));

                if (!topologyChanged(fut) && !fut.isDone()) {
                    // Send demand message.
                    try {
                        ctx.io().sendOrderedMessage(node, rebalanceTopics.get(topicId),
                            d.convertIfNeeded(node.version()), grp.ioPolicy(), grp.preloader().timeout());

                        if (log.isDebugEnabled())
                            log.debug("Send next demand message [" + demandRoutineInfo(topicId, nodeId, supplyMsg) + "]");
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Supplier has left [" + demandRoutineInfo(topicId, nodeId, supplyMsg) +
                                ", errMsg=" + e.getMessage() + ']');
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Will not request next demand message [" + demandRoutineInfo(topicId, nodeId, supplyMsg) +
                            ", topChanged=" + topologyChanged(fut) + ", rebalanceFuture=" + fut + "]");
                }
            }
            catch (IgniteSpiException | IgniteCheckedException e) {
                LT.error(log, e, "Error during rebalancing [" + demandRoutineInfo(topicId, nodeId, supplyMsg) +
                    ", err=" + e + ']');
            }
        }
        finally {
            fut.cancelLock.readLock().unlock();
        }
    }

    /**
     * Adds mvcc entries with theirs history to partition p.
     *
     * @param node Node which sent entry.
     * @param p Partition id.
     * @param infos Entries info for preload.
     * @param topVer Topology version.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void mvccPreloadEntries(AffinityTopologyVersion topVer, ClusterNode node, int p,
        Iterator<GridCacheEntryInfo> infos) throws IgniteCheckedException {
        if (!infos.hasNext())
            return;

        List<GridCacheMvccEntryInfo> entryHist = new ArrayList<>();

        GridCacheContext cctx = grp.sharedGroup() ? null : grp.singleCacheContext();

        // Loop through all received entries and try to preload them.
        while (infos.hasNext() || !entryHist.isEmpty()) {
            ctx.database().checkpointReadLock();

            try {
                for (int i = 0; i < 100; i++) {
                    boolean hasMore = infos.hasNext();

                    assert hasMore || !entryHist.isEmpty();

                    GridCacheMvccEntryInfo entry = null;

                    boolean flushHistory;

                    if (hasMore) {
                        entry = (GridCacheMvccEntryInfo)infos.next();

                        GridCacheMvccEntryInfo prev = entryHist.isEmpty() ? null : entryHist.get(0);

                        flushHistory = prev != null && ((grp.sharedGroup() && prev.cacheId() != entry.cacheId())
                            || !prev.key().equals(entry.key()));
                    }
                    else
                        flushHistory = true;

                    if (flushHistory) {
                        assert !entryHist.isEmpty();

                        int cacheId = entryHist.get(0).cacheId();

                        if (grp.sharedGroup() && (cctx == null || cacheId != cctx.cacheId())) {
                            assert cacheId != CU.UNDEFINED_CACHE_ID;

                            cctx = grp.shared().cacheContext(cacheId);
                        }

                        if (cctx != null) {
                            if (!mvccPreloadEntry(cctx, node, entryHist, topVer, p)) {
                                if (log.isTraceEnabled())
                                    log.trace("Got entries for invalid partition during " +
                                        "preloading (will skip) [p=" + p +
                                        ", entry=" + entryHist.get(entryHist.size() - 1) + ']');

                                return; // Skip current partition.
                            }

                            //TODO: IGNITE-11330: Update metrics for touched cache only.
                            for (GridCacheContext ctx : grp.caches()) {
                                if (ctx.statisticsEnabled())
                                    ctx.cache().metrics0().onRebalanceKeyReceived();
                            }
                        }

                        if (!hasMore)
                            return;

                        entryHist.clear();
                    }

                    entryHist.add(entry);
                }
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * Adds entries with theirs history to partition p.
     *
     * @param node Node which sent entry.
     * @param p Partition id.
     * @param infos Entries info for preload.
     * @param topVer Topology version.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void preloadEntries(AffinityTopologyVersion topVer, ClusterNode node, int p,
        Iterator<GridCacheEntryInfo> infos) throws IgniteCheckedException {
        GridCacheContext cctx = null;

        // Loop through all received entries and try to preload them.
        while (infos.hasNext()) {
            ctx.database().checkpointReadLock();

            try {
                for (int i = 0; i < 100; i++) {
                    if (!infos.hasNext())
                        break;

                    GridCacheEntryInfo entry = infos.next();

                    if (cctx == null || (grp.sharedGroup() && entry.cacheId() != cctx.cacheId())) {
                        cctx = grp.sharedGroup() ? grp.shared().cacheContext(entry.cacheId()) : grp.singleCacheContext();

                        if (cctx == null)
                            continue;
                        else if (cctx.isNear())
                            cctx = cctx.dhtCache().context();
                    }

                    if (!preloadEntry(node, p, entry, topVer, cctx)) {
                        if (log.isTraceEnabled())
                            log.trace("Got entries for invalid partition during " +
                                "preloading (will skip) [p=" + p + ", entry=" + entry + ']');

                        return;
                    }

                    //TODO: IGNITE-11330: Update metrics for touched cache only.
                    for (GridCacheContext ctx : grp.caches()) {
                        if (ctx.statisticsEnabled())
                            ctx.cache().metrics0().onRebalanceKeyReceived();
                    }
                }
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * Adds {@code entry} to partition {@code p}.
     *
     * @param from Node which sent entry.
     * @param p Partition id.
     * @param entry Preloaded entry.
     * @param topVer Topology version.
     * @param cctx Cache context.
     * @return {@code False} if partition has become invalid during preloading.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private boolean preloadEntry(
        ClusterNode from,
        int p,
        GridCacheEntryInfo entry,
        AffinityTopologyVersion topVer,
        GridCacheContext cctx
    ) throws IgniteCheckedException {
        assert ctx.database().checkpointLockIsHeldByThread();

        try {
            GridCacheEntryEx cached = null;

            try {
                cached = cctx.cache().entryEx(entry.key(), topVer);

                if (log.isTraceEnabled()) {
                    log.trace("Rebalancing key [key=" + entry.key() + ", part=" + p + ", fromNode=" +
                        from.id() + ", grpId=" + grp.groupId() + ']');
                }

                if (preloadPred == null || preloadPred.apply(entry)) {
                    if (cached.initialValue(
                        entry.value(),
                        entry.version(),
                        cctx.mvccEnabled() ? ((MvccVersionAware)entry).mvccVersion() : null,
                        cctx.mvccEnabled() ? ((MvccUpdateVersionAware)entry).newMvccVersion() : null,
                        cctx.mvccEnabled() ? ((MvccVersionAware)entry).mvccTxState() : TxState.NA,
                        cctx.mvccEnabled() ? ((MvccUpdateVersionAware)entry).newMvccTxState() : TxState.NA,
                        entry.ttl(),
                        entry.expireTime(),
                        true,
                        topVer,
                        cctx.isDrEnabled() ? DR_PRELOAD : DR_NONE,
                        false
                    )) {
                        cached.touch(); // Start tracking.

                        if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                            cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(), null,
                                null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, entry.value(), true, null,
                                false, null, null, null, true);
                    }
                    else {
                        cached.touch(); // Start tracking.

                        if (log.isTraceEnabled())
                            log.trace("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                                ", part=" + p + ']');
                    }
                }
                else if (log.isTraceEnabled())
                    log.trace("Rebalance predicate evaluated to false for entry (will ignore): " + entry);
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isTraceEnabled())
                    log.trace("Entry has been concurrently removed while rebalancing (will ignore) [key=" +
                        cached.key() + ", part=" + p + ']');
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition became invalid during rebalancing (will ignore): " + p);

                return false;
            }
        }
        catch (IgniteInterruptedCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [local=" +
                ctx.localNode() + ", node=" + from.id() + ", key=" + entry.key() + ", part=" + p + ']', e);
        }

        return true;
    }

    /**
     * Adds mvcc {@code entry} with it's history to partition {@code p}.
     *
     * @param cctx Cache context.
     * @param from Node which sent entry.
     * @param history Mvcc entry history.
     * @param topVer Topology version.
     * @param p Partition id.
     * @return {@code False} if partition has become invalid during preloading.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private boolean mvccPreloadEntry(
        GridCacheContext cctx,
        ClusterNode from,
        List<GridCacheMvccEntryInfo> history,
        AffinityTopologyVersion topVer,
        int p
    ) throws IgniteCheckedException {
        assert ctx.database().checkpointLockIsHeldByThread();
        assert !history.isEmpty();

        GridCacheMvccEntryInfo info = history.get(0);

        assert info.key() != null;

        try {
            GridCacheEntryEx cached = null;

            try {
                cached = cctx.cache().entryEx(info.key(), topVer);

                if (log.isTraceEnabled())
                    log.trace("Rebalancing key [key=" + info.key() + ", part=" + p + ", node=" + from.id() + ']');

                if (cached.mvccPreloadEntry(history)) {
                    cached.touch(); // Start tracking.

                    if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                        cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(), null,
                            null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, null, true, null,
                            false, null, null, null, true);
                }
                else {
                    cached.touch(); // Start tracking.

                    if (log.isTraceEnabled())
                        log.trace("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                            ", part=" + p + ']');
                }
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isTraceEnabled())
                    log.trace("Entry has been concurrently removed while rebalancing (will ignore) [key=" +
                        cached.key() + ", part=" + p + ']');
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition became invalid during rebalancing (will ignore): " + p);

                return false;
            }
        }
        catch (IgniteInterruptedCheckedException | ClusterTopologyCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [local=" +
                ctx.localNode() + ", node=" + from.id() + ", key=" + info.key() + ", part=" + p + ']', e);
        }

        return true;
    }

    /**
     * String representation of demand routine.
     *
     * @param topicId Topic id.
     * @param supplier Supplier.
     * @param supplyMsg Supply message.
     */
    private String demandRoutineInfo(int topicId, UUID supplier, GridDhtPartitionSupplyMessage supplyMsg) {
        return "grp=" + grp.cacheOrGroupName() + ", topVer=" + supplyMsg.topologyVersion() + ", supplier=" + supplier + ", topic=" + topicId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemander.class, this);
    }

    /**
     *
     */
    public static class RebalanceFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final GridCacheSharedContext<?, ?> ctx;

        /** */
        private final CacheGroupContext grp;

        /** */
        private final IgniteLogger log;

        /** Remaining. */
        private final Map<UUID, IgniteDhtDemandedPartitionsMap> remaining = new HashMap<>();

        /** Missed. */
        private final Map<UUID, Collection<Integer>> missed = new HashMap<>();

        /** Exchange ID. */
        @GridToStringExclude
        private final GridDhtPartitionExchangeId exchId;

        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Unique (per demander) rebalance id. */
        private final long rebalanceId;

        /** The number of rebalance routines. */
        private final long routines;

        /** Used to order rebalance cancellation and supply message processing, they should not overlap.
         * Otherwise partition clearing could start on still rebalancing partition resulting in eviction of
         * partition in OWNING state. */
        private final ReentrantReadWriteLock cancelLock;

        /** Rebalance statistics */
        @GridToStringExclude
        private final RebalanceFutureStatistics stat = new RebalanceFutureStatistics();

        /**
         * @param grp Cache group.
         * @param assignments Assignments.
         * @param log Logger.
         * @param rebalanceId Rebalance id.
         */
        RebalanceFuture(
            CacheGroupContext grp,
            GridDhtPreloaderAssignments assignments,
            IgniteLogger log,
            long rebalanceId) {
            assert assignments != null;

            exchId = assignments.exchangeId();
            topVer = assignments.topologyVersion();

            assignments.forEach((k, v) -> {
                assert v.partitions() != null :
                    "Partitions are null [grp=" + grp.cacheOrGroupName() + ", fromNode=" + k.id() + "]";

                remaining.put(k.id(), v.partitions());
            });

            this.routines = remaining.size();

            this.grp = grp;
            this.log = log;
            this.rebalanceId = rebalanceId;

            ctx = grp.shared();

            cancelLock = new ReentrantReadWriteLock();
        }

        /**
         * Dummy future. Will be done by real one.
         */
        RebalanceFuture() {
            this.exchId = null;
            this.topVer = null;
            this.ctx = null;
            this.grp = null;
            this.log = null;
            this.rebalanceId = -1;
            this.routines = 0;
            this.cancelLock = new ReentrantReadWriteLock();
        }

        /**
         * @return Topology version.
         */
        public AffinityTopologyVersion topologyVersion() {
            return topVer;
        }

        /**
         * @param rebalanceId Rebalance id.
         * @return true in case future created for specified {@code rebalanceId}, false in other case.
         */
        private boolean isActual(long rebalanceId) {
            return this.rebalanceId == rebalanceId;
        }

        /**
         * @return Is initial (created at demander creation).
         */
        private boolean isInitial() {
            return topVer == null;
        }

        /**
         * Cancels this future.
         *
         * @return {@code True}.
         */
        @Override public boolean cancel() {
            try {
                // Cancel lock is needed only for case when some message might be on the fly while rebalancing is
                // cancelled.
                cancelLock.writeLock().lock();

                synchronized (this) {
                    if (isDone())
                        return true;

                    U.log(log, "Cancelled rebalancing from all nodes [grp=" + grp.cacheOrGroupName() +
                        ", topVer=" + topologyVersion() + "]");

                    if (!ctx.kernalContext().isStopping()) {
                        for (UUID nodeId : remaining.keySet())
                            cleanupRemoteContexts(nodeId);
                    }

                    remaining.clear();

                    checkIsDone(true /* cancelled */);
                }

                return true;
            }
            finally {
                cancelLock.writeLock().unlock();
            }
        }

        /**
         * @param nodeId Node id.
         */
        private void cancel(UUID nodeId) {
            synchronized (this) {
                if (isDone())
                    return;

                U.log(log, ("Cancelled rebalancing [grp=" + grp.cacheOrGroupName() +
                    ", supplier=" + nodeId + ", topVer=" + topologyVersion() + ']'));

                cleanupRemoteContexts(nodeId);

                remaining.remove(nodeId);

                onDone(false); // Finishing rebalance future as non completed.

                checkIsDone(); // But will finish syncFuture only when other nodes are preloaded or rebalancing cancelled.
            }
        }

        /**
         * @param nodeId Node id.
         * @param p Partition id.
         */
        private void partitionMissed(UUID nodeId, int p) {
            synchronized (this) {
                if (isDone())
                    return;

                missed.computeIfAbsent(nodeId, k -> new HashSet<>());

                missed.get(nodeId).add(p);
            }
        }

        /**
         * @param nodeId Node id.
         */
        private void cleanupRemoteContexts(UUID nodeId) {
            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null)
                return;

            GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                // Negative number of id signals that supply context
                // with the same positive id must be cleaned up at the supply node.
                -rebalanceId,
                this.topologyVersion(),
                grp.groupId());

            d.timeout(grp.preloader().timeout());

            try {
                for (int idx = 0; idx < ctx.gridConfig().getRebalanceThreadPoolSize(); idx++) {
                    d.topic(GridCachePartitionExchangeManager.rebalanceTopic(idx));

                    ctx.io().sendOrderedMessage(node, GridCachePartitionExchangeManager.rebalanceTopic(idx),
                        d.convertIfNeeded(node.version()), grp.ioPolicy(), grp.preloader().timeout());
                }
            }
            catch (IgniteCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send failover context cleanup request to node " + nodeId);
            }
        }

        /**
         * @param nodeId Node id.
         * @param p Partition number.
         */
        private void partitionDone(UUID nodeId, int p, boolean updateState) {
            synchronized (this) {
                if (updateState && grp.localWalEnabled())
                    grp.topology().own(grp.topology().localPartition(p));

                if (isDone())
                    return;

                if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                    rebalanceEvent(p, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());

                IgniteDhtDemandedPartitionsMap parts = remaining.get(nodeId);

                assert parts != null : "Remaining not found [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                    ", part=" + p + "]";

                boolean rmvd = parts.remove(p);

                assert rmvd : "Partition already done [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                    ", part=" + p + ", left=" + parts + "]";

                if (parts.isEmpty()) {
                    int remainingRoutines = remaining.size() - 1;

                    U.log(log, "Completed " + ((remainingRoutines == 0 ? "(final) " : "") +
                            "rebalancing [grp=" + grp.cacheOrGroupName() +
                            ", supplier=" + nodeId +
                            ", topVer=" + topologyVersion() +
                            ", progress=" + (routines - remainingRoutines) + "/" + routines + "]"));

                    remaining.remove(nodeId);
                }

                checkIsDone();
            }
        }

        /**
         * @param part Partition.
         * @param type Type.
         * @param discoEvt Discovery event.
         */
        private void rebalanceEvent(int part, int type, DiscoveryEvent discoEvt) {
            assert discoEvt != null;

            grp.addRebalanceEvent(part, type, discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
        }

        /**
         * @param type Type.
         * @param discoEvt Discovery event.
         */
        private void rebalanceEvent(int type, DiscoveryEvent discoEvt) {
            rebalanceEvent(-1, type, discoEvt);
        }

        /**
         *
         */
        private void checkIsDone() {
            checkIsDone(false);
        }

        /**
         * @param cancelled Is cancelled.
         */
        private void checkIsDone(boolean cancelled) {
            if (remaining.isEmpty()) {
                sendRebalanceFinishedEvent();

                if (log.isInfoEnabled())
                    log.info("Completed rebalance future: " + this);

                if (log.isDebugEnabled())
                    log.debug("Partitions have been scheduled to resend [reason=" +
                        "Rebalance is done [grp=" + grp.cacheOrGroupName() + "]");

                ctx.exchange().scheduleResendPartitions();

                Collection<Integer> m = new HashSet<>();

                for (Map.Entry<UUID, Collection<Integer>> e : missed.entrySet()) {
                    if (e.getValue() != null && !e.getValue().isEmpty())
                        m.addAll(e.getValue());
                }

                if (!m.isEmpty()) {
                    U.log(log, ("Reassigning partitions that were missed: " + m));

                    onDone(false); //Finished but has missed partitions, will force dummy exchange

                    ctx.exchange().forceReassign(exchId);

                    return;
                }

                if (!cancelled && !grp.preloader().syncFuture().isDone())
                    ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();

                onDone(!cancelled);
            }
        }

        /**
         * @return Collection of supplier nodes. Value {@code empty} means rebalance already finished.
         */
        private synchronized Collection<UUID> remainingNodes() {
            return remaining.keySet();
        }

        /**
         *
         */
        private void sendRebalanceStartedEvent() {
            if (grp.eventRecordable(EVT_CACHE_REBALANCE_STARTED))
                rebalanceEvent(EVT_CACHE_REBALANCE_STARTED, exchId.discoveryEvent());
        }

        /**
         *
         */
        private void sendRebalanceFinishedEvent() {
            if (grp.eventRecordable(EVT_CACHE_REBALANCE_STOPPED))
                rebalanceEvent(EVT_CACHE_REBALANCE_STOPPED, exchId.discoveryEvent());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceFuture.class, this);
        }
    }

    /** {@link RebalanceFuture} statistics */
    private static class RebalanceFutureStatistics {
        /** Start rebalance time in mills */
        private final long startTime = currentTimeMillis();

        /** End rebalance time in mills */
        private volatile long endTime = startTime;

        /** First key - topic id. Second key - supplier node. */
        private final Map<Integer, Map<ClusterNode, RebalanceMessageStatistics>> msgStats = new ConcurrentHashMap<>();
    }

    /** Rebalance messages statistics */
    private static class RebalanceMessageStatistics {
        /** Time send message in mills */
        private final long sndMsgTime;

        /** Statistics by received partitions */
        private final List<ReceivePartitionStatistics> receivePartStats = new CopyOnWriteArrayList<>();

        /**
         * @param sndMsgTime time send demand message
         * */
        public RebalanceMessageStatistics(final long sndMsgTime) {
            this.sndMsgTime = sndMsgTime;
        }
    }

    /** Receive partition statistics */
    private static class ReceivePartitionStatistics {
        /** Time receive message with partition in mills */
        private final long rcvMsgTime;

        /** Size receive message in bytes */
        private final long msgSize;

        /** Received partitions */
        private final List<PartitionStatistics> parts;

        /**
         * @param rcvMsgTime time receive message in mills
         * @param msgSize message size in bytes
         * @param parts received partitions require not null
         * */
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

    /** Received partition info */
    private static class PartitionStatistics {
        /** Partition id */
        private final int id;

        /** Count entries in partitions */
        private final int entryCount;

        /**
         * @param id - partition id
         * @param entryCount Count entries in partitions
         * */
        public PartitionStatistics(final int id, final int entryCount) {
            this.id = id;
            this.entryCount = entryCount;
        }
    }

    /**
     * Is enable print statistics or not by {@code IGNITE_QUIET}, {@code IGNITE_WRITE_REBALANCE_STATISTICS}.
     *
     * @return is enable print statisctics or not
     * @see IgniteSystemProperties#IGNITE_QUIET
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS
     */
    private boolean isPrintRebalanceStatistics() {
        return !getBoolean(IGNITE_QUIET, true) && getBoolean(IGNITE_WRITE_REBALANCE_STATISTICS, false);
    }

    /** Return list demanders per cache groups */
    private List<GridDhtPartitionDemander> demanders(){
        return ctx.cacheContexts().stream()
            .map(GridCacheContext::preloader)
            .filter(GridDhtPreloader.class::isInstance)
            .map(GridDhtPreloader.class::cast)
            .map(GridDhtPreloader::demander)
            .collect(toList());
    }

    /**
     * Print rebalance statistics into log.
     * Statistic will print if {@link #isPrintRebalanceStatistics()}.
     * <p/>
     * If {@code finish} == true then print full statistics
     * (include successful and not rebalances), else print
     * statistics only for current success cache group.
     * <p/>
     * If {@code finish} == true then clears statistics when printed or not.
     * <p/>
     * Partition distribution only for last success rebalance, per cache group.
     *
     * @param finish is finish rebalance
     * */
    private void printRebalanceStatistics(final boolean finish) {
        boolean printStat = isPrintRebalanceStatistics();

        log.info(format("Print rebalance statistics [finish=%s, printStat=%s]", finish, printStat));

        Map<CacheGroupContext, List<RebalanceFuture>> rebFuts = !finish ? singletonMap(grp, singletonList(rebalanceFut))
            : demanders().stream().collect(toMap(demander -> demander.grp, demander -> demander.lastStatFutures));

        try {
            if (!printStat)
                return;

            AtomicInteger nodeCnt = new AtomicInteger();

            Map<ClusterNode, Integer> nodeAliases = toRebalanceFutureStream(rebFuts)
                .flatMap(future -> future.stat.msgStats.entrySet().stream())
                .flatMap(entry -> entry.getValue().keySet().stream())
                .distinct()
                .collect(toMap(identity(), node -> nodeCnt.getAndIncrement()));

            StringJoiner joiner = new StringJoiner(" ");

            if (finish)
                joiner.add(totalRebalanceStatistics(rebFuts, nodeAliases));

            joiner
                .add(cacheGroupsRebalanceStatistics(rebFuts, nodeAliases, finish))
                .add(aliasesRebalanceStatistics("p - partitions, e - entries, b - bytes, d - duration", nodeAliases))
                .add(partitionsDistributionRebalanceStatistics(rebFuts, nodeAliases, nodeCnt));

            log.info(joiner.toString());
        }
        finally {
            if (finish) {
                demanders().forEach(demander -> {
                    demander.rebalanceFut.stat.msgStats.clear();
                    demander.lastStatFutures.clear();
                });

                log.info("Clear statistics");
            }
        }
    }

    /**
     * Total statistics for rebalance.
     * Parameters requires not null.
     *
     * @param rebFuts participating in successful and not rebalances
     * @param nodeAliases for print nodeId=1 instead long string
     * @return total statistics
     */
    private String totalRebalanceStatistics(
        final Map<CacheGroupContext, List<RebalanceFuture>> rebFuts,
        final Map<ClusterNode, Integer> nodeAliases
    ) {
        assert nonNull(rebFuts);
        assert nonNull(nodeAliases);

        long minStartTime = minStartTime(toRebalanceFutureStream(rebFuts));
        long maxEndTime = maxEndTime(toRebalanceFutureStream(rebFuts));

        Map<Integer, List<RebalanceMessageStatistics>> topicStat = toTopicStatistics(toRebalanceFutureStream(rebFuts));
        Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat = toSupplierStatistics(toRebalanceFutureStream(rebFuts));

        return new StringJoiner(" ")
            .add(format("Total information (%s):", SUCCESSFUL_OR_NOT_REBALANCE_TEXT))
            .add(format("Time [%s]", toStartEndDuration(minStartTime, maxEndTime)))
            .add(topicRebalanceStatistics(topicStat))
            .add(supplierRebalanceStatistics(supplierStat, nodeAliases))
            .toString();
    }

    /**
     * Rebalance statistics per cache group.
     * <p/>
     * If {@code finish} == true then
     * add {@link #SUCCESSFUL_OR_NOT_REBALANCE_TEXT}
     * else add {@link #SUCCESSFUL_REBALANCE_TEXT}
     * into header.
     * Parameters requires not null.
     *
     * @param rebFuts participating in successful and not rebalances
     * @param nodeAliases for print nodeId=1 instead long string
     * @param finish is finish rebalance
     * @return statistics per cache group
     */
    private String cacheGroupsRebalanceStatistics(
        final Map<CacheGroupContext, List<RebalanceFuture>> rebFuts,
        final Map<ClusterNode, Integer> nodeAliases,
        final boolean finish
    ) {
        assert nonNull(rebFuts);
        assert nonNull(nodeAliases);

        StringJoiner joiner = new StringJoiner(" ")
            .add(format(
                "Information per cache group (%s):",
                finish ? SUCCESSFUL_OR_NOT_REBALANCE_TEXT : SUCCESSFUL_REBALANCE_TEXT
            ));

        rebFuts.forEach((context, futures) -> {
            long minStartTime = minStartTime(futures.stream());
            long maxEndTime = maxEndTime(futures.stream());

            Map<Integer, List<RebalanceMessageStatistics>> topicStat = toTopicStatistics(futures.stream());
            Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat = toSupplierStatistics(futures.stream());

            joiner.add(format(
                "[id=%s, name=%s, %s]",
                context.groupId(),
                context.cacheOrGroupName(),
                toStartEndDuration(minStartTime, maxEndTime)
            ))
                .add(topicRebalanceStatistics(topicStat))
                .add(supplierRebalanceStatistics(supplierStat, nodeAliases));
        });

        return joiner.toString();
    }

    /**
     * Partitions distribution per cache group.
     * Only for last success rebalance.
     * Works if {@code IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS} set true.
     * Parameters requires not null.
     *
     * @param rebFuts participating in successful and not rebalances
     * @param nodeAliases for print nodeId=1 instead long string
     * @param nodeCnt for adding new nodes into {@code nodeAliases}
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS
     */
    private String partitionsDistributionRebalanceStatistics(
        final Map<CacheGroupContext, List<RebalanceFuture>> rebFuts,
        final Map<ClusterNode, Integer> nodeAliases,
        final AtomicInteger nodeCnt
    ) {
        assert nonNull(rebFuts);
        assert nonNull(nodeAliases);
        assert nonNull(nodeCnt);

        if (!getBoolean(IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, false))
            return "";

        StringJoiner joiner = new StringJoiner(" ")
            .add(format("Partitions distribution per cache group (%s):", SUCCESSFUL_REBALANCE_TEXT));

        Comparator<RebalanceFuture> startTimeCmp = comparingLong(fut -> fut.stat.startTime);

        rebFuts.forEach((context, futures) -> {
            joiner.add(format("[id=%s, name=%s]", context.groupId(), context.cacheOrGroupName()));

            RebalanceFuture lastSuccessFuture = futures.stream()
                .filter(GridFutureAdapter::isDone)
                .filter(this::rebalanceFutureResult)
                .sorted(startTimeCmp.reversed())
                .findFirst()
                .orElse(null);

            if (isNull(lastSuccessFuture))
                return;

            AffinityAssignment affinity = lastSuccessFuture.grp.affinity().cachedAffinity(lastSuccessFuture.topVer);

            Map<ClusterNode, Set<Integer>> primaryParts = affinity.nodes().stream()
                .collect(toMap(identity(), node -> affinity.primaryPartitions(node.id())));

            lastSuccessFuture.stat.msgStats.entrySet().stream()
                .flatMap(entry -> entry.getValue().entrySet().stream())
                .flatMap(entry -> entry.getValue().receivePartStats.stream()
                    .flatMap(rps -> rps.parts.stream())
                    .map(ps -> new T2<>(entry.getKey(), ps))
                )
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

        return joiner.add(aliasesRebalanceStatistics("pr - primary, bu - backup, su - supplier node", nodeAliases))
            .toString();
    }

    /**
     * Statistics per topic.
     * Parameter requires not null.
     *
     * @param topicStat statistics by topics (in successful and not rebalances)
     * @return statistics per topic
     */
    private String topicRebalanceStatistics(final Map<Integer, List<RebalanceMessageStatistics>> topicStat) {
        assert nonNull(topicStat);

        StringJoiner joiner = new StringJoiner(" ")
            .add("Topic statistics:");

        topicStat.forEach((topicId, msgStats) -> {
            long partCnt = sum(msgStats, rps -> rps.parts.size());
            long byteSum = sum(msgStats, rps -> rps.msgSize);
            long entryCount = sum(msgStats, rps -> rps.parts.stream().mapToLong(ps -> ps.entryCount).sum());

            joiner.add(format("[id=%s, %s]",topicId, toPartitionsEntriesBytes(partCnt, entryCount, byteSum)));
        });

        return joiner.toString();
    }

    /**
     * Stattistics per supplier node.
     * Parameters requires not null.
     *
     * @param supplierStat statistics by supplier (in successful and not rebalances)
     * @param nodeAliases for print nodeId=1 instead long string
     * @return statistics per supplier node
     */
    private String supplierRebalanceStatistics(
        final Map<ClusterNode, List<RebalanceMessageStatistics>> supplierStat,
        final Map<ClusterNode, Integer> nodeAliases
    ) {
        assert nonNull(supplierStat);
        assert nonNull(nodeAliases);

        StringJoiner joiner = new StringJoiner(" ")
            .add("Supplier statistics:");

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

        return joiner.toString();
    }

    /**
     * Statistics aliases, for reducing output string.
     * Parameters requires not null.
     *
     * @param nodeAliases for print nodeId=1 instead long string
     * @param abbreviations abbreviations ex. b - bytes
     * @return aliases
     */
    private String aliasesRebalanceStatistics(
        final String abbreviations,
        final Map<ClusterNode, Integer> nodeAliases
    ) {
        assert nonNull(abbreviations);
        assert nonNull(nodeAliases);

        String nodes = nodeAliases.entrySet().stream()
            .sorted(comparingInt(Map.Entry::getValue))
            .map(entry -> format("[%s=%s,%s]", entry.getValue(), entry.getKey().id(), entry.getKey().consistentId()))
            .collect(joining(", "));

        return "Aliases: " + abbreviations + ", nodeId mapping (nodeId=id,consistentId) " + nodes;
    }

    /**
     * Wraps get future result as unchecked exception.
     * Parameter requires not null.
     *
     * @param future requires not null
     * @return result future
     * */
    private boolean rebalanceFutureResult(final RebalanceFuture future) {
        assert nonNull(future);

        try {
            return future.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Time in mills */
    private LocalDateTime toLocalDateTime(final long time){
        return new Date(time).toInstant().atZone(systemDefault()).toLocalDateTime();
    }

    /** Return min {@link RebalanceFutureStatistics#startTime}. Parameter requires not null. */
    private long minStartTime(final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.mapToLong(value -> value.stat.startTime).min().orElse(0);
    }

    /** Return max {@link RebalanceFutureStatistics#endTime}. Parameter requires not null. */
    private long maxEndTime(final Stream<RebalanceFuture> stream) {
        assert nonNull(stream);

        return stream.mapToLong(value -> value.stat.endTime).max().orElse(0);
    }

    /** Return stream of futures. Parameter requires not null. */
    private Stream<RebalanceFuture> toRebalanceFutureStream(
        final Map<CacheGroupContext, List<RebalanceFuture>> rebFuts
    ) {
        assert nonNull(rebFuts);

        return rebFuts.entrySet().stream().flatMap(entry -> entry.getValue().stream());
    }

    /** Statistics per topic. Parameter requires not null. */
    private Map<Integer, List<RebalanceMessageStatistics>> toTopicStatistics(final Stream<RebalanceFuture> stream) {
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

    /** Statistics per supplier. Parameter requires not null. */
    private Map<ClusterNode, List<RebalanceMessageStatistics>> toSupplierStatistics(
        final Stream<RebalanceFuture> stream
    ) {
        assert nonNull(stream);

        return stream.flatMap(future -> future.stat.msgStats.entrySet().stream())
            .flatMap(entry -> entry.getValue().entrySet().stream())
            .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toList())));
    }

    /**
     * To string like "start=2019-08-01 13:15:10,100, end=2019-08-01 13:15:10,200, d=100 ms"
     *
     * @param start in ms
     * @param end in ms
     * @return string like "start=2019-08-01 13:15:10,100, end=2019-08-01 13:15:10,200, d=100 ms"
     * @see #REBALANCE_STATISTICS_DTF
     * */
    private String toStartEndDuration(final long start, final long end) {
        return format(
            "start=%s, end=%s, d=%s ms",
            REBALANCE_STATISTICS_DTF.format(toLocalDateTime(start)),
            REBALANCE_STATISTICS_DTF.format(toLocalDateTime(end)),
            end - start
        );
    }

    /**
     * Calculate sum use {@code longExtractor}. Parameters requires not null.
     *
     * @param msgStats message statistics
     * @param longExtractor long extractor
     * @return sum
     * */
    private long sum(
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

    /** To string like "p=1, e=10, b=1024" */
    private String toPartitionsEntriesBytes(final long parts, final long entries, final long bytes) {
        return format("p=%s, e=%s, b=%s", parts, entries, bytes);
    }
}
