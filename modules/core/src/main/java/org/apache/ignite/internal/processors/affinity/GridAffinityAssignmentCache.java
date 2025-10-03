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

package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityCentralizedFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MIN_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.getFloat;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Affinity cached function.
 */
public class GridAffinityAssignmentCache {
    /** @see IgniteSystemProperties#IGNITE_AFFINITY_HISTORY_SIZE */
    public static final int DFLT_AFFINITY_HISTORY_SIZE = 25;

    /** @see IgniteSystemProperties#IGNITE_MIN_AFFINITY_HISTORY_SIZE */
    public static final int DFLT_MIN_AFFINITY_HISTORY_SIZE = 2;

    /** @see IgniteSystemProperties#IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD */
    public static final float DFLT_PART_DISTRIBUTION_WARN_THRESHOLD = 50f;

    /**
     * Affinity cache will shrink when total number of non-shallow (see {@link HistoryAffinityAssignmentImpl})
     * historical instances will be greater than value of this constant.
     */
    private final int MAX_NON_SHALLOW_HIST_SIZE = getInteger(IGNITE_AFFINITY_HISTORY_SIZE, DFLT_AFFINITY_HISTORY_SIZE);

    /**
     * Affinity cache will also shrink when total number of both shallow ({@link HistoryAffinityAssignmentShallowCopy})
     * and non-shallow (see {@link HistoryAffinityAssignmentImpl}) historical instances will be greater than
     * value of this constant.
     */
    private final int MAX_TOTAL_HIST_SIZE = MAX_NON_SHALLOW_HIST_SIZE * 10;

    /**
     * Independent of {@link #MAX_NON_SHALLOW_HIST_SIZE} and {@link #MAX_TOTAL_HIST_SIZE}, affinity cache will always
     * keep this number of non-shallow (see {@link HistoryAffinityAssignmentImpl}) instances.
     * We need at least one real instance, otherwise we won't be able to get affinity cache for
     * {@link GridCachePartitionExchangeManager#lastAffinityChangedTopologyVersion} in case cluster has experienced
     * too many client joins / client leaves / local cache starts.
     */
    private final int MIN_NON_SHALLOW_HIST_SIZE =
        getInteger(IGNITE_MIN_AFFINITY_HISTORY_SIZE, DFLT_MIN_AFFINITY_HISTORY_SIZE);

    /** Partition distribution. */
    private final float partDistribution = getFloat(IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD,
        DFLT_PART_DISTRIBUTION_WARN_THRESHOLD);

    /** Group name if specified or cache name. */
    private final String cacheOrGrpName;

    /** Group ID. */
    private final int grpId;

    /** Number of backups. */
    private final int backups;

    /** Affinity function. */
    private final AffinityFunction aff;

    /** */
    private final IgnitePredicate<ClusterNode> nodeFilter;

    /** Partitions count. */
    private final int partsCnt;

    /** Affinity calculation results cache: topology version => partition => nodes. */
    private final ConcurrentNavigableMap<AffinityTopologyVersion, HistoryAffinityAssignment> affCache;

    public static class RangeHistoryAffinityAssignment {
        public final HistoryAffinityAssignmentImpl histAssignment;
        public volatile AffinityTopologyVersion startVer;
        public volatile AffinityTopologyVersion endVer;

        RangeHistoryAffinityAssignment(
            HistoryAffinityAssignmentImpl histAssignment,
            AffinityTopologyVersion startVer
        ) {
            this.histAssignment = histAssignment;
            this.startVer = startVer;
            this.endVer = startVer;
        }
        RangeHistoryAffinityAssignment(
            HistoryAffinityAssignmentImpl histAssignment,
            AffinityTopologyVersion startVer,
            AffinityTopologyVersion endVer
        ) {
            this.histAssignment = histAssignment;
            this.startVer = startVer;
            this.endVer = endVer;

//            assert startVer.topologyVersion() == endVer.topologyVersion() :
//                "startVer=" + startVer + ", endVer=" + endVer;
        }
        @Override
        public String toString() {
            return S.toString(RangeHistoryAffinityAssignment.class, this);
        }
    }
    private final ConcurrentNavigableMap<AffinityTopologyVersion, RangeHistoryAffinityAssignment> affCache2;

    /** */
    private volatile IdealAffinityAssignment idealAssignment;

    /** */
    private volatile IdealAffinityAssignment baselineAssignment;

    /** */
    private BaselineTopology baselineTopology;

    /** Cache item corresponding to the head topology version. */
    private final AtomicReference<GridAffinityAssignmentV2> head;

    /** Ready futures. */
    private final ConcurrentMap<AffinityTopologyVersion, AffinityReadyFuture> readyFuts = new ConcurrentSkipListMap<>();

    /** Log. */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final boolean locCache;

    /** */
    private final boolean persistentCache;

    /** */
    private final boolean bltForInMemoryCachesSup = isFeatureEnabled(IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE);

    /** Node stop flag. */
    private volatile IgniteCheckedException stopErr;

    /** Numner of non-shallow (see {@link HistoryAffinityAssignmentImpl}) affinity cache instances.  */
    private final AtomicInteger nonShallowHistSize = new AtomicInteger();

    /** */
    private final Object similarAffKey;

    /**
     * Constructs affinity cached calculations.
     *
     * @param ctx Kernal context.
     * @param cacheOrGrpName Cache or cache group name.
     * @param grpId Group ID.
     * @param aff Affinity function.
     * @param nodeFilter Node filter.
     * @param backups Number of backups.
     * @param locCache Local cache flag.
     */
    public GridAffinityAssignmentCache(GridKernalContext ctx,
        String cacheOrGrpName,
        int grpId,
        AffinityFunction aff,
        IgnitePredicate<ClusterNode> nodeFilter,
        int backups,
        boolean locCache,
        boolean persistentCache
    ) {
        assert ctx != null;
        assert aff != null;
        assert nodeFilter != null;
        assert grpId != 0;

        this.ctx = ctx;
        this.aff = aff;
        this.nodeFilter = nodeFilter;
        this.cacheOrGrpName = cacheOrGrpName;
        this.grpId = grpId;
        this.backups = backups;
        this.locCache = locCache;
        this.persistentCache = persistentCache;

        log = ctx.log(GridAffinityAssignmentCache.class);

        partsCnt = aff.partitions();
        affCache = new ConcurrentSkipListMap<>();
        affCache2 = new ConcurrentSkipListMap<>();
        head = new AtomicReference<>(new GridAffinityAssignmentV2(AffinityTopologyVersion.NONE));

        similarAffKey = ctx.affinity().similaryAffinityKey(aff, nodeFilter, backups, partsCnt);

        assert similarAffKey != null;

//        if (cacheOrGrpName.equalsIgnoreCase("static-cache-0")) {
//            log.warning(">>>>> MAX_TOTAL_HIST_SIZE=" + MAX_TOTAL_HIST_SIZE
//                + ", MAX_NON_SHALLOW_HIST_SIZE=" + MAX_NON_SHALLOW_HIST_SIZE
//                + ", MIN_NON_SHALLOW_HIST_SIZE=" + MIN_NON_SHALLOW_HIST_SIZE);
//        }
    }

    /**
     * @return Key to find caches with similar affinity.
     */
    public Object similarAffinityKey() {
        return similarAffKey;
    }

    /**
     * @return Group name if it is specified, otherwise cache name.
     */
    public String cacheOrGroupName() {
        return cacheOrGrpName;
    }

    /**
     * @return Cache group ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * Initializes affinity with given topology version and assignment.
     *
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment for topology version.
     */
    public void initialize(AffinityTopologyVersion topVer, List<List<ClusterNode>> affAssignment) {
        assert topVer.compareTo(lastVersion()) >= 0 : "[topVer = " + topVer + ", last=" + lastVersion() + ']';

        assert idealAssignment != null;

        GridAffinityAssignmentV2 assignment = new GridAffinityAssignmentV2(topVer, affAssignment, idealAssignment.assignment());

        HistoryAffinityAssignmentImpl newHistEntry = new HistoryAffinityAssignmentImpl(assignment, backups);

        HistoryAffinityAssignment existing = affCache.put(topVer, newHistEntry);
        RangeHistoryAffinityAssignment existing2 = affCache2.put(
            topVer,
            new RangeHistoryAffinityAssignment(newHistEntry, topVer));

        head.set(assignment);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (initialized affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

//        if (!ctx.discovery().localNode().isClient()) {
//            if (cacheOrGrpName.equalsIgnoreCase("static-cache-0")) {
//                log.warning(">>>>> initialize [topVer=" + topVer
//                    + ", topVer=" + topVer
//                    + ", existing=" + existing
//                    + ", newItem=" + newHistEntry
//                    + ']');
//            }
//        }

        onHistoryAdded(existing, newHistEntry);
        // TODO existing2 newHistEntry2
        onHistoryAdded2(existing, newHistEntry);

        if (log.isTraceEnabled()) {
            log.trace("New affinity assignment [grp=" + cacheOrGrpName
                + ", topVer=" + topVer
                + ", aff=" + fold(affAssignment) + "]");
        }
    }

    /**
     * @param assignment Assignment.
     */
    public void idealAssignment(AffinityTopologyVersion topVer, List<List<ClusterNode>> assignment) {
        this.idealAssignment = IdealAffinityAssignment.create(topVer, assignment);
    }

    /**
     * @return Assignment.
     */
    @Nullable public List<List<ClusterNode>> idealAssignmentRaw() {
        return idealAssignment != null ? idealAssignment.assignment() : null;
    }

    /**
     *
     */
    @Nullable public IdealAffinityAssignment idealAssignment() {
        return idealAssignment;
    }

    /**
     * @return {@code True} if affinity function has {@link AffinityCentralizedFunction} annotation.
     */
    public boolean centralizedAffinityFunction() {
        return U.hasAnnotation(aff, AffinityCentralizedFunction.class);
    }

    /**
     * Kernal stop callback.
     *
     * @param err Error.
     */
    public void cancelFutures(IgniteCheckedException err) {
        stopErr = err;

        for (AffinityReadyFuture fut : readyFuts.values())
            fut.onDone(err);
    }

    /**
     *
     */
    public void onReconnected() {
        idealAssignment = null;

        affCache.clear();
        affCache2.clear();

        nonShallowHistSize.set(0);

        head.set(new GridAffinityAssignmentV2(AffinityTopologyVersion.NONE));

        stopErr = null;
    }

    /**
     * Calculates ideal assignment for given topology version and events happened since last calculation.
     *
     * @param topVer Topology version to calculate affinity cache for.
     * @param events Discovery events that caused this topology version change.
     * @param discoCache Discovery cache.
     * @return Ideal affinity assignment.
     */
    public IdealAffinityAssignment calculate(
        AffinityTopologyVersion topVer,
        @Nullable ExchangeDiscoveryEvents events,
        @Nullable DiscoCache discoCache
    ) {
        if (log.isDebugEnabled())
            log.debug("Calculating ideal affinity [topVer=" + topVer + ", locNodeId=" + ctx.localNodeId() +
                ", discoEvts=" + events + ']');

        IdealAffinityAssignment prevAssignment = idealAssignment;

        // Already calculated.
        if (prevAssignment != null && prevAssignment.topologyVersion().equals(topVer))
            return prevAssignment;

        // Resolve nodes snapshot for specified topology version.
        List<ClusterNode> sorted;

        if (!locCache) {
            sorted = new ArrayList<>(discoCache.cacheGroupAffinityNodes(groupId()));

            Collections.sort(sorted, NodeOrderComparator.getInstance());
        }
        else
            sorted = Collections.singletonList(ctx.discovery().localNode());

        boolean hasBaseline = false;
        boolean changedBaseline = false;

        BaselineTopology blt = null;

        if (discoCache != null) {
            blt = discoCache.state().baselineTopology();

            hasBaseline = !ctx.state().inMemoryClusterWithoutBlt() && blt != null;

            if (!persistentCache && hasBaseline)
                hasBaseline = bltForInMemoryCachesSup;

            changedBaseline = !hasBaseline ? baselineTopology != null : !blt.equals(baselineTopology);
        }

        IdealAffinityAssignment assignment;

        if (prevAssignment != null && events != null) {
            /* Skip affinity calculation only when all nodes triggered exchange
               don't belong to affinity for current group (client node or filtered by nodeFilter). */
            boolean skipCalculation = true;

            for (DiscoveryEvent event : events.events()) {
                boolean affinityNode = CU.affinityNode(event.eventNode(), nodeFilter);

                if (affinityNode || event.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                    skipCalculation = false;

                    break;
                }
            }

            if (hasBaseline && changedBaseline) {
                recalculateBaselineAssignment(topVer, events, prevAssignment, sorted, blt);

                assignment = IdealAffinityAssignment.create(topVer, baselineAssignmentWithoutOfflineNodes(discoCache));
            }
            else if (skipCalculation)
                assignment = prevAssignment;
            else if (hasBaseline) {
                if (baselineAssignment == null)
                    recalculateBaselineAssignment(topVer, events, prevAssignment, sorted, blt);

                assignment = IdealAffinityAssignment.create(topVer, baselineAssignmentWithoutOfflineNodes(discoCache));
            }
            else {
                List<List<ClusterNode>> calculated = aff.assignPartitions(new GridAffinityFunctionContextImpl(
                    sorted,
                    prevAssignment.assignment(),
                    events.lastEvent(),
                    topVer,
                    backups
                ));

                assignment = IdealAffinityAssignment.create(topVer, calculated);
            }
        }
        else {
            if (hasBaseline) {
                recalculateBaselineAssignment(topVer, events, prevAssignment, sorted, blt);

                assignment = IdealAffinityAssignment.create(topVer, baselineAssignmentWithoutOfflineNodes(discoCache));
            }
            else {
                List<List<ClusterNode>> calculated = aff.assignPartitions(new GridAffinityFunctionContextImpl(sorted,
                    prevAssignment != null ? prevAssignment.assignment() : null,
                    events != null ? events.lastEvent() : null,
                    topVer,
                    backups
                ));

                assignment = IdealAffinityAssignment.create(topVer, calculated);
            }
        }

        assert assignment != null;

        idealAssignment = assignment;

        if (ctx.cache().cacheMode(cacheOrGrpName) == PARTITIONED && !ctx.clientNode())
            printDistributionIfThresholdExceeded(assignment.assignment(), sorted.size());

        if (hasBaseline) {
            baselineTopology = blt;

            assert baselineAssignment != null;
        }
        else {
            baselineTopology = null;
            baselineAssignment = null;
        }

        if (locCache)
            initialize(topVer, assignment.assignment());

        return assignment;
    }

    /**
     * @param topVer Topology version.
     * @param events Evetns.
     * @param prevAssignment Previous assignment.
     * @param sorted Sorted cache group nodes.
     * @param blt Baseline topology.
     */
    private void recalculateBaselineAssignment(
        AffinityTopologyVersion topVer,
        ExchangeDiscoveryEvents events,
        IdealAffinityAssignment prevAssignment,
        List<ClusterNode> sorted,
        BaselineTopology blt
    ) {
        List<ClusterNode> baselineAffinityNodes = blt.createBaselineView(sorted, nodeFilter);

        List<List<ClusterNode>> calculated = aff.assignPartitions(new GridAffinityFunctionContextImpl(
            baselineAffinityNodes,
            prevAssignment != null ? prevAssignment.assignment() : null,
            events != null ? events.lastEvent() : null,
            topVer,
            backups
        ));

        baselineAssignment = IdealAffinityAssignment.create(topVer, calculated);
    }

    /**
     * @param disco Discovery history.
     * @return Baseline assignment with filtered out offline nodes.
     */
    private List<List<ClusterNode>> baselineAssignmentWithoutOfflineNodes(DiscoCache disco) {
        Map<Object, ClusterNode> alives = new HashMap<>();

        for (ClusterNode node : disco.serverNodes())
            alives.put(node.consistentId(), node);

        List<List<ClusterNode>> assignment = baselineAssignment.assignment();

        List<List<ClusterNode>> result = new ArrayList<>(assignment.size());

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> baselineMapping = assignment.get(p);
            List<ClusterNode> currentMapping = null;

            for (ClusterNode node : baselineMapping) {
                ClusterNode aliveNode = alives.get(node.consistentId());

                if (aliveNode != null) {
                    if (currentMapping == null)
                        currentMapping = new ArrayList<>();

                    currentMapping.add(aliveNode);
                }
            }

            result.add(p, currentMapping != null ? currentMapping : Collections.<ClusterNode>emptyList());
        }

        return result;
    }

    /**
     * Calculates and logs partitions distribution if threshold of uneven distribution {@link #partDistribution} is exceeded.
     *
     * @param assignments Assignments to calculate partitions distribution.
     * @param nodes Affinity nodes number.
     * @see IgniteSystemProperties#IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD
     */
    private void printDistributionIfThresholdExceeded(List<List<ClusterNode>> assignments, int nodes) {
        int locPrimaryCnt = 0;
        int locBackupCnt = 0;

        for (List<ClusterNode> assignment : assignments) {
            for (int i = 0; i < assignment.size(); i++) {
                ClusterNode node = assignment.get(i);

                if (node.isLocal()) {
                    if (i == 0)
                        locPrimaryCnt++;
                    else
                        locBackupCnt++;
                }
            }
        }

        float expCnt = (float)partsCnt / nodes;

        float deltaPrimary = Math.abs(1 - (float)locPrimaryCnt / expCnt) * 100;
        float deltaBackup = Math.abs(1 - (float)locBackupCnt / (expCnt * backups)) * 100;

        if ((deltaPrimary > partDistribution || deltaBackup > partDistribution) && log.isInfoEnabled()) {
            log.info(String.format("Local node affinity assignment distribution is not even " +
                    "[cache=%s, expectedPrimary=%.2f, actualPrimary=%d, " +
                    "expectedBackups=%.2f, actualBackups=%d, warningThreshold=%.2f%%]",
                cacheOrGrpName, expCnt, locPrimaryCnt,
                expCnt * backups, locBackupCnt, partDistribution));
        }
    }

    /**
     * Copies previous affinity assignment when discovery event does not cause affinity assignment changes
     * (e.g. client node joins on leaves).
     *
     * @param evt Event.
     * @param topVer Topology version.
     */
    public void clientEventTopologyChange(DiscoveryEvent evt, AffinityTopologyVersion topVer) {
        assert topVer.compareTo(lastVersion()) >= 0 : "[topVer = " + topVer + ", last=" + lastVersion() + ']';

        GridAffinityAssignmentV2 aff = head.get();

        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT || aff.primaryPartitions(evt.eventNode().id()).isEmpty() : evt;

        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT || aff.backupPartitions(evt.eventNode().id()).isEmpty() : evt;

        GridAffinityAssignmentV2 assignmentCpy = new GridAffinityAssignmentV2(topVer, aff);

        AffinityTopologyVersion prevVer = topVer.minorTopologyVersion() == 0 ?
            new AffinityTopologyVersion(topVer.topologyVersion() - 1, Integer.MAX_VALUE) :
            new AffinityTopologyVersion(topVer.topologyVersion(), topVer.minorTopologyVersion() - 1);

        Map.Entry<AffinityTopologyVersion, HistoryAffinityAssignment> prevHistEntry = affCache.floorEntry(prevVer);
        Map.Entry<AffinityTopologyVersion, RangeHistoryAffinityAssignment> prevHistEntry2 = affCache2.floorEntry(prevVer);

        HistoryAffinityAssignment newHistEntry = (prevHistEntry == null) ?
            new HistoryAffinityAssignmentImpl(assignmentCpy, backups) :
            new HistoryAffinityAssignmentShallowCopy(prevHistEntry.getValue().origin(), topVer);

        RangeHistoryAffinityAssignment newHistEntry2 = (prevHistEntry2 == null) ?
            new RangeHistoryAffinityAssignment(new HistoryAffinityAssignmentImpl(assignmentCpy, backups), topVer) :
            new RangeHistoryAffinityAssignment(
                prevHistEntry2.getValue().histAssignment,
                prevHistEntry2.getValue().startVer,
                topVer);

        HistoryAffinityAssignment existing = affCache.put(topVer, newHistEntry);
        if (prevHistEntry2 == null) {
            RangeHistoryAffinityAssignment existing2 = affCache2.put(topVer, newHistEntry2);
        } else {
            RangeHistoryAffinityAssignment existing2 = affCache2.put(prevHistEntry2.getKey(), newHistEntry2);
        }

        head.set(assignmentCpy);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (use previous affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

//        if (!ctx.discovery().localNode().isClient()) {
//            if (cacheOrGrpName.equalsIgnoreCase("static-cache-0")) {
//                log.warning(">>>>> clientEventTopologyChange [topVer=" + topVer
//                    + ", prev=" + prevVer
//                    + ", prevHistEntry=" + prevHistEntry
//                    + ", existing=" + existing
//                    + ", newItem=" + newHistEntry
//                    + ']');
//            }
//        }

        onHistoryAdded(existing, newHistEntry);
        // TODO existing2 newHistEntry2
        onHistoryAdded2(existing, newHistEntry);
    }

    /**
     * @return Last initialized affinity version.
     */
    public AffinityTopologyVersion lastVersion() {
        return head.get().topologyVersion();
    }

    /**
     * @return Last initialized affinity assignment.
     */
    public AffinityAssignment lastReadyAffinity() {
        return head.get();
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> assignments(AffinityTopologyVersion topVer) {
        AffinityAssignment aff = cachedAffinity(topVer);

        return aff.assignment();
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> readyAssignments(AffinityTopologyVersion topVer) {
        AffinityAssignment aff = readyAffinity(topVer);

        assert aff != null : "No ready affinity [grp=" + cacheOrGrpName + ", ver=" + topVer + ']';

        return aff.assignment();
    }

    /**
     * Gets future that will be completed after topology with version {@code topVer} is calculated.
     *
     * @param topVer Topology version to await for.
     * @return Future that will be completed after affinity for topology version {@code topVer} is calculated.
     */
    @Nullable public IgniteInternalFuture<AffinityTopologyVersion> readyFuture(AffinityTopologyVersion topVer) {
        GridAffinityAssignmentV2 aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Returning finished future for readyFuture [head=" + aff.topologyVersion() +
                    ", topVer=" + topVer + ']');

            return null;
        }

        GridFutureAdapter<AffinityTopologyVersion> fut = F.addIfAbsent(readyFuts, topVer,
            new AffinityReadyFuture(topVer));

        aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Completing topology ready future right away [head=" + aff.topologyVersion() +
                    ", topVer=" + topVer + ']');

            fut.onDone(aff.topologyVersion());
        }
        else if (stopErr != null)
            fut.onDone(stopErr);

        return fut;
    }

    /**
     * @return Partition count.
     */
    public int partitions() {
        return partsCnt;
    }

    /**
     * Gets affinity nodes for specified partition.
     *
     * @param part Partition.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodes(int part, AffinityTopologyVersion topVer) {
        // Resolve cached affinity nodes.
        return cachedAffinity(topVer).get(part);
    }

    /**
     * @param topVer Topology version.
     */
    public Set<Integer> partitionPrimariesDifferentToIdeal(AffinityTopologyVersion topVer) {
        return cachedAffinity(topVer).partitionPrimariesDifferentToIdeal();
    }

    /**
     * Get primary partitions for specified node ID.
     *
     * @param nodeId Node ID to get primary partitions for.
     * @param topVer Topology version.
     * @return Primary partitions for specified node ID.
     */
    public Set<Integer> primaryPartitions(UUID nodeId, AffinityTopologyVersion topVer) {
        return cachedAffinity(topVer).primaryPartitions(nodeId);
    }

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @param topVer Topology version.
     * @return Backup partitions for specified node ID.
     */
    public Set<Integer> backupPartitions(UUID nodeId, AffinityTopologyVersion topVer) {
        return cachedAffinity(topVer).backupPartitions(nodeId);
    }

    public void dumpAssignmentsDebugInfo() {
        log.warning(">>>>> initial map: " + affCache.size());
        for (Map.Entry<AffinityTopologyVersion, HistoryAffinityAssignment> e : affCache.entrySet()) {
            log.warning(">>> >>> " + e.getKey() + ", copy=" + (e.getValue() instanceof HistoryAffinityAssignmentShallowCopy));
        }
        log.warning(">>>>> updated map: " + affCache2.size());
        for (Map.Entry<AffinityTopologyVersion, RangeHistoryAffinityAssignment> e : affCache2.entrySet()) {
            GridAffinityAssignmentCache.RangeHistoryAffinityAssignment a = e.getValue();
            log.warning(">>> >>> [" + a.startVer + " - " + a.endVer + ']');
        }
//        log.warning(">>>>> affinity processor map: " + map.size());
//        for (Map.Entry<GridAffinityProcessor.AffinityAssignmentKey, IgniteInternalFuture<GridAffinityProcessor.AffinityInfo>> e : map.entrySet()) {
//            if (e.getKey().cacheName.equalsIgnoreCase("static-cache-0")) {
//                log.warning(">>> >>> " + e.getKey().topVer);
//            }
//        }
        log.warning(">>>>> end map.");
    }

    /**
     * Dumps debug information.
     *
     * @return {@code True} if there are pending futures.
     */
    public boolean dumpDebugInfo() {
        if (!readyFuts.isEmpty()) {
            U.warn(log, "First 3 pending affinity ready futures [grp=" + cacheOrGrpName +
                ", total=" + readyFuts.size() +
                ", lastVer=" + lastVersion() + "]:");

            int cnt = 0;

            for (AffinityReadyFuture fut : readyFuts.values()) {
                U.warn(log, ">>> " + fut);

                if (++cnt == 3)
                    break;
            }

            return true;
        }

        return false;
    }

    /**
     * @param topVer Topology version.
     * @return Assignment.
     * @throws IllegalStateException If affinity assignment is not initialized for the given topology version.
     */
    public AffinityAssignment readyAffinity(AffinityTopologyVersion topVer) {
        AffinityAssignment cache = head.get();
        AffinityAssignment cache2 = head.get();

        if (!cache.topologyVersion().equals(topVer)) {
            cache = affCache.get(topVer);

            Map.Entry<AffinityTopologyVersion,RangeHistoryAffinityAssignment> entry = affCache2.floorEntry(topVer);

            if (entry != null) {
                boolean found = topVer.isBetween(entry.getValue().startVer, entry.getValue().endVer);
                if (found) {
                    cache2 = new HistoryAffinityAssignmentShallowCopy(entry.getValue().histAssignment, topVer);
                }
            }

            assert (cache != null && cache2 != null) || (cache == null && cache2 == null) :
                "One of the caches is null: [cache=" + cache + ", cache2=" + cache2 + ']';

            if (cache == null || entry == null) {
                throw new IllegalStateException("Affinity for topology version is " +
                    "not initialized [locNode=" + ctx.discovery().localNode().id() +
                    ", grp=" + cacheOrGrpName +
                    ", topVer=" + topVer +
                    ", head=" + head.get().topologyVersion() +
                    ", history=" + affCache.keySet() +
                    ", maxNonShallowHistorySize=" + MAX_NON_SHALLOW_HIST_SIZE +
                    ']');
            }
        }

        return cache;
    }

    /**
     * Get cached affinity for specified topology version.
     *
     * @param topVer Topology version.
     * @return Cached affinity.
     * @throws IllegalArgumentException in case of the specified topology version {@code topVer}
     *                                  is earlier than affinity is calculated
     *                                  or the history of assignments is already cleaned.
     */
    public AffinityAssignment cachedAffinity(AffinityTopologyVersion topVer) {
        AffinityTopologyVersion lastAffChangeTopVer =
            ctx.cache().context().exchange().lastAffinityChangedTopologyVersion(topVer);

        return cachedAffinity(topVer, lastAffChangeTopVer);
    }

    /**
     * Get cached affinity for specified topology version.
     *
     * @param topVer Topology version for which affinity assignment is requested.
     * @param lastAffChangeTopVer Topology version of last affinity assignment change.
     * @return Cached affinity.
     * @throws IllegalArgumentException in case of the specified topology version {@code topVer}
     *                                  is earlier than affinity is calculated
     *                                  or the history of assignments is already cleaned.
     */
    public AffinityAssignment cachedAffinity(
        AffinityTopologyVersion topVer,
        AffinityTopologyVersion lastAffChangeTopVer
    ) {
        if (topVer.equals(AffinityTopologyVersion.NONE))
            topVer = lastAffChangeTopVer = lastVersion();
        else {
            if (lastAffChangeTopVer.equals(AffinityTopologyVersion.NONE))
                lastAffChangeTopVer = topVer;

            awaitTopologyVersion(lastAffChangeTopVer);
        }

        assert topVer.topologyVersion() >= 0 : topVer;

        AffinityAssignment cache = head.get();
        AffinityAssignment cache2 = head.get();

        if (cache.topologyVersion().before(lastAffChangeTopVer) || cache.topologyVersion().after(topVer)) {

            // TODO
            Map.Entry<AffinityTopologyVersion, HistoryAffinityAssignment> e = affCache.ceilingEntry(lastAffChangeTopVer);
            // ceilingEntry vs floorEntry???
            // it is assumed that lastAffChangeTopVer is always present in affCache2 as follows:
            // it is not possible to have a range like [startVer= top1, endVer=top2] where lastAffChangeTopVer is between top1 and top2
            // i.e. top1 < lastAffChangeTopVer < top2
            // I think it always should be startVer == lastAffChangeTopVer
            Map.Entry<AffinityTopologyVersion, RangeHistoryAffinityAssignment> en = affCache2.floorEntry(lastAffChangeTopVer);

            if (e != null) {
                cache = e.getValue();
//                log.warning(">>>>> e is NOT null [cahetopver=" + cache.topologyVersion() + ']');
            } else {
//                log.warning(">>>>> e is null in affCache [lastAffChangeTopVer=" + lastAffChangeTopVer
//                    + ", topVer=" + topVer
//                    + ", affCache=" + affCache.keySet()
//                    + ']');
            }

            if (en != null) {
                //*topver??? lastAffChangeTopVer??*/
                boolean found = lastAffChangeTopVer.isBetween(en.getValue().startVer, en.getValue().endVer);
                if (found) {
                    cache2 = new HistoryAffinityAssignmentShallowCopy(en.getValue().histAssignment, lastAffChangeTopVer/*???*/);
                }
                else {
                    /// should we do anything?
//                log.warning(">>>>> NOT FOUND in affCache2 [enKey=" + en.getKey()
//                        + ", start=" + en.getValue().startVer
//                        + ", end=" + en.getValue().endVer
//                        + ", lastAffChangeTopVer=" + lastAffChangeTopVer
//                        + ", topVer=" + topVer
//                        + ']');
                }
//                if (cacheOrGrpName.equalsIgnoreCase("static-cache-3")) {
//                    log.warning(">>>>> test message [enTopVer=" + en.getKey() + ", start=" + en.getValue().startVer +
//                        ", end=" + en.getValue().endVer +
//                        ", found=" + found + ", topVer=" + topVer + ']');
//                }
            } else {
//                log.warning(">>>>> en is null in affCache2 [lastAffChangeTopVer=" + lastAffChangeTopVer
//                    + ", topVer=" + topVer
//                    + ", affCache2=" + affCache2.keySet()
//                    + ']');
                en = affCache2.firstEntry();
                boolean found = true;//lastAffChangeTopVer.isBetween(en.getValue().startVer, en.getValue().endVer);
                if (found) {
//                    log.warning(">>>>> en is null found [en=" + en + ']');
                    cache2 = new HistoryAffinityAssignmentShallowCopy(en.getValue().histAssignment, en.getValue().startVer/*???*/);
                }
            }

            assert (cache != null && cache2 != null) || (cache == null && cache2 == null) :
                "One of the caches is null: [cache=" + cache + ", cache2=" + cache2 + ']';

            if (cache == null || cache2 == null) {
                throw new IllegalStateException("Getting affinity for topology version earlier than affinity is " +
                    "calculated [locNode=" + ctx.discovery().localNode() +
                    ", grp=" + cacheOrGrpName +
                    ", topVer=" + topVer +
                    ", lastAffChangeTopVer=" + lastAffChangeTopVer +
                    ", head=" + head.get().topologyVersion() +
                    ", history=" + affCache.keySet() +
                    ", minNonShallowHistorySize=" + MIN_NON_SHALLOW_HIST_SIZE +
                    ", maxNonShallowHistorySize=" + MAX_NON_SHALLOW_HIST_SIZE +
                    ']');
            }


            if (!cache.topologyVersion().equals(cache2.topologyVersion())) {
                log.warning(">>>>> WARNING: different cached affinity instances: " +
                    "[topVer=" + topVer + ", affChangd=" + lastAffChangeTopVer + ", cache=" + cache + ", cache2=" + cache2 + ']');
                boolean r1 = cache.topologyVersion().compareTo(topVer) > 0;
                boolean r2 = cache2.topologyVersion().compareTo(topVer) > 0;
                log.warning(">>>>> WARNING head = [" + head.get() + ", r1=" + r1 + ", r2=" + r2 + ']');
                this.dumpAssignmentsDebugInfo();
            }

            assert cache.topologyVersion().equals(cache2.topologyVersion()) :
                "Different cached affinity instances: [topVer=" + topVer +
                    ", affChangd=" + lastAffChangeTopVer +
                    ", cache=" + cache +
                    ", cache2=" + cache2 + ']';

            if (cache.topologyVersion().compareTo(topVer) > 0 || cache2.topologyVersion().compareTo(topVer) > 0) {
                throw new IllegalStateException("Getting affinity for too old topology version that is already " +
                    "out of history (try to increase '" + IGNITE_MIN_AFFINITY_HISTORY_SIZE + "' or '" +
                    IGNITE_AFFINITY_HISTORY_SIZE + "' system properties)" +
                    " [locNode=" + ctx.discovery().localNode() +
                    ", grp=" + cacheOrGrpName +
                    ", topVer=" + topVer +
                    ", lastAffChangeTopVer=" + lastAffChangeTopVer +
                    ", head=" + head.get().topologyVersion() +
                    ", history=" + affCache.keySet() +
                    ", minNonShallowHistorySize=" + MIN_NON_SHALLOW_HIST_SIZE +
                    ", maxNonShallowHistorySize=" + MAX_NON_SHALLOW_HIST_SIZE +
                    ']');
            }
        }

        assert cache.topologyVersion().compareTo(lastAffChangeTopVer) >= 0 &&
            cache.topologyVersion().compareTo(topVer) <= 0 : "Invalid cached affinity: [cache=" + cache + ", topVer=" + topVer + ", lastAffChangedTopVer=" + lastAffChangeTopVer + "]";

        assert cache2.topologyVersion().compareTo(lastAffChangeTopVer) >= 0 &&
            cache2.topologyVersion().compareTo(topVer) <= 0 : "Invalid cached affinity (2): [cache=" + cache2 + ", topVer=" + topVer + ", lastAffChangedTopVer=" + lastAffChangeTopVer + "]";

        return cache2;
    }

    /**
     * @param part Partition.
     * @param startVer Start version.
     * @param endVer End version.
     * @return {@code True} if primary changed or required affinity version not found in history.
     */
    public boolean primaryChanged(int part, AffinityTopologyVersion startVer, AffinityTopologyVersion endVer) {
        // TODO
        AffinityAssignment aff = affCache.get(startVer);
        AffinityAssignment aff2 = null;
        Map.Entry<AffinityTopologyVersion, RangeHistoryAffinityAssignment> range = affCache2.floorEntry(startVer);

        if (range != null) {
            boolean found = startVer.isBetween(range.getValue().startVer, range.getValue().endVer);
            if (found) {
                aff2 = range.getValue().histAssignment;
            }
        } else {
            range = affCache2.firstEntry();
            if (range != null && startVer.isBetween(range.getValue().startVer, range.getValue().endVer)) {
                aff2 = range.getValue().histAssignment;
            }
        }

        if (!((aff == null && aff2 == null) || (aff != null && aff2 != null))) {
            log.warning(">>>>> One of the caches is null: [aff=" + aff + ", aff2=" + aff2
                + ", part=" + part + ", startVer=" + startVer + ", endVer=" + endVer
                + ']');

            dumpAssignmentsDebugInfo();
        }

        assert (aff == null && aff2 == null) || (aff != null && aff2 != null) :
            "One of the caches is null: [aff=" + aff + ", aff2=" + aff2 + ']';

        if (aff == null)
            return false;

        List<ClusterNode> nodes = aff.get(part);
        List<ClusterNode> nodes2 = aff2.get(part);

        assert nodes.equals(nodes2) : "Different nodes [nodes=" + nodes + ", nodes2=" + nodes2 + ']';

        if (nodes.isEmpty())
            return true;

        ClusterNode primary = nodes.get(0);
        ClusterNode primary2 = nodes2.get(0);

        assert primary.equals(primary2) : "Different primaries [primary=" + primary + ", primary2=" + primary2 + ']';

        boolean ret1 = true;

        assert range != null;

        boolean ret2 = !range.getValue().endVer.after(startVer);

        for (AffinityAssignment assignment : affCache.tailMap(startVer, false).values()) {
            List<ClusterNode> nodes0 = assignment.assignment().get(part);

            if (nodes0.isEmpty()) {
                ret1 = true;
                break;
            }

            if (!nodes0.get(0).equals(primary)) {
                ret1 = true;
                break;
            }

            if (assignment.topologyVersion().equals(endVer)) {
                ret1 = false;
                break;
            }
        }

        for (RangeHistoryAffinityAssignment r : affCache2.tailMap(startVer, false).values()) {
            List<ClusterNode> nodes0 = r.histAssignment.get(part);

            if (nodes0.isEmpty()) {
                ret2 = true;
                break;
            }

            if (!nodes0.get(0).equals(primary)) {
                ret2 = true;
                break;
            }

            // assignment.topologyVersion().equals(endVer)
            if (endVer.isBetween(r.startVer, r.endVer)) {
                ret2 = false;
                break;
            }
        }

        if (ret1 != ret2) {
            log.warning(">>>>> Different results: [ret1=" + ret1 + ", ret2=" + ret2
                + ", part=" + part + ", startVer=" + startVer + ", endVer=" + endVer
                + ", range=" + range
                + ", size=" + affCache2.tailMap(startVer, false).size()
                + ']');

            dumpAssignmentsDebugInfo();
        }
        assert ret1 == ret2 : "Different results [ret1=" + ret1 + ", ret2=" + ret2 + ']';
        return ret2;
    }

    /**
     * @param aff Affinity cache.
     */
    public void init(GridAffinityAssignmentCache aff) {
        assert aff.lastVersion().compareTo(lastVersion()) >= 0;
        assert aff.idealAssignmentRaw() != null;

        idealAssignment(aff.lastVersion(), aff.idealAssignmentRaw());

        AffinityAssignment assign = aff.cachedAffinity(aff.lastVersion());

        initialize(aff.lastVersion(), assign.assignment());
    }

    /**
     * @param topVer Topology version to wait.
     */
    private void awaitTopologyVersion(AffinityTopologyVersion topVer) {
        GridAffinityAssignmentV2 aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0)
            return;

        try {
            if (log.isDebugEnabled())
                log.debug("Will wait for topology version [locNodeId=" + ctx.localNodeId() +
                ", topVer=" + topVer + ']');

            IgniteInternalFuture<AffinityTopologyVersion> fut = readyFuture(topVer);

            if (fut != null) {
                Thread curTh = Thread.currentThread();

                String threadName = curTh.getName();

                try {
                    curTh.setName(threadName + " (waiting " + topVer + ")");

                    fut.get();
                }
                finally {
                    curTh.setName(threadName);
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for affinity ready future for topology version: " + topVer,
                e);
        }
    }

    /**
     * Cleaning the affinity history.
     *
     * @param replaced Replaced entry in case history item was already present, null otherwise.
     * @param added New history item.
     */
    private void onHistoryAdded(
        HistoryAffinityAssignment replaced,
        HistoryAffinityAssignment added
    ) {
        boolean cleanupNeeded = false;

        if (replaced == null) {
            cleanupNeeded = true;

            if (added.requiresHistoryCleanup())
                nonShallowHistSize.incrementAndGet();
        }
        else {
            if (replaced.requiresHistoryCleanup() != added.requiresHistoryCleanup()) {
                if (added.requiresHistoryCleanup()) {
                    cleanupNeeded = true;

                    nonShallowHistSize.incrementAndGet();
                }
                else
                    nonShallowHistSize.decrementAndGet();
            }
        }

        if (!cleanupNeeded)
            return;

        int nonShallowSize = nonShallowHistSize.get();

        int totalSize = affCache.size();

        int originNonShallowSize = nonShallowSize;

        int originTotalSize = totalSize;

        if (shouldContinueCleanup(nonShallowSize, totalSize)) {
            int initNonShallowSize = nonShallowSize;

            AffinityTopologyVersion firstVer = null;
            AffinityTopologyVersion lastVer = null;

            Iterator<HistoryAffinityAssignment> it = affCache.values().iterator();

            while (it.hasNext()) {
                HistoryAffinityAssignment aff0 = it.next();

                if (firstVer == null)
                    firstVer = aff0.topologyVersion();

                if (aff0.requiresHistoryCleanup()) {
                    // We can stop cleanup only on non-shallow item.
                    // Keeping part of shallow items chain if corresponding real item is missing makes no sense.
                    if (!shouldContinueCleanup(nonShallowSize, totalSize)) {
                        nonShallowHistSize.getAndAdd(nonShallowSize - initNonShallowSize);

                        // GridAffinityProcessor#affMap has the same size and instance set as #affCache.
                        ctx.affinity().removeCachedAffinity(aff0.topologyVersion());

                        if (log.isDebugEnabled()) {
                            String msg = String.format(
                                "Removed affinity assignments for group [name=%s, nonShallowSize=%s, totalSize=%s",
                                cacheOrGrpName, originNonShallowSize, originTotalSize
                            );

                            if (lastVer == null || lastVer.equals(firstVer)) {
                                msg += String.format(", version=[topVer=%s, minorTopVer=%s]]",
                                    firstVer.topologyVersion(), firstVer.minorTopologyVersion());
                            }
                            else {
                                msg += String.format(", from=[topVer=%s, minorTopVer=%s], " +
                                        "to=[topVer=%s, minorTopVer=%s]]",
                                    firstVer.topologyVersion(), firstVer.minorTopologyVersion(),
                                    lastVer.topologyVersion(), lastVer.minorTopologyVersion());
                            }

                            log.debug(msg);
                        }

                        return;
                    }

                    nonShallowSize--;
                }

                totalSize--;

                it.remove();

                lastVer = aff0.topologyVersion();
            }

            assert false : "All elements have been removed from affinity cache during cleanup";
        }
    }

    private void onHistoryAdded2(
        HistoryAffinityAssignment replaced,
        HistoryAffinityAssignment added
    ) {
        if (true) {
            int totalSize = affCache2.size();

            while (shouldContinueCleanup(totalSize, totalSize)) {
                Map.Entry<AffinityTopologyVersion, RangeHistoryAffinityAssignment> entry = affCache2.firstEntry();

                assert entry != null;

                affCache2.remove(entry.getKey());
                // GridAffinityProcessor#affMap has the same size and instance set as #affCache.
                ctx.affinity().removeCachedAffinity(entry.getValue().endVer); // perhaps it should be it.next().startVer

                totalSize -= 1;
            }

            return;
        }

        boolean cleanupNeeded = false;

        if (replaced == null) {
            cleanupNeeded = true;

            if (added.requiresHistoryCleanup())
                nonShallowHistSize.incrementAndGet();
        }
        else {
            if (replaced.requiresHistoryCleanup() != added.requiresHistoryCleanup()) {
                if (added.requiresHistoryCleanup()) {
                    cleanupNeeded = true;

                    nonShallowHistSize.incrementAndGet();
                }
                else
                    nonShallowHistSize.decrementAndGet();
            }
        }

        if (!cleanupNeeded)
            return;

        int nonShallowSize = nonShallowHistSize.get();

        int totalSize = affCache.size();

        int originNonShallowSize = nonShallowSize;

        int originTotalSize = totalSize;

        if (shouldContinueCleanup(nonShallowSize, totalSize)) {
            int initNonShallowSize = nonShallowSize;

            AffinityTopologyVersion firstVer = null;
            AffinityTopologyVersion lastVer = null;

            Iterator<HistoryAffinityAssignment> it = affCache.values().iterator();

            while (it.hasNext()) {
                HistoryAffinityAssignment aff0 = it.next();

                if (firstVer == null)
                    firstVer = aff0.topologyVersion();

                if (aff0.requiresHistoryCleanup()) {
                    // We can stop cleanup only on non-shallow item.
                    // Keeping part of shallow items chain if corresponding real item is missing makes no sense.
                    if (!shouldContinueCleanup(nonShallowSize, totalSize)) {
                        nonShallowHistSize.getAndAdd(nonShallowSize - initNonShallowSize);

                        // GridAffinityProcessor#affMap has the same size and instance set as #affCache.
                        ctx.affinity().removeCachedAffinity(aff0.topologyVersion());

                        if (log.isDebugEnabled()) {
                            String msg = String.format(
                                "Removed affinity assignments for group [name=%s, nonShallowSize=%s, totalSize=%s",
                                cacheOrGrpName, originNonShallowSize, originTotalSize
                            );

                            if (lastVer == null || lastVer.equals(firstVer)) {
                                msg += String.format(", version=[topVer=%s, minorTopVer=%s]]",
                                    firstVer.topologyVersion(), firstVer.minorTopologyVersion());
                            }
                            else {
                                msg += String.format(", from=[topVer=%s, minorTopVer=%s], " +
                                        "to=[topVer=%s, minorTopVer=%s]]",
                                    firstVer.topologyVersion(), firstVer.minorTopologyVersion(),
                                    lastVer.topologyVersion(), lastVer.minorTopologyVersion());
                            }

                            log.debug(msg);
                        }

                        return;
                    }

                    nonShallowSize--;
                }

                totalSize--;

                it.remove();

                lastVer = aff0.topologyVersion();
            }

            assert false : "All elements have been removed from affinity cache during cleanup";
        }
    }

    /**
     * Checks whether affinity cache size conditions are still unsatisfied.
     *
     * @param nonShallowSize Non-shallow size.
     * @param totalSize Total size.
     * @return <code>true</code> if affinity cache cleanup is not finished yet.
     */
    private boolean shouldContinueCleanup(int nonShallowSize, int totalSize) {
        if (nonShallowSize <= MIN_NON_SHALLOW_HIST_SIZE)
            return false;

        return nonShallowSize > MAX_NON_SHALLOW_HIST_SIZE || totalSize > MAX_TOTAL_HIST_SIZE;
    }

    /**
     * @return All initialized versions.
     */
    public NavigableSet<AffinityTopologyVersion> cachedVersions() {
        if (true)
            return affCache.keySet();

        // TODO the following code is error-prone, need to be fixed
        NavigableSet<AffinityTopologyVersion> res = new GridConcurrentSkipListSet<>();

        for (Map.Entry<AffinityTopologyVersion, RangeHistoryAffinityAssignment> e : affCache2.entrySet()) {
            AffinityTopologyVersion startVer = e.getValue().startVer;
            AffinityTopologyVersion endVer = e.getValue().endVer;

            for (int topVer = startVer.minorTopologyVersion(); topVer <= endVer.minorTopologyVersion(); topVer++) {
                res.add(new AffinityTopologyVersion(startVer.topologyVersion(), topVer));
            }
        }
        return res;
    }

    public List<IgniteBiTuple<AffinityTopologyVersion, AffinityTopologyVersion>> cachedVersions2() {
        List<IgniteBiTuple<AffinityTopologyVersion, AffinityTopologyVersion>> res = new ArrayList<>(affCache2.size());

        for (Map.Entry<AffinityTopologyVersion, RangeHistoryAffinityAssignment> e : affCache2.entrySet()) {
            AffinityTopologyVersion startVer = e.getValue().startVer;
            AffinityTopologyVersion endVer = e.getValue().endVer;

            res.add(new IgniteBiTuple<>(startVer, endVer));
        }
        //return affCache.keySet();
        return res;
    }

    /**
     * @param affAssignment Affinity assignment.
     * @return String representation of given {@code affAssignment}.
     */
    private static String fold(List<List<ClusterNode>> affAssignment) {
        SB sb = new SB();

        for (int p = 0; p < affAssignment.size(); p++) {
            sb.a("Part [");
            sb.a("id=" + p + ", ");

            SB partOwners = new SB();

            List<ClusterNode> affOwners = affAssignment.get(p);

            for (ClusterNode node : affOwners) {
                partOwners.a(node.consistentId());
                partOwners.a(' ');
            }

            sb.a("owners=[");
            sb.a(partOwners);
            sb.a(']');

            sb.a("] ");
        }

        return sb.toString();
    }

    /**
     * Affinity ready future. Will remove itself from ready futures map.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<AffinityTopologyVersion> {
        /** */
        private AffinityTopologyVersion reqTopVer;

        /**
         *
         * @param reqTopVer Required topology version.
         */
        private AffinityReadyFuture(AffinityTopologyVersion reqTopVer) {
            this.reqTopVer = reqTopVer;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(AffinityTopologyVersion res, @Nullable Throwable err) {
            assert res != null || err != null;

            boolean done = super.onDone(res, err);

            if (done)
                readyFuts.remove(reqTopVer, this);

            return done;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityReadyFuture.class, this);
        }
    }
}
