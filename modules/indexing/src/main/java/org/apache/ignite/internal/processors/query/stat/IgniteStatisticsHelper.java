/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.gridgain.internal.h2.table.Column;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods to statistics messages generation.
 */
public class IgniteStatisticsHelper {
    /** Logger. */
    private final IgniteLogger log;

    /** Local node id. */
    private final UUID locNodeId;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param locNodeId Local node id.
     * @param schemaMgr Schema manager.
     * @param logSupplier Ignite logger supplier to get logger from.
     */
    public IgniteStatisticsHelper(
        UUID locNodeId,
        SchemaManager schemaMgr,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.locNodeId = locNodeId;
        this.schemaMgr = schemaMgr;
        this.log = logSupplier.apply(IgniteStatisticsHelper.class);
    }

    /**
     * Aggregate specified partition level statistics to local level statistics.
     *
     * @param keyMsg Aggregation key.
     * @param stats Collection of all local partition level or local level statistics by specified key to aggregate.
     * @return Local level aggregated statistics.
     */
    public ObjectStatisticsImpl aggregateLocalStatistics(
        StatisticsKeyMessage keyMsg,
        Collection<? extends ObjectStatisticsImpl> stats
    ) {
        // For now there can be only tables
        GridH2Table tbl = schemaMgr.dataTable(keyMsg.schema(), keyMsg.obj());

        if (tbl == null) {
            // remove all loaded statistics.
            if (log.isDebugEnabled())
                log.debug(String.format("Removing statistics for object %s.%s cause table doesn't exists.",
                        keyMsg.schema(), keyMsg.obj()));
        }

        return aggregateLocalStatistics(tbl, filterColumns(tbl.getColumns(), keyMsg.colNames()), stats, log);
    }

    /**
     * Aggregate partition level statistics to local level one or local statistics to global one.
     *
     * @param tbl Table to aggregate statistics by.
     * @param selectedCols Columns to aggregate statistics by.
     * @param stats Collection of partition level or local level statistics to aggregate.
     * @param log
     * @return Local level statistics.
     */
    public static ObjectStatisticsImpl aggregateLocalStatistics(
        GridH2Table tbl,
        Column[] selectedCols,
        Collection<? extends ObjectStatisticsImpl> stats,
        IgniteLogger log
    ) {
        assert !stats.isEmpty();

        Map<Column, List<ColumnStatistics>> colPartStats = new HashMap<>(selectedCols.length);
        long rowCnt = 0;

        for (Column col : selectedCols)
            colPartStats.put(col, new ArrayList<>());

        for (ObjectStatisticsImpl partStat : stats) {
            for (Column col : selectedCols) {
                ColumnStatistics colPartStat = partStat.columnStatistics(col.getName());

                if (colPartStat != null) {
                    colPartStats.computeIfPresent(col, (k, v) -> {
                        v.add(colPartStat);

                        return v;
                    });
                }
            }

            rowCnt += partStat.rowCount();
        }

        Map<String, ColumnStatistics> colStats = new HashMap<>(selectedCols.length);

        for (Column col : selectedCols) {
            ColumnStatistics stat = ColumnStatisticsCollector.aggregate(tbl::compareValues, colPartStats.get(col));

            if (log.isDebugEnabled())
                log.debug("Aggregate column statistic done [col=" + col.getName() + ", stat=" + stat + ']');

            colStats.put(col.getName(), stat);
        }

        ObjectStatisticsImpl tblStats = new ObjectStatisticsImpl(rowCnt, colStats);

        return tblStats;
    }

    /**
     * Get cache group context by specified statistics key.
     *
     * @param key Statistics key to get context by.
     * @return Cache group context for the given key.
     * @throws IgniteCheckedException If unable to find table by specified key.
     */
    public CacheGroupContext getGroupContext(StatisticsKeyMessage key) throws IgniteCheckedException {
        GridH2Table tbl = schemaMgr.dataTable(key.schema(), key.obj());
        if (tbl == null)
            throw new IgniteCheckedException(String.format("Can't find object %s.%s", key.schema(), key.obj()));

        return tbl.cacheContext().group();
    }

    /**
     * Return map cache group to corresponding stats keys.
     *
     * @param keys Statistics key to map.
     * @return Map of <group ids> to <collection of keys in groups>
     * @throws IgniteCheckedException In case of lack some of specified objects.
     */
    protected Map<CacheGroupContext, Collection<StatisticsKeyMessage>> mapToCacheGroups(
        Collection<StatisticsKeyMessage> keys
    ) throws IgniteCheckedException {
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> res = new HashMap<>(keys.size());
        for (StatisticsKeyMessage key : keys)
            res.computeIfAbsent(getGroupContext(key), k -> new ArrayList<>()).add(key);

        return res;
    }

    /**
     * Split specified keys to cache groups.
     *
     * @param keys Keys to split.
     * @return Map cache group to collection of keys in group.
     */
    public Map<CacheGroupContext, Collection<StatisticsKeyMessage>> splitByGroups(Collection<StatisticsKeyMessage> keys) {
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> res = new HashMap<>();

        for (StatisticsKeyMessage key : keys) {
            GridH2Table tbl = schemaMgr.dataTable(key.schema(), key.obj());
            CacheGroupContext grp = (tbl == null) ? null : tbl.cacheContext().group();

            res.computeIfAbsent(grp, k -> new ArrayList<>()).add(key);
        }

        return res;
    }

    /**
     * Return all nodes where specified cache group located.
     *
     * @param grp Cache group context to locate.
     * @return Set of node ids where group located.
     */
    public static Set<UUID> nodes(CacheGroupContext grp) {
        Set<UUID> res = new HashSet<>();
        AffinityTopologyVersion grpTopVer = grp.shared().exchange().readyAffinityVersion();
        List<List<ClusterNode>> assignments = grp.affinity().assignments(grpTopVer);

        for (List<ClusterNode> pnodes : assignments) {
            for (ClusterNode node : pnodes)
                res.add(node.id());
        }

        return res;
    }

    /**
     * Get map cluster node to it's partitions for the specified cache group.
     *
     * @param grp Cache group context to get partition information by.
     * @param partIds Partition to collect information by, if {@code null} - will collect map for all cache group partitions.
     * @param isPrimary if {@code true} - only primary partitions will be selected, if {@code false} - only backups.
     * @return Map nodeId to array of partitions, related to node.
     */
    public static Map<UUID, int[]> nodePartitions(
        CacheGroupContext grp,
        Collection<Integer> partIds,
        boolean isPrimary
    ) {
        AffinityTopologyVersion grpTopVer = grp.shared().exchange().readyAffinityVersion();
        List<List<ClusterNode>> assignments = grp.affinity().assignments(grpTopVer);

        if (partIds == null)
            partIds = IntStream.range(0, assignments.size()).boxed().collect(Collectors.toList());

        Map<UUID, List<Integer>> res = new HashMap<>();
        for (Integer partId : partIds) {
            assert partId < assignments.size();

            List<ClusterNode> partNodes = assignments.get(partId);
            if (F.isEmpty(partNodes))
                continue;

            if (isPrimary)
                res.computeIfAbsent(partNodes.get(0).id(), k -> new ArrayList<>()).add(partId);
            else {
                for (int i = 1; i < partNodes.size(); i++)
                    res.computeIfAbsent(partNodes.get(i).id(), k -> new ArrayList<>()).add(partId);
            }
        }

        return res.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                v -> v.getValue().stream().mapToInt(Integer::intValue).toArray()));
    }

    /**
     * Get all nodes where specified cache groups located.
     *
     * @param grps Cache groups.
     * @return Set of node ids.
     */
    public static Map<UUID, Collection<StatisticsKeyMessage>> nodeKeys(
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grps
    ) {
        Map<UUID, Collection<StatisticsKeyMessage>> res = new HashMap<>();

        AffinityTopologyVersion topVer = grps.keySet().iterator().next().shared().exchange().readyAffinityVersion();
        for (Map.Entry<CacheGroupContext, Collection<StatisticsKeyMessage>> grpKeys : grps.entrySet()) {
            List<List<ClusterNode>> assignments = grpKeys.getKey().affinity().assignments(topVer);

            for (List<ClusterNode> partNodes : assignments) {
                for (ClusterNode node : partNodes)
                    res.computeIfAbsent(node.id(), k -> new HashSet<>()).addAll(grpKeys.getValue());
            }
        }
        return res;
    }

    /**
     * Filter columns by specified names.
     *
     * @param cols Columns to filter.
     * @param colNames Column names.
     * @return Column with specified names.
     */
    public static Column[] filterColumns(Column[] cols, @Nullable Collection<String> colNames) {
        if (F.isEmpty(colNames)) {
            return Arrays.stream(cols)
                .filter(c -> c.getColumnId() >= QueryUtils.DEFAULT_COLUMNS_COUNT)
                .toArray(Column[]::new);
        }

        Set<String> colNamesSet = new HashSet<>(colNames);

        return Arrays.stream(cols).filter(c -> colNamesSet.contains(c.getName())).toArray(Column[]::new);
    }
}
