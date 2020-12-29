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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsClearRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsPropagationMessage;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.typedef.F;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Current statistics collections with methods to work with it's messages.
 */
public class IgniteStatisticsHelper {
    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     */
    public IgniteStatisticsHelper(SchemaManager schemaMgr) {
        this.schemaMgr = schemaMgr;
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
     * Extract groups of stats keys.
     *
     * @param keys Statistics key to extract groups from.
     * @return Map of <group ids></group> to <collection of keys in groups>
     * @throws IgniteCheckedException In case of lack some of specified objects.
     */
    protected Map<CacheGroupContext, Collection<StatisticsKeyMessage>> extractGroups(Collection<StatisticsKeyMessage> keys)
            throws IgniteCheckedException {
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> res = new HashMap<>(keys.size());
        for (StatisticsKeyMessage key : keys) {
            res.compute(getGroupContext(key), (k, v) -> {
                if (v == null)
                    v = new ArrayList<>();

                v.add(key);

                return v;
            });
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
        // На какой ноде на какой таблице какие партиции собирать
        List<List<ClusterNode>> assignments = grp.affinity().assignments(grpTopVer);

        assignments.forEach(pnodes -> pnodes.forEach(cn -> res.add(cn.id())));

        return res;
    }

    /**
     * Get map cluster node to it's partitions for the specified cache group.
     *
     * @param grp Cache group context to get partition information by.
     * @param partIds Partition to collect information by
     * @param isPrimary if {@code true} - only master partitions will be selected, if {@code false} - only backups.
     * @return Map nodeId to array of partitions, related to node.
     */
    public static Map<UUID, int[]> nodePartitions(
        CacheGroupContext grp,
        Collection<Integer> partIds,
        boolean isPrimary
    ) {
        AffinityTopologyVersion grpTopVer = grp.shared().exchange().readyAffinityVersion();
        // На какой ноде на какой таблице какие партиции собирать
        List<List<ClusterNode>> assignments = grp.affinity().assignments(grpTopVer);

        Map<UUID, List<Integer>> res = new HashMap<>();
        if (partIds == null)
            for (int i = 0; i < assignments.size(); i++)
                fillPartition(res, assignments, i, isPrimary);
        else
            for (Integer partId : partIds)
                fillPartition(res, assignments, partId, isPrimary);

        return res.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                v -> v.getValue().stream().mapToInt(Integer::intValue).toArray()));
    }

    /**
     * Fill map with master or backups node of specified partition.
     *
     * @param res Map to fill.
     * @param assignments Partitions assignments.
     * @param partId Partition to process.
     * @param isPrimary if {@code true} only primary nodes will be choosen, if {@code false} - only backups.
     */
    protected static  void fillPartition(
        Map<UUID, List<Integer>> res,
        List<List<ClusterNode>> assignments,
        int partId,
        boolean isPrimary
    ) {
        assert partId < assignments.size();
        List<ClusterNode> partNodes = assignments.get(partId);
        if (F.isEmpty(partNodes))
            return;

        if (isPrimary)
            res.compute(partNodes.get(0).id(), (k, v) -> {
                if (v == null)
                    v = new ArrayList<>();

                v.add(partId);

                return v;
            });
        else
            for (int i = 1; i < partNodes.size() - 1; i++)
                res.compute(partNodes.get(i).id(), (k, v) -> {
                    if (v == null)
                        v = new ArrayList<>();

                    v.add(partId);

                    return v;
                });
    }

    /**
     * Prepare statistics collection request for specified node.
     *
     * @param colId Collection id.
     * @param data Keys to partitions for current node.
     * @return Statistics collection request.
     */
    public static StatisticsGatheringRequest prepareRequest(UUID colId, Map<StatisticsKeyMessage, int[]> data) {
        UUID reqId = UUID.randomUUID();
        return new StatisticsGatheringRequest(colId, reqId, data);
    }

    // TODO add extraction of necessary partIds by statKeyMsg

    /**
     * Generate statistics collection requests by given keys.
     *
     * @param colId Collection id.
     * @param locNodeId Local node id.
     * @param failedPartitions Map of stat key to array of failed partitions to generate requests by.
     *            If {@code null} - requests will be
     * @return Collection of statistics collection addressed request.
     */
    public static Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> generateCollectionRequests(
        UUID colId,
        UUID locNodeId,
        Map<StatisticsKeyMessage, int[]> failedPartitions,
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpContexts
    ) {
        // NodeId to <Key to partitions on node> map
        Map<UUID, Map<StatisticsKeyMessage, int[]>> reqMap = new HashMap<>();
        for (Map.Entry<CacheGroupContext, Collection<StatisticsKeyMessage>> grpEntry : grpContexts.entrySet()) {
            // TODO: can we specify partitions here? from failed parts will it be effective?
            Map<UUID, int[]> reqNodes = nodePartitions(grpEntry.getKey(), null, true);

            for (Map.Entry<UUID, int[]> nodeParts : reqNodes.entrySet())
                reqMap.compute(nodeParts.getKey(), (k, v) -> {
                    if (v == null)
                        v = new HashMap<>();

                    Collection<StatisticsKeyMessage> grpKeys = grpContexts.get(grpEntry.getKey());

                    for (StatisticsKeyMessage key : grpKeys) {
                        int[] keyNodeParts = (failedPartitions == null) ? nodeParts.getValue() :
                                GridArrays.intersect(nodeParts.getValue(), failedPartitions.get(key));

                        if (keyNodeParts.length > 0)
                            v.put(key, keyNodeParts);
                    }

                    return v.isEmpty() ? null : v;
                });
        }

        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> reqs = new ArrayList<>();
        for (Map.Entry<UUID, Map<StatisticsKeyMessage, int[]>> nodeGpsParts: reqMap.entrySet()) {
            if (nodeGpsParts.getValue().isEmpty())
                continue;

            StatisticsGatheringRequest req = prepareRequest(colId, nodeGpsParts.getValue());
            reqs.add(new StatisticsAddrRequest<>(req, locNodeId, nodeGpsParts.getKey()));
        }

        return reqs;
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

        for (Map.Entry<CacheGroupContext, Collection<StatisticsKeyMessage>> grpKeys : grps.entrySet()) {
            CacheGroupContext grp = grpKeys.getKey();
            AffinityTopologyVersion grpTopVer = grp.shared().exchange().readyAffinityVersion();
            List<List<ClusterNode>> assignments = grp.affinity().assignments(grpTopVer);

            assignments.forEach(nodes -> nodes.forEach(clusterNode -> res.compute(clusterNode.id(), (k, v) -> {
                if (v == null)
                    v = new HashSet<>();

                v.addAll(grpKeys.getValue());

                return v;
            })));
        }
        return res;
    }

    /**
     * Generate statistics clear requests.
     *
     * @param locNodeId Local node id.
     * @param keys Keys to clean statistics by.
     * @return Collection of addressed statistics clear requests.
     * @throws IgniteCheckedException In case of errors.
     */
    public Collection<StatisticsAddrRequest<StatisticsClearRequest>> generateClearRequests(
        UUID locNodeId,
        Collection<StatisticsKeyMessage> keys
    ) throws IgniteCheckedException {
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpContexts = extractGroups(keys);
        Map<UUID, Collection<StatisticsKeyMessage>> nodeKeys = nodeKeys(grpContexts);

        return nodeKeys.entrySet().stream().map(node -> new StatisticsAddrRequest<>(
            new StatisticsClearRequest(UUID.randomUUID(), new ArrayList<>(node.getValue())), locNodeId, node.getKey()))
                .collect(Collectors.toList());
    }

    /**
     * Generate collection of statistics propagation messages to send collected partiton level statistics to backup
     * nodes. Can be called for single cache group and partition statistics only.
     *
     * @param locNodeId Local node id.
     * @param key Statistics key by which it was collected.
     * @param objStats Collection of object statistics (for the same partition).
     * @return Collection of propagation messages.
     */
    public Collection<StatisticsAddrRequest<StatisticsPropagationMessage>> generatePropagationMessages(
        UUID locNodeId,
        StatisticsKeyMessage key,
        Collection<ObjectPartitionStatisticsImpl> objStats
    ) throws IgniteCheckedException {
        List<StatisticsObjectData> objData = new ArrayList<>(objStats.size());
        objStats.forEach(ops -> {
            try {
                objData.add(StatisticsUtils.toObjectData(key, StatisticsType.PARTITION, ops));
            }
            catch (IgniteCheckedException e) {
                // TODO: log
            }
        });

        StatisticsPropagationMessage msg = new StatisticsPropagationMessage(objData);

        int partId = objStats.iterator().next().partId();
        CacheGroupContext grpCtx = getGroupContext(key);
        Map<UUID, int[]> nodePartitions = nodePartitions(grpCtx, Collections.singleton(partId), false);

        return nodePartitions.keySet().stream().map(nodeId -> new StatisticsAddrRequest(msg, locNodeId, nodeId))
            .collect(Collectors.toList());
    }

    public Collection<StatisticsAddrRequest<StatisticsPropagationMessage>> generateGlobalPropagationMessages(
        UUID locNodeId,
        Map<StatisticsKeyMessage, ObjectStatisticsImpl> objStats
    ) throws IgniteCheckedException {
        Map<StatisticsKeyMessage, StatisticsObjectData> objData = new HashMap<>(objStats.size());
        objStats.forEach((k, v) -> {
            try {
                objData.put(k, StatisticsUtils.toObjectData(k, StatisticsType.GLOBAL, v));
            }
            catch (IgniteCheckedException e) {
                // TODO: log
            }
        });
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpsKeys = extractGroups(objStats.keySet());
        Map<CacheGroupContext, Collection<StatisticsObjectData>> grpsData = new HashMap<>(grpsKeys.size());

        grpsKeys.forEach((gpr, keys) -> {
            Collection<StatisticsObjectData> stats = keys.stream().map(objData::get).collect(Collectors.toList());
            grpsData.put(gpr, stats);
        });

        Map<UUID, Collection<StatisticsObjectData>> reqMap = new HashMap<>();
        for (Map.Entry<CacheGroupContext, Collection<StatisticsObjectData>> grpKeys : grpsData.entrySet()) {
            Set<UUID> grpNodes = nodes(grpKeys.getKey());
            grpNodes.forEach(node -> reqMap.compute(node, (k, v) -> {
                if (v == null)
                    v = new HashSet<>();

                v.addAll(grpKeys.getValue());
                return v;
            }));
        }

        return reqMap.entrySet().stream().map(e -> new StatisticsAddrRequest(
            new StatisticsPropagationMessage(new ArrayList<>(e.getValue())), locNodeId, e.getKey()))
                .collect(Collectors.toList());
    }



    /**
     * Extract all partitions from specified statistics collection requests.
     *
     * @param reqs Failed request to extract partitions from.
     * @return Map StatisticsKeyMessage to List of corresponding partitions.
     */
    public static Map<StatisticsKeyMessage, int[]> extractFailed(StatisticsGatheringRequest[] reqs) {
        Map<StatisticsKeyMessage, List<Integer>> res = new HashMap<>();

        UUID colId = null;
        for (StatisticsGatheringRequest req : reqs) {

            assert colId == null || colId.equals(req.gatId());
            colId = req.gatId();

            for (Map.Entry<StatisticsKeyMessage, int[]> keyEntry : req.keys().entrySet()) {
                res.compute(keyEntry.getKey(), (k, v) -> {
                    if (v == null)
                        v = new ArrayList<>();

                    for (int i = 0; i < keyEntry.getValue().length; i++)
                        v.add(keyEntry.getValue()[i]);

                    return v;
                });
            }
        }

        return res.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                v -> v.getValue().stream().mapToInt(Integer::intValue).toArray()));
    }

    /**
     * Generate statistics collection requests by given keys.
     *
     * @param gatId Gathering id.
     * @param locNodeId Local node id.
     * @param keys Collection of keys to collect statistics by.
     * @return Collection of statistics collection addressed request.
     * @throws IgniteCheckedException In case of errors.
     */
    protected Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> generateCollectionRequests(
            UUID gatId,
            UUID locNodeId,
            Collection<StatisticsKeyMessage> keys,
            Map<StatisticsKeyMessage, int[]> failedPartitions
    ) throws IgniteCheckedException {
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpContexts = extractGroups(keys);

        return generateCollectionRequests(gatId, locNodeId, failedPartitions, grpContexts);
    }

    /**
     * Filter columns from specified statistics.
     *
     * @param stat Statistics to filter columns from.
     * @param cols Column names to return in result object.
     * @return Statistics with only specified columns.
     */
    public static ObjectStatisticsImpl filterColumns(ObjectStatisticsImpl stat, Collection<String> cols) {
        ObjectStatisticsImpl res = stat.clone();
        res.columnsStatistics().clear();
        cols.forEach(col -> {
            ColumnStatistics colStat = stat.columnStatistics(col);
            if (colStat != null)
                res.columnsStatistics().put(col, colStat);
        });

        return res;
    }

    /**
     * Get failed partitions map from request and its response.
     *
     * @param req Request to get the original requested partitions from.
     * @param resp Response to get actually collected partitions.
     * @return Map of not collected partitions.
     */
    public static Map<StatisticsKeyMessage, int[]> extractFailed(StatisticsGatheringRequest req, StatisticsGatheringResponse resp) {
        assert req.gatId().equals(resp.gatId());

        Map<StatisticsKeyMessage, int[]> collected = new HashMap<>(resp.data().size());
        for (Map.Entry<StatisticsObjectData, int[]> data : resp.data().entrySet())
            collected.put(data.getKey().key(), data.getValue());

        Map<StatisticsKeyMessage, int[]> res = new HashMap<>();
        for (Map.Entry<StatisticsKeyMessage, int[]> keyEntry : req.keys().entrySet()) {
            int[] failed = GridArrays.subtract(keyEntry.getValue(), collected.get(keyEntry.getKey()));

            if (failed.length > 0)
                res.put(keyEntry.getKey(), failed);
        }
        return res;
    }
}
