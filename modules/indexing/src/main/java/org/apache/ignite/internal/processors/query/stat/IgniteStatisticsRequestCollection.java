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
import org.apache.ignite.internal.processors.query.stat.messages.StatsClearRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatsCollectionResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatsObjectData;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.typedef.F;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Current statistics collections with methods to work with it's messages.
 */
public class IgniteStatisticsRequestCollection {
    /** Current collections, collection id to collection status map. */
    private final Map<UUID, StatCollectionStatus> currColls = new ConcurrentHashMap<>();

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     */
    public IgniteStatisticsRequestCollection(SchemaManager schemaMgr) {
        this.schemaMgr = schemaMgr;
    }

    /**
     * Add new statistics collection status.
     *
     * @param id Id to save status by.
     * @param status status to add.
     */
    public void addActiveCollectionStatus(UUID id, StatCollectionStatus status) {
        currColls.put(id, status);
    }

    /**
     * Update status of statistics gathering task.
     *
     * @param id Statistics collection task id.
     * @param transformation Transformation to apply, if it returns {@code null} - status will be removed.
     */
    public void updateCollection(UUID id, Function<StatCollectionStatus, StatCollectionStatus> transformation) {
        currColls.compute(id, (k, v) -> transformation.apply(v));
    }

    /**
     * Get collection status.
     *
     * @param id Id to get status by.
     * @return Collection status.
     */
    public StatCollectionStatus getCollection(UUID id) {
        return currColls.get(id);
    }


    /**
     * Extract groups of stats keys.
     *
     * @param keys Statistics key to extract groups from.
     * @return Map of <group ids></group> to <collection of keys in groups>
     * @throws IgniteCheckedException In case of lack some of specified objects.
     */
    protected Map<CacheGroupContext, Collection<StatsKeyMessage>> extractGroups(Collection<StatsKeyMessage> keys)
            throws IgniteCheckedException {
        Map<CacheGroupContext, Collection<StatsKeyMessage>> res = new HashMap<>(keys.size());
        for (StatsKeyMessage key : keys) {
            GridH2Table tbl = schemaMgr.dataTable(key.schema(), key.obj());
            if (tbl == null)
                throw new IgniteCheckedException(String.format("Can't find object %s.%s", key.schema(), key.obj()));

            res.compute(tbl.cacheContext().group(), (k, v) -> {
                if (v == null)
                    v = new ArrayList<>();

                v.add(key);

                return v;
            });
        }
        return res;
    }

    /**
     * Get map cluster node to it's partitions for the specified cache group.
     *
     * @param grp Cache group context to get partition information by.
     * @param partIds Partition to collect information by
     * @return Map nodeId to array of partitions, related to node.
     */
    public static Map<UUID, int[]> nodePartitions(CacheGroupContext grp, Collection<Integer> partIds) {
        AffinityTopologyVersion grpTopVer = grp.shared().exchange().readyAffinityVersion();
        // На какой ноде на какой таблице какие партиции собирать
        List<List<ClusterNode>> assignments = grp.affinity().assignments(grpTopVer);

        Map<UUID, List<Integer>> res = new HashMap<>();
        if (partIds == null)
            for (int i = 0; i < assignments.size(); i++)
                fillPartitionMaster(res, assignments, i);
        else
            for (Integer partId : partIds)
                fillPartitionMaster(res, assignments, partId);

        return res.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                v -> v.getValue().stream().mapToInt(Integer::intValue).toArray()));
    }

    /**
     * Fill map with master node of specified partition.
     *
     * @param res Map to fill.
     * @param assignments Partitions assignments.
     * @param partId Partition to process.
     */
    protected static  void fillPartitionMaster(
        Map<UUID, List<Integer>> res,
        List<List<ClusterNode>> assignments,
        int partId
    ) {
        assert partId < assignments.size();
        List<ClusterNode> partNodes = assignments.get(partId);
        if (F.isEmpty(partNodes))
            return;

        res.compute(partNodes.get(0).id(), (k, v) -> {
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
    public static StatsCollectionRequest prepareRequest(UUID colId, Map<StatsKeyMessage, int[]> data) {
        UUID reqId = UUID.randomUUID();
        return new StatsCollectionRequest(colId, reqId, data);
    }

    // TODO add extraction of necessary partIds by statKeyMsg

    /**
     * Generate statistics collection requests by given keys.
     *
     * @param colId Collection id.
     * @param keys Collection of keys to collect statistics by.
     * @param failedPartitions Map of stat key to array of failed partitions to generate requests by.
     *            If {@code null} - requests will be
     * @return Collection of statistics collection addressed request.
     * @throws IgniteCheckedException In case of errors.
     */
    public static Collection<StatsAddrRequest<StatsCollectionRequest>> generateCollectionRequests(
        UUID colId,
        Collection<StatsKeyMessage> keys,
        Map<StatsKeyMessage, int[]> failedPartitions,
        Map<CacheGroupContext, Collection<StatsKeyMessage>> grpContexts
    ) {

        // NodeId to <Key to partitions on node> map
        Map<UUID, Map<StatsKeyMessage, int[]>> reqMap = new HashMap<>();
        for (Map.Entry<CacheGroupContext, Collection<StatsKeyMessage>> grpEntry : grpContexts.entrySet()) {
            Map<UUID, int[]> reqNodes = nodePartitions(grpEntry.getKey(), null); // TODO Null?
            for (Map.Entry<UUID, int[]> nodeParts : reqNodes.entrySet())
                reqMap.compute(nodeParts.getKey(), (k, v) -> {
                    if (v == null)
                        v = new HashMap<>();

                    Collection<StatsKeyMessage> grpKeys = grpContexts.get(grpEntry.getKey());

                    for (StatsKeyMessage key : grpKeys) {
                        int[] keyNodeParts = (failedPartitions == null) ? nodeParts.getValue() :
                                GridArrays.intersect(nodeParts.getValue(), failedPartitions.get(key));

                        if (keyNodeParts.length > 0)
                            v.put(key, keyNodeParts);
                    }

                    return v.isEmpty() ? null : v;
                });
        }

        Collection<StatsAddrRequest<StatsCollectionRequest>> reqs = new ArrayList<>();
        for (Map.Entry<UUID, Map<StatsKeyMessage, int[]>> nodeGpsParts: reqMap.entrySet()) {
            if (nodeGpsParts.getValue().isEmpty())
                continue;

            StatsCollectionRequest req = prepareRequest(colId, nodeGpsParts.getValue());
            reqs.add(new StatsAddrRequest<>(req, nodeGpsParts.getKey()));
        }

        return reqs;
    }

    /**
     * Get all nodes where specified cache grous located.
     *
     * @param grps Cache groups.
     * @return Set of node ids.
     */
    public static Map<UUID, Collection<StatsKeyMessage>> nodeKeys(Map<CacheGroupContext, Collection<StatsKeyMessage>> grps) {
        Map<UUID, Collection<StatsKeyMessage>> res = new HashMap<>();

        for (Map.Entry<CacheGroupContext, Collection<StatsKeyMessage>> grpKeys : grps.entrySet()) {
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
     * @param keys Keys to clean statistics by.
     * @return Collection of addressed statistics clear requests.
     * @throws IgniteCheckedException In case of errors.
     */
    public Collection<StatsAddrRequest<StatsClearRequest>> generateClearRequests(
        Collection<StatsKeyMessage> keys
    ) throws IgniteCheckedException {
        Map<CacheGroupContext, Collection<StatsKeyMessage>> grpContexts = extractGroups(keys);
        Map<UUID, Collection<StatsKeyMessage>> nodeKeys = nodeKeys(grpContexts);

        return nodeKeys.entrySet().stream().map(node -> new StatsAddrRequest<>(
            new StatsClearRequest(UUID.randomUUID(), new ArrayList<>(node.getValue())), node.getKey()))
                .collect(Collectors.toList());
    }


    /**
     * Generate statistics collection requests by given keys.
     *
     * @param colId Collection id.
     * @param keys Collection of keys to collect statistics by.
     * @return Collection of statistics collection addressed request.
     * @throws IgniteCheckedException In case of errors.
     */
    protected Collection<StatsAddrRequest<StatsCollectionRequest>> generateCollectionRequests(
        UUID colId,
        Collection<StatsKeyMessage> keys,
        Map<StatsKeyMessage, int[]> failedPartitions
    ) throws IgniteCheckedException {
        Map<CacheGroupContext, Collection<StatsKeyMessage>> grpContexts = extractGroups(keys);

        return generateCollectionRequests(colId, keys, failedPartitions, grpContexts);
    }

    /**
     * Extract all partitions from specified statistics collection requests.
     *
     * @param reqs Failed request to extract partitions from.
     * @return Map StatisticsKeyMessage to List of corresponding partitions.
     */
    public static Map<StatsKeyMessage, int[]> extractFailed(StatsCollectionRequest[] reqs) {
        Map<StatsKeyMessage, List<Integer>> res = new HashMap<>();

        UUID colId = null;
        for (StatsCollectionRequest req : reqs) {

            assert colId == null || colId.equals(req.colId());
            colId = req.colId();

            for (Map.Entry<StatsKeyMessage, int[]> keyEntry : req.keys().entrySet()) {
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
     * Get failed partitions map from request and its response.
     *
     * @param req Request to get the original requested partitions from.
     * @param resp Response to get actually collected partitions.
     * @return Map of not collected partitions.
     */
    public static Map<StatsKeyMessage, int[]> extractFailed(StatsCollectionRequest req, StatsCollectionResponse resp) {
        assert req.colId().equals(resp.colId());

        Map<StatsKeyMessage, int[]> collected = new HashMap<>(resp.data().size());
        for (Map.Entry<StatsObjectData, int[]> data : resp.data().entrySet())
            collected.put(data.getKey().key(), data.getValue());

        Map<StatsKeyMessage, int[]> res = new HashMap<>();
        for (Map.Entry<StatsKeyMessage, int[]> keyEntry : req.keys().entrySet()) {
            int[] failed = GridArrays.subtract(keyEntry.getValue(), collected.get(keyEntry.getKey()));

            if (failed.length > 0)
                res.put(keyEntry.getKey(), failed);
        }
        return res;
    }

    /**
     * Apply specified transformation to each active statistics collection status.
     *
     * @param transformation Transformation to apply.
     */
    public void updateAllCollections(Function<StatCollectionStatus, StatCollectionStatus> transformation) {
        currColls.keySet().forEach(k -> {
            currColls.computeIfPresent(k, (k1, v) -> transformation.apply(v)); // TODO sync?
        });
    }
}
