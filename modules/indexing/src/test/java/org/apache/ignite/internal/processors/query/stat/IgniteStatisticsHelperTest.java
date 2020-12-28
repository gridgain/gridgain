package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * IgniteStatisticsRequestCollection unit tests.
 */
public class IgniteStatisticsHelperTest extends GridCommonAbstractTest {
    /** */
    private static final UUID node1 = UUID.randomUUID();

    /** */
    private static final UUID node2 = UUID.randomUUID();

    /** */
    private static final UUID c1 = UUID.randomUUID();

    /** */
    private static final UUID c2 = UUID.randomUUID();

    /** */
    private static final UUID r1_1 = UUID.randomUUID();

    /** */
    private static final UUID r1_2 = UUID.randomUUID();

    /** */
    private static final UUID r1_3 = UUID.randomUUID();

    /** */
    private static final UUID r2_1 = UUID.randomUUID();

    /** */
    private static final StatisticsKeyMessage k1 = keyMsg(1);

    /** */
    private static final StatisticsKeyMessage k2 = keyMsg(2, 1, 2);

    /** */
    private static final StatisticsKeyMessage k3 = keyMsg(3, 1, 2);

    /** */
    private static final Map<UUID, int[]> nodeParts;

    /** */
    private static final CacheGroupContext cgc1;

    /** */
    private static final CacheGroupContext cgc2;

    /** */
    private static final CacheGroupContext cgc3;

    static {
        nodeParts = new HashMap<>();
        int[] node1parts = new int[]{0, 2, 4, 6};
        int[] node2parts = new int[]{1, 3, 5, 7};
        nodeParts.put(node1, node1parts);
        nodeParts.put(node2, node2parts);

        cgc1 = cgc(nodeParts, 0);
        cgc2 = cgc(nodeParts, 3);
        cgc3 = cgc(nodeParts, 0);
    }

    /**
     * Extract failed keys to partitions map for single failed request.
     */
    @Test
    public void testExtractFailedOne() {
        StatisticsGatheringRequest r1 = new StatisticsGatheringRequest(c1, r1_1, new HashMap<>());
        StatisticsKeyMessage k1 = keyMsg(1, 1, 2, 3);
        r1.keys().put(k1, new int[]{100, 200, 300});

        Map<StatisticsKeyMessage, int[]> extracted = IgniteStatisticsHelper.extractFailed(
                new StatisticsGatheringRequest[]{r1});

        assertEquals(1, extracted.size());
        assertTrue(extracted.containsKey(k1));
        assertEquals(3, extracted.get(k1).length);
    }

    /**
     * Extract failed keys to partitions map for multiple failed request.
     */
    @Test
    public void testExtractFailed() {
        StatisticsGatheringRequest r1 = new StatisticsGatheringRequest(c1, r1_1, new HashMap<>());
        StatisticsKeyMessage k1 = keyMsg(1, 1, 2, 3);
        r1.keys().put(k1, new int[]{100, 200, 300});
        StatisticsKeyMessage k2 = keyMsg(2, 2, 3, 4);
        r1.keys().put(k2, new int[]{100, 200, 300});

        StatisticsGatheringRequest r2 = new StatisticsGatheringRequest(c1, r1_2, new HashMap<>());
        r2.keys().put(k1, new int[]{400});

        StatisticsGatheringRequest r3 = new StatisticsGatheringRequest(c1, r1_3, new HashMap<>());
        r3.keys().put(k2, new int[]{500, 600, 800});

        Map<StatisticsKeyMessage, int[]> extracted = IgniteStatisticsHelper.extractFailed(
                new StatisticsGatheringRequest[]{r1, r2, r3});

        assertEquals(2, extracted.size());
        assertTrue(extracted.containsKey(k1));
        assertEquals(4, extracted.get(k1).length);

        assertTrue(extracted.containsKey(k2));
        assertEquals(6, extracted.get(k2).length);
    }

    /**
     * Test extraction failed of different collections requests.
     */
    @Test(expected = AssertionError.class)
    public void testExtractDifferentCollections() {
        StatisticsGatheringRequest r1 = new StatisticsGatheringRequest(c1, r1_1, new HashMap<>());
        StatisticsKeyMessage k1 = keyMsg(1, 1, 2, 3);
        r1.keys().put(k1, new int[]{1});

        StatisticsGatheringRequest r2 = new StatisticsGatheringRequest(c2, r2_1, new HashMap<>());
        r1.keys().put(k1, new int[]{2});

        Map<StatisticsKeyMessage, int[]> extracted = IgniteStatisticsHelper.extractFailed(
                new StatisticsGatheringRequest[]{r1, r2});
    }

    /**
     * Generate test key message by some key numbers.
     *
     * @param keyId Object "id".
     * @param subId Columns "ids".
     * @return Generated test statistics key message.
     */
    private static StatisticsKeyMessage keyMsg(int keyId, int... subId) {
        List<String> cols = (subId == null) ? null : Arrays.stream(subId).boxed().map(s -> ("COL" + s))
                .collect(Collectors.toList());
        return new StatisticsKeyMessage("SCHEMA", "OBJECT" + keyId, cols);
    }

    /**
     * Simple test to generate statistics collection requests for single key on single node.
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testGenerateCollectionRequestsSingle() throws IgniteCheckedException {
        UUID colId = UUID.randomUUID();
        StatisticsKeyMessage k1 = keyMsg(1);
        CacheGroupContext cgc1 = cgc(Collections.singletonMap(node1, new int[]{0, 1, 2, 3}), 0);
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpContexts =
            Collections.singletonMap(cgc1, Collections.singletonList(k1));


        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> reqs = IgniteStatisticsHelper
                .generateCollectionRequests(colId, Collections.singletonList(k1), null, grpContexts);

        assertEquals(1, reqs.size());

        StatisticsAddrRequest<StatisticsGatheringRequest> req = reqs.stream().filter(r -> node1.equals(r.targetNodeId()))
                .findAny().orElse(null);

        assertNotNull(req);
        assertTrue(Arrays.equals(new int[]{0, 1, 2, 3}, req.req().keys().get(k1)));
    }

    /**
     * Test generateCollectionRequests for two keys and two nodes and check that generated requests contains all
     * required keys and partitions.
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testGenerateCollectionRequests() throws IgniteCheckedException {
        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> reqs = testGenerateCollectionRequests(null);

        assertEquals(2, reqs.size());

        StatisticsAddrRequest<StatisticsGatheringRequest> req1 = reqs.stream().filter(req -> node1.equals(req.targetNodeId()))
                .findAny().orElse(null);
        StatisticsAddrRequest<StatisticsGatheringRequest> req2 = reqs.stream().filter(req -> node2.equals(req.targetNodeId()))
                .findAny().orElse(null);
        assertNotNull(req1);
        assertNotNull(req2);
        assertTrue(reqs.stream().allMatch(req -> req.req().keys().size() == 2));
        assertTrue(Arrays.equals(nodeParts.get(node1), req1.req().keys().get(k1)));
        assertTrue(Arrays.equals(nodeParts.get(node1), req1.req().keys().get(k2)));
        assertTrue(Arrays.equals(nodeParts.get(node2), req2.req().keys().get(k1)));
        assertTrue(Arrays.equals(nodeParts.get(node2), req2.req().keys().get(k2)));
    }

    /**
     * Test generateCollectionRequests for two keys and two nodes with only one failed partition and check that
     * generated requests contains request to regenerate only that one partition.
     */
    @Test
    public void testGenerateCollectionRequestsFailedPartitions() throws IgniteCheckedException {
        Map<StatisticsKeyMessage, int[]> failedPartitions = Collections.singletonMap(k1, new int[]{1});

        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> reqs = testGenerateCollectionRequests(failedPartitions);

        assertEquals(1, reqs.size());

        StatisticsAddrRequest<StatisticsGatheringRequest> req = reqs.stream().filter(r -> node2.equals(r.targetNodeId())).findAny()
                .orElse(null);

        assertNotNull(req);

        int[] k1parts = req.req().keys().get(k1);

        assertTrue(Arrays.equals(new int[]{1}, k1parts));
    }

    /**
     * Prepare and generate statistics collection requests for two keys and two nodes.
     *
     * @param failedPartitions Map of failed partitions to generate requests only by its.
     * @return Collection of statistics collection requests.
     * @throws IgniteCheckedException In case of errors.
     */
    private Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> testGenerateCollectionRequests(
        Map<StatisticsKeyMessage, int[]> failedPartitions
    ) throws IgniteCheckedException {
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpContexts = new HashMap<>();
        grpContexts.put(cgc1, Collections.singletonList(k1));
        grpContexts.put(cgc2, Collections.singletonList(k2));

        List<StatisticsKeyMessage> keys = new ArrayList<>();
        keys.add(k1);
        keys.add(k2);

        return IgniteStatisticsHelper.generateCollectionRequests(
                c1, keys, failedPartitions, grpContexts);
    }

    /**
     * Test node partitions with and without backups.
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testNodePartitions() throws IgniteCheckedException {
        // Without backups
        testNodePartitionsInt(0);

        // With 2 backups
        testNodePartitionsInt(3);
    }

    /**
     * Test node partitions with specified number of backups.
     *
     * @param backups Number of backups.
     * @throws IgniteCheckedException In case of exceptions.
     */
    private void testNodePartitionsInt(int backups) throws IgniteCheckedException {
        // 1 node not all partitions
        CacheGroupContext cgc = cgc(Collections.singletonMap(node1, new int[]{0, 1, 4}), backups);
        Map<UUID, int[]> calcNodeParts = IgniteStatisticsHelper.nodePartitions(cgc, null);

        assertEquals(1, calcNodeParts.size());


        // 1 node
        CacheGroupContext cgc2 = cgc(Collections.singletonMap(node1, new int[]{0, 1, 2, 3, 4}), backups);
        Map<UUID, int[]> calcNodeParts2 = IgniteStatisticsHelper.nodePartitions(cgc2, null);

        assertEquals(1, calcNodeParts.size());

        // 2 nodes
        Map<UUID, int[]> nodeParts = new HashMap<>();
        nodeParts.put(node1, new int[]{1, 2, 3});
        nodeParts.put(node2, new int[]{0, 4});
        CacheGroupContext cgc3 = cgc(nodeParts, 0);
        Map<UUID, int[]> calcNodeParts3 = IgniteStatisticsHelper.nodePartitions(cgc3, null);

        assertEquals(2, calcNodeParts3.size());
    }

    /**
     * Create mock for cache group context with given partition assignment.
     *
     * @param assignment Cache group partition assignment.
     * @param backups Number of backups (random node ids).
     * @return Cache group context.
     */
    private static CacheGroupContext cgc(Map<UUID, int[]> assignment, int backups) {
        AffinityTopologyVersion topVer = Mockito.mock(AffinityTopologyVersion.class);

        GridCachePartitionExchangeManager exMgr = Mockito.mock(GridCachePartitionExchangeManager.class);
        Mockito.when(exMgr.readyAffinityVersion()).thenReturn(topVer);

        GridCacheSharedContext csCtxt = Mockito.mock(GridCacheSharedContext.class);
        Mockito.when(csCtxt.exchange()).thenReturn(exMgr);

        CacheGroupContext cgc = Mockito.mock(CacheGroupContext.class);
        Mockito.when(cgc.shared()).thenReturn(csCtxt);

        GridAffinityAssignmentCache aaCache = Mockito.mock(GridAffinityAssignmentCache.class);
        List<List<ClusterNode>> partAssignment = makeAssignment(assignment, backups);
        Mockito.when(aaCache.assignments(Mockito.any(AffinityTopologyVersion.class))).thenReturn(partAssignment);

        Mockito.when(cgc.affinity()).thenReturn(aaCache);
        return cgc;
    }

    /**
     * Convert map nodeId to primary partitions to assignments lists.
     *
     * @param assignment Assignments as node partitions map.
     * @param backups Number of backups (random node ids).
     * @return Assignments as list of lists.
     */
    private static List<List<ClusterNode>> makeAssignment(Map<UUID, int[]> assignment, int backups) {
        int partCnt = assignment.values().stream().map(arr -> Arrays.stream(arr).max().getAsInt())
                .max(Integer::compareTo).orElse(null) + 1;
        List<List<ClusterNode>> partAssignment = Arrays.asList(new List[partCnt]);

        for (Map.Entry<UUID, int[]> nodeParts : assignment.entrySet()) {
            for (int partId : nodeParts.getValue()) {
                assert partAssignment.get(partId) == null;

                List<ClusterNode> partNodes = new ArrayList<>(backups + 1);
                partNodes.add(new GridTestNode(nodeParts.getKey()));
                for (int i = 0; i < backups; i++)
                    partNodes.add(new GridTestNode(UUID.randomUUID()));

                partAssignment.set(partId, partNodes);
            }
        }

        return partAssignment;
    }

    /**
     * Test failed partition extraction by response and request:
     *
     * 1) All keys collected.
     * 2) Some partition lack.
     * 3) Some keys lack.
     * 4) Some partition and keys lack.
     * 5) All keys lack.
     */
    @Test
    public void testExtractFailedByResponse() {
        //UUID colId, UUID reqId, Map<StatsKeyMessage, int[]> keys
        Map<StatisticsKeyMessage, int[]> keys = new HashMap<>();
        keys.put(k1, new int[]{1, 2, 3});
        keys.put(k2, new int[]{1, 2, 3});

        StatisticsGatheringRequest req = new StatisticsGatheringRequest(c1, r1_1, keys);

        // 1) All keys collected
        StatisticsObjectData sod1 = new StatisticsObjectData(k1, 0, StatisticsType.LOCAL, 0, 0, Collections.emptyMap());
        StatisticsObjectData sod2 = new StatisticsObjectData(k2, 0, StatisticsType.LOCAL, 0, 0, Collections.emptyMap());
        Map<StatisticsObjectData, int[]> collectedKeys1 = new HashMap<>();
        collectedKeys1.put(sod1, new int[]{1, 2, 3});
        collectedKeys1.put(sod2, new int[]{1, 2, 3});

        StatisticsGatheringResponse resp1 = new StatisticsGatheringResponse(c1, r1_1, collectedKeys1);

        Map<StatisticsKeyMessage, int[]> failed1 = IgniteStatisticsHelper.extractFailed(req, resp1);

        assertTrue(failed1.isEmpty());

        // 2) Some partition lack.
        Map<StatisticsObjectData, int[]> collectedKeys2 = new HashMap<>(collectedKeys1);
        collectedKeys2.put(sod1, new int[]{1, 3});
        StatisticsGatheringResponse resp2 = new StatisticsGatheringResponse(c1, r1_2, collectedKeys2);


        Map<StatisticsKeyMessage, int[]> failed2 = IgniteStatisticsHelper.extractFailed(req, resp2);

        assertTrue(Arrays.equals(failed2.get(k1), new int[]{2}));

        // 3) Some keys lack.
        Map<StatisticsObjectData, int[]> collectedKeys3 = new HashMap<>(collectedKeys1);
        collectedKeys3.put(sod2, new int[0]);
        StatisticsGatheringResponse resp3 = new StatisticsGatheringResponse(c1, r1_3, collectedKeys3);


        Map<StatisticsKeyMessage, int[]> failed3 = IgniteStatisticsHelper.extractFailed(req, resp3);

        assertTrue(Arrays.equals(failed3.get(k2), new int[]{1, 2, 3}));

        // 4) Some partition and keys lack.
        Map<StatisticsObjectData, int[]> collectedKeys4 = new HashMap<>();
        collectedKeys4.put(sod1, new int[]{1, 2});
        collectedKeys4.put(sod2, new int[0]);
        StatisticsGatheringResponse resp4 = new StatisticsGatheringResponse(c1, r1_1, collectedKeys4);


        Map<StatisticsKeyMessage, int[]> failed4 = IgniteStatisticsHelper.extractFailed(req, resp4);

        assertTrue(Arrays.equals(failed4.get(k1), new int[]{3}));
        assertTrue(Arrays.equals(failed4.get(k2), new int[]{1, 2, 3}));


        // 5) All keys lack.
        Map<StatisticsObjectData, int[]> collectedKeys5 = new HashMap<>();
        collectedKeys5.put(sod1, new int[0]);
        collectedKeys5.put(sod2, new int[0]);
        StatisticsGatheringResponse resp5 = new StatisticsGatheringResponse(c1, r1_2, collectedKeys5);


        Map<StatisticsKeyMessage, int[]> failed5 = IgniteStatisticsHelper.extractFailed(req, resp5);

        assertTrue(Arrays.equals(failed5.get(k1), new int[]{1, 2, 3}));
        assertTrue(Arrays.equals(failed5.get(k2), new int[]{1, 2, 3}));
    }

    /**
     * Test groups keys to node distribution:
     *
     * 1) Single group with single key to two nodes -> 2 req with 2 keys.
     * 2) Single group with two keys to two node -> 2 req with 2 keys.
     * 3) Two groups with single key to two separate nodes -> 2 req with differs key.
     */
    @Test
    public void nodesTest() {
        // 1) Single group with single key to two nodes.
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpKeys1 = Collections.singletonMap(cgc1, Collections.singleton(k1));

        Map<UUID, Collection<StatisticsKeyMessage>> nodeKeys1 = IgniteStatisticsHelper.nodeKeys(grpKeys1);

        assertEquals(2, nodeKeys1.size());
        assertTrue(nodeKeys1.get(node1).contains(k1));
        assertTrue(nodeKeys1.get(node2).contains(k1));

        // 2) Single group with two keys to two node.
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpKeys2 = new HashMap<>();
        grpKeys2.put(cgc1, Collections.singleton(k1));
        grpKeys2.put(cgc3, Arrays.asList(k2, k3));

        Map<UUID, Collection<StatisticsKeyMessage>> nodeKeys2 = IgniteStatisticsHelper.nodeKeys(grpKeys2);

        assertEquals(2, nodeKeys2.size());
        assertEquals(3, nodeKeys2.get(node1).size());
        assertEquals(3, nodeKeys2.get(node2).size());

        // 3) Two groups with single key to two separate nodes.
        Map<UUID, int[]> nodeParts1 = Collections.singletonMap(node1, new int[]{0, 1, 2, 3});
        Map<UUID, int[]> nodeParts2 = Collections.singletonMap(node2, new int[]{0, 1, 2, 3});
        CacheGroupContext cgc1d = cgc(nodeParts1, 0);
        CacheGroupContext cgc2d = cgc(nodeParts2, 0);
        Map<CacheGroupContext, Collection<StatisticsKeyMessage>> grpKeys3 = new HashMap<>();
        grpKeys3.put(cgc1d, Collections.singleton(k1));
        grpKeys3.put(cgc2d, Collections.singleton(k2));

        Map<UUID, Collection<StatisticsKeyMessage>> nodeKeys3 = IgniteStatisticsHelper.nodeKeys(grpKeys3);

        assertEquals(2, nodeKeys3.size());
        assertEquals(1, nodeKeys3.get(node1).size());
        assertTrue(nodeKeys3.get(node1).contains(k1));
        assertEquals(1, nodeKeys3.get(node2).size());
        assertTrue(nodeKeys3.get(node2).contains(k2));
    }
}
