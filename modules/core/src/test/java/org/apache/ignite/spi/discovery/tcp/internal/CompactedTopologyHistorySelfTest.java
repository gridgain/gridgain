package org.apache.ignite.spi.discovery.tcp.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class CompactedTopologyHistorySelfTest {

    /** Typical topology history when the same nodes leave and rejoin cluster.*/
    Map<Long, Collection<ClusterNode>> simpleHistory;

    @Before
    public void setUp() {
        simpleHistory = new TreeMap<>();

        TcpDiscoveryNode n1 = newNode(
                UUID.randomUUID(),
                "n1",
                new T2<>("name", "node-1"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        TcpDiscoveryNode n2 = newNode(
                UUID.randomUUID(),
                "n2",
                new T2<>("name", "node-2"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        TcpDiscoveryNode n3 = newNode(
                UUID.randomUUID(),
                "n3",
                new T2<>("name", "node-3"),
                new T2<>("foo", "bar"),
                new T2<>("qwe", "xyz")
        );

        long lastTopVer = 1;

        simpleHistory.put(lastTopVer++, newArrayList(n1));
        simpleHistory.put(lastTopVer++, newArrayList(n1, n2));
        simpleHistory.put(lastTopVer++, newArrayList(n1, n2, n3));

        simpleHistory.put(lastTopVer++, newArrayList(n1, n2));
        simpleHistory.put(lastTopVer++, newArrayList(n1, n2, n3));

        simpleHistory.put(lastTopVer++, newArrayList(n2, n3));
        simpleHistory.put(lastTopVer++, newArrayList(n2));
        simpleHistory.put(lastTopVer++, newArrayList(n1, n2));
        simpleHistory.put(lastTopVer++, newArrayList(n1, n2, n3));
    }

    @SafeVarargs
    private final TcpDiscoveryNode newNode(UUID id, String consistentId, T2<String, Object>... attrs) {
        TcpDiscoveryNode node = new TcpDiscoveryNode(
                id,
                singletonList("127.0.0.1"),
                singletonList("localhost"),
                0,
                mock(DiscoveryMetricsProvider.class),
                IgniteVersionUtils.VER,
                consistentId
        );

        Map<String, Object> attrMap = new HashMap<>();

        for (T2<String, Object> attr : attrs) {
            attrMap.put(attr.getKey(), attr.getValue());
        }

        node.setAttributes(attrMap);

        return node;
    }

    @Test
    public void theTest() {
        CompactedTopologyHistory compactedTopologyHistory = new CompactedTopologyHistory(simpleHistory);

        Map<Long, Collection<ClusterNode>> restored = compactedTopologyHistory.restore();

        assertEquals(simpleHistory, restored);
    }
}