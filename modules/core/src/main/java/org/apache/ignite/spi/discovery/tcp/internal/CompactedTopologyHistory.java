package org.apache.ignite.spi.discovery.tcp.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyMap;

public class CompactedTopologyHistory {

    private final Map<Object, Data> historyByNode;

    public CompactedTopologyHistory(@NotNull Map<Long, Collection<ClusterNode>> topHist) {
        assert !topHist.isEmpty();

        long minVer = Long.MAX_VALUE;
        long maxVer = 0;

        for (long topVer : topHist.keySet()) {
            minVer = Math.min(minVer, topVer);
            maxVer = Math.max(maxVer, topVer);
        }

        long lowTopVer = minVer;
        long highTopVer = maxVer;

        historyByNode = new HashMap<>();

        Map<Object, Map<String, Object>> prevAttrsPerNode = new HashMap<>();

        for (Map.Entry<Long, Collection<ClusterNode>> e : topHist.entrySet()) {
            Long topVer = e.getKey();
            Collection<ClusterNode> top = e.getValue();

            for (ClusterNode node_ : top) {
                assert node_ instanceof TcpDiscoveryNode;

                TcpDiscoveryNode node = (TcpDiscoveryNode) node_;

                Map<String, Object> prevAttrs = prevAttrsPerNode.get(node.consistentId());

                Data data = historyByNode.computeIfAbsent(node.consistentId(), k -> new Data(lowTopVer, highTopVer));

                data.add(topVer, node, prevAttrs);

                prevAttrsPerNode.put(node.consistentId(), node.getAttributes());
            }
        }
    }

    public Map<Long, Collection<ClusterNode>> restore(){
        Map<Long, Collection<ClusterNode>> topHist = new TreeMap<>();

        for (Map.Entry<Object, Data> e : historyByNode.entrySet()) {
            Object consistentId = e.getKey();
            Data data = e.getValue();


            Map<String, Object> attrs = new HashMap<>();

            DataEntry[] entries = data.entries;
            for (int j = 0, entriesLength = entries.length; j < entriesLength; j++) {
                DataEntry entry = entries[j];
                long topVer = j + data.lowTopVer;

                if (entry == null)
                    continue;

                TcpDiscoveryNode node = entry.nodeWithoutAttrs;

                for (int i = 0; entry.attrKeys != null && i < entry.attrKeys.length; i++) {
                    String attrKey = entry.attrKeys[i];
                    Object attrVal = entry.attrVals[i];

                    attrs.put(attrKey, attrVal);
                }

                node.setAttributes(attrs);

                topHist.computeIfAbsent(topVer, k -> new ArrayList<>()).add(node);
            }
        }

        return topHist;
    }

    private static class Data {
        private final long lowTopVer;

        private final DataEntry[] entries;

        public Data(long lowTopVer, long highTopVer) {
            this.lowTopVer = lowTopVer;
            entries = new DataEntry[(int) (highTopVer - lowTopVer + 1)];
        }

        public void add(long topVer, TcpDiscoveryNode node, @Nullable Map<String, Object> prevAttrs) {
            Map<String, Object> attrs = node.getAttributes();

            node.setAttributes(emptyMap());

            List<String> deltaKeys = new ArrayList<>();
            List<Object> deltaVals = new ArrayList<>();

            for (Map.Entry<String, Object> e : attrs.entrySet()) {
                String key = e.getKey();
                Object val = e.getValue();

                boolean hasKey = prevAttrs != null && prevAttrs.containsKey(key);

                if (!hasKey || Objects.equals(prevAttrs.get(key), val)) {
                    deltaKeys.add(key);
                    deltaVals.add(val);
                }
            }

            DataEntry dataEntry = deltaKeys.isEmpty()
                    ? new DataEntry(node)
                    : new DataEntry(node, deltaKeys.toArray(new String[0]), deltaVals.toArray());

            entries[(int) (topVer - lowTopVer)] = dataEntry;
        }
    }

    private static class DataEntry {
        TcpDiscoveryNode nodeWithoutAttrs;
        String[] attrKeys;
        Object[] attrVals;

        public DataEntry(TcpDiscoveryNode nodeWithoutAttrs) {
            this.nodeWithoutAttrs = nodeWithoutAttrs;
        }

        public DataEntry(TcpDiscoveryNode nodeWithoutAttrs, String[] attrKeys, Object[] attrVals) {
            this.nodeWithoutAttrs = nodeWithoutAttrs;
            this.attrKeys = attrKeys;
            this.attrVals = attrVals;
        }
    }
}
