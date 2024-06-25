package org.apache.ignite.spi.discovery.tcp.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.Extension;
import org.apache.ignite.plugin.segmentation.SegmentationResolver;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyMap;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;

public class CompactedTopologyHistory implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final Map<Serializable, Data> historyByNode;

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

                assert node.consistentId() instanceof Serializable;

                Serializable consistentId = (Serializable) node.consistentId();

                Map<String, Object> prevAttrs = prevAttrsPerNode.get(consistentId);

                Data data = historyByNode.computeIfAbsent(consistentId, k -> new Data(lowTopVer, highTopVer));

                data.add(topVer, node, prevAttrs);

                prevAttrsPerNode.put(consistentId, node.getAttributes());
            }
        }
    }

    public Map<Long, Collection<ClusterNode>> restore() {
        Map<Long, Collection<ClusterNode>> topHist = new TreeMap<>();

        for (Map.Entry<Serializable, Data> e : historyByNode.entrySet()) {
            Serializable consistentId = e.getKey();
            Data data = e.getValue();

            Map<String, Object> attrs = new HashMap<>();

            DataEntry[] entries = data.entries;
            for (int j = 0, entriesLength = entries.length; j < entriesLength; j++) {
                DataEntry entry = entries[j];
                long topVer = j + data.lowTopVer;

                if (entry == null)
                    continue;

                TcpDiscoveryNode node = new TcpDiscoveryNode(
                        entry.id,
                        entry.addrs,
                        entry.hostNames,
                        entry.discPort,
                        new DiscoveryMetricsProvider() {
                            @Override
                            public ClusterMetrics metrics() {
                                return entry.metrics;
                            }

                            @Override
                            public Map<Integer, CacheMetrics> cacheMetrics() {
                                return null;
                            }
                        },
                        entry.ver,
                        consistentId
                );

                for (int i = 0; entry.attrKeys != null && i < entry.attrKeys.length; i++) {
                    String attrKey = (String) entry.attrKeys[i];
                    Object attrVal = entry.attrVals[i];

                    attrs.put(attrKey, attrVal);
                }

                node.setAttributes(attrs);

                topHist.computeIfAbsent(topVer, k -> new ArrayList<>()).add(node);
            }
        }

        return topHist;
    }

    private static class Data implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        private final long lowTopVer;

        private final DataEntry[] entries;

        public Data(long lowTopVer, long highTopVer) {
            this.lowTopVer = lowTopVer;
            entries = new DataEntry[(int) (highTopVer - lowTopVer + 1)];
        }

        public void add(long topVer, TcpDiscoveryNode node, @Nullable Map<String, Object> prevAttrs) {
            Map<String, Object> attrs = node.getAttributes();

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

    private static class DataEntry implements Externalizable {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        UUID id;

        Object[] attrKeys;

        Object[] attrVals;

        Collection<String> addrs;

        Collection<String> hostNames;

        int discPort;

        ClusterMetrics metrics;

        long order;

        long intOrder;

        IgniteProductVersion ver;

        UUID clientRouterNodeId;

        public DataEntry(TcpDiscoveryNode node) {
            this(node, null, null);
        }

        public DataEntry(TcpDiscoveryNode node, String[] attrKeys, Object[] attrVals) {
            this.id = node.id();
            this.attrKeys = attrKeys;
            this.attrVals = attrVals;

            this.addrs = node.addresses();
            this.hostNames = node.hostNames();
            this.discPort = node.discoveryPort();
            this.metrics = node.metrics();
            this.order = node.order();
            this.ver = node.version();
            this.clientRouterNodeId = node.clientRouterNodeId();
        }

        public DataEntry() {
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUuid(out, id);
            U.writeArray(out, attrKeys);
            U.writeArray(out, attrVals);
            U.writeCollection(out, addrs);
            U.writeCollection(out, hostNames);
            out.writeInt(discPort);

            ClusterMetrics metrics = this.metrics;

            byte[] mtr = null;

            if (metrics != null)
                mtr = ClusterMetricsSnapshot.serialize(metrics);

            U.writeByteArray(out, mtr);

            out.writeLong(order);
            out.writeLong(intOrder);
            out.writeObject(ver);
            U.writeUuid(out, clientRouterNodeId);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id = U.readUuid(in);

            attrKeys = U.readArray(in);
            attrVals = U.readArray(in);

            addrs = U.readCollection(in);
            hostNames = U.readCollection(in);
            discPort = in.readInt();

            byte[] mtr = U.readByteArray(in);

            if (mtr != null)
                metrics = ClusterMetricsSnapshot.deserialize(mtr, 0);

            order = in.readLong();
            intOrder = in.readLong();
            ver = (IgniteProductVersion) in.readObject();
            clientRouterNodeId = U.readUuid(in);
        }
    }
}
