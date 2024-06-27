/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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
import java.util.stream.Collectors;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This class serves to optimize message size with topology history.
 * In case of long-lived cluster topology history could be hundreds and even thousands entries.
 * Each entry contains multiple {@link TcpDiscoveryNode} objects which may contains hundreds different attributes.
 * Combination of many historical entries with multiple nodes in topology and multiple attributes on each node blow up
 * message size.
 * Currently only attributes are optimized.
 * We assume that nodes rarely change their attributes during normal exploitation, therefore we divide history according
 * node's consistentId and only store initial state of attributes for oldest topology version, for older topology
 * versions we only record an attribute change.
 */
public class CompactedTopologyHistory implements Externalizable {

    /** */
    private static final long serialVersionUID = 1L;

    /** Stores topology history. Indexed by consistent ID. */
    private Map<Serializable, HistoryData> historyByNode;

    /** Ctor. */
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

                HistoryData data = historyByNode.computeIfAbsent(
                        consistentId,
                        k -> new HistoryData(lowTopVer, highTopVer)
                );

                data.add(topVer, node, prevAttrs);

                prevAttrsPerNode.put(consistentId, node.getAttributes());
            }
        }
    }

    /** Ctor required by {@link Externalizable}. */
    public CompactedTopologyHistory() {}

    /** Restore topology history to its original format. */
    public Map<Long, Collection<ClusterNode>> restore() {
        Map<Long, Collection<ClusterNode>> topHist = new TreeMap<>();

        for (Map.Entry<Serializable, HistoryData> e : historyByNode.entrySet()) {
            Serializable consistentId = e.getKey();
            HistoryData data = e.getValue();

            Map<String, Object> attrs = new HashMap<>();

            Object[] entries = data.entries;
            for (int i = 0, entriesLength = entries.length; i < entriesLength; i++) {
                if (entries[i] == null)
                    continue;

                assert entries[i] instanceof NodeData;

                NodeData entry = (NodeData) entries[i];
                long topVer = i + data.lowTopVer;

                TcpDiscoveryNode node = new TcpDiscoveryNode(
                        entry.id,
                        entry.addrs,
                        entry.hostNames,
                        entry.discPort,
                        new DiscoveryMetricsProvider() {
                            /** {@inheritDoc} */
                            @Override public ClusterMetrics metrics() {
                                return entry.metrics;
                            }

                            /** {@inheritDoc} */
                            @Override public Map<Integer, CacheMetrics> cacheMetrics() {
                                return null;
                            }
                        },
                        entry.ver,
                        consistentId,
                        false
                );

                for (int j = 0; entry.attrKeys != null && j < entry.attrKeys.length; j++) {
                    String attrKey = (String) entry.attrKeys[j];
                    Object attrVal = entry.attrVals[j];

                    if (attrVal == null)
                        attrs.remove(attrKey);
                    else
                        attrs.put(attrKey, attrVal);
                }

                node.setAttributes(attrs);

                topHist.computeIfAbsent(topVer, k -> new ArrayList<>()).add(node);
            }
        }

        return topHist;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeMap(out, historyByNode);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        historyByNode = U.readMap(in);
    }

    private static class HistoryData implements Externalizable {
        /** */
        private static final long serialVersionUID = 1L;

        /** Oldest topology version. */
        private long lowTopVer;

        /** DataEntries. */
        private Object[] entries;

        /** Ctor required by {@link Externalizable}. */
        public HistoryData() {}

        /** Ctor. */
        public HistoryData(long lowTopVer, long highTopVer) {
            this.lowTopVer = lowTopVer;
            entries = new NodeData[(int) (highTopVer - lowTopVer + 1)];
        }

        /**
         * Add new entry to storage.
         * @param topVer Topology version.
         * @param node Ignite node.
         * @param prevAttrs Previous topology version attributes.
         */
        public void add(long topVer, TcpDiscoveryNode node, @Nullable Map<String, Object> prevAttrs) {
            Map<String, Object> attrs = node.getAttributes();

            List<String> deltaKeys = new ArrayList<>();
            List<Object> deltaVals = new ArrayList<>();

            //Store new keys and updated values
            for (Map.Entry<String, Object> e : attrs.entrySet()) {
                String key = e.getKey();
                Object val = e.getValue();

                boolean prevHasKey = prevAttrs != null && prevAttrs.containsKey(key);

                if (!prevHasKey || !Objects.equals(prevAttrs.get(key), val)) {
                    deltaKeys.add(key);
                    deltaVals.add(val);
                }
            }

            if (prevAttrs != null) {
                List<String> deletedKeys = prevAttrs.keySet().stream()
                        .filter(k -> !attrs.containsKey(k))
                        .collect(Collectors.toList());

                //store delete keys
                for (String deletedKey : deletedKeys) {
                    deltaKeys.add(deletedKey);
                    deltaVals.add(null);
                }
            }

            NodeData nodeData = deltaKeys.isEmpty()
                    ? new NodeData(node)
                    : new NodeData(node, deltaKeys.toArray(new String[0]), deltaVals.toArray());

            entries[(int) (topVer - lowTopVer)] = nodeData;
        }

        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(lowTopVer);
            U.writeArray(out, entries);
        }

        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            lowTopVer = in.readLong();
            entries = U.readArray(in);
        }
    }

    /** Represents compacted node data. */
    private static class NodeData implements Externalizable {
        /** */
        private static final long serialVersionUID = 1L;

        /** Node ID. */
        private UUID id;

        /** Node compacted attribute keys. */
        private Object[] attrKeys;

        /** Node compacted attribute vals. */
        private Object[] attrVals;

        /** Node addresses. */
        private Collection<String> addrs;

        /** Node hostname. */
        private Collection<String> hostNames;

        /** Node discovery port. */
        private int discPort;

        /** Node metrics. */
        private ClusterMetrics metrics;

        /** Node order. */
        private long order;

        /** Node internal order. */
        private long intOrder;

        /** Node product version. */
        private IgniteProductVersion ver;

        /** Client router node ID. */
        private UUID clientRouterNodeId;

        /** Ctor required by {@link Externalizable}. */
        public NodeData() {}

        /** Ctor. */
        public NodeData(TcpDiscoveryNode node) {
            this(node, null, null);
        }

        /** Ctor. */
        public NodeData(TcpDiscoveryNode node, String[] attrKeys, Object[] attrVals) {
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

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
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

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
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
