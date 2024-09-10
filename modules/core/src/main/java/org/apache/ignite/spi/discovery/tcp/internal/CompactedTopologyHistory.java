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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;

/**
 * This class serves to optimize message size with topology history.
 * In case of long-lived cluster topology history could be hundreds and even thousands entries.
 * Each entry contains multiple {@link TcpDiscoveryNode} objects which may contains hundreds different attributes.
 * Combination of many historical entries with multiple nodes in topology and multiple attributes on each node blow up
 * message size.
 * Currently only attributes are optimized.
 * We assume that nodes rarely change their attributes during normal exploitation, therefore we divide history according
 * to node's consistentId and only store initial state of attributes for oldest topology version; for later topology
 * versions we only record an attribute change.
 */
public class CompactedTopologyHistory implements Externalizable {
    /** */
    private static final long serialVersionUID = 1L;

    /** Indicates that node didn't yet appear in topology history. */
    private static final int NEW_NODE = 0;

    /** Indicates that node appeared in topology history before, and it's not the same instance. */
    private static final int DELTA_NODE = 1;

    /** Indicates that node appeared in topology history before, and it's the same instance. */
    private static final int SAME_NODE = 2;

    /** Original topology history map. */
    private SortedMap<Long, Collection<ClusterNode>> topHist;

    /** The earliest known topology version. Cached in a field to save some time on a hot path. */
    private long earliestTopVer;

    /** Ctor. */
    public CompactedTopologyHistory(@NotNull Map<Long, Collection<ClusterNode>> topHist) {
        assert topHist instanceof SortedMap && !topHist.isEmpty() : topHist + " is invalid";

        this.topHist = (SortedMap<Long, Collection<ClusterNode>>)topHist;
        earliestTopVer = this.topHist.firstKey();
    }

    /** Ctor required by {@link Externalizable}. */
    public CompactedTopologyHistory() {
    }

    /** Returns a topology history map. */
    public SortedMap<Long, Collection<ClusterNode>> asMap() {
        return topHist;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // We write a single "long" here, so that later we can only write "int" as a diff between "ver" and "firstKey".
        out.writeLong(earliestTopVer);

        int size = topHist.size();
        out.writeInt(size);

        // Map from "consistentId" to the latest currently known (in terms of topology versions) cluster node instance.
        Map<Object, TcpDiscoveryNode> nodesMap = new HashMap<>();

        for (Map.Entry<Long, Collection<ClusterNode>> entry : topHist.entrySet()) {
            long ver = entry.getKey();
            Collection<ClusterNode> top = entry.getValue();

            out.writeInt((int)(ver - earliestTopVer));
            out.writeInt(top.size());

            for (ClusterNode clusterNode : top) {
                TcpDiscoveryNode newNode = (TcpDiscoveryNode)clusterNode;

                Object consistentId = newNode.consistentId();
                TcpDiscoveryNode oldNode = nodesMap.get(consistentId);

                if (oldNode == null) {
                    nodesMap.put(consistentId, newNode);

                    out.write(NEW_NODE);
                    writeTcpDiscoveryNode(out, newNode);
                }
                else if (oldNode != newNode) {
                    // Node instance might be different only if node re-entered topology.
                    nodesMap.put(consistentId, newNode);

                    out.write(DELTA_NODE);
                    writeTcpDiscoveryNode(out, diff(oldNode, newNode));
                }
                else { // oldNode == node
                    out.write(SAME_NODE);

                    // Although consistent IDs are usually strings or UUIDs, it's still more optimal to write them as
                    // objects, due to internal cache in "ObjectOutput" instance.
                    out.writeObject(consistentId);
                }
            }
        }
    }

    /**
     * Writes {@link TcpDiscoveryNode} into an output stream, without writing any extra meta information about the type.
     */
    private static void writeTcpDiscoveryNode(ObjectOutput out, TcpDiscoveryNode node) throws IOException {
        node.writeExternal(out);
    }

    /**
     * Reads {@link TcpDiscoveryNode} from an input stream, without actually calling {@link ObjectInput#readObject()}.
     * We call {@link TcpDiscoveryNode#readExternal(ObjectInput)} directly.
     */
    private static TcpDiscoveryNode readTcpDiscoveryNode(ObjectInput in) throws IOException, ClassNotFoundException {
        TcpDiscoveryNode node = new TcpDiscoveryNode();

        node.readExternal(in);

        return node;
    }

    /**
     * Helper class that stores mutable values, that are necessary to change in lambdas of
     * {@link #diff(TcpDiscoveryNode, TcpDiscoveryNode)}.
     */
    private static class DiffState {
        /**
         * Optimization flag. Will be {@code true} if {@code newAttrs} contains all attributes from {@code oldAttrs}.
         * In such a case, if {@code oldAttrs.size()} is equal to {@code newAttrs.size()}, we can conclude that their
         * {@link Map#keySet()}s are equal.
         *
         * @see #diff(TcpDiscoveryNode, TcpDiscoveryNode)
         */
        boolean hasAllKeys = true;

        /**
         * Map with the difference between {@code oldAttrs} and {@code newAttrs}. Key with {@code null} value means a
         * removed attribute. This map is initially equal to {@code null} and initialized lazily, in order to save some
         * precious allocations.
         */
        Map<String, Object> delta = null;
    }

    /**
     * Returns a copy of {@code newNode}, but its attributes map contains a difference between attributes of
     * {@code oldNode} and {@code newNode}.
     */
    private static TcpDiscoveryNode diff(TcpDiscoveryNode oldNode, TcpDiscoveryNode newNode) {
        // It's important that we call "*.getAttributes()" instead of "*.attributes()", because the latter returns a
        // wrapper that's a "view" on top of the original map, and it also filters out security attributes, meaning that
        // it's literally missing some data.
        Map<String, Object> oldAttrs = oldNode.getAttributes();
        Map<String, Object> newAttrs = newNode.getAttributes();

        DiffState diffState = new DiffState();

        // Calculate updated keys.
        // "forEach" here is not a stylistic choice, but rather an optimization. Attributes map that we iterate here is
        // initialized with "Collections.unmodifiableMap", and its "*.entrySet()" iterator leads to a missive allocation
        // of "Map.Entry" instances, which is noticeably slower than "forEach".
        newAttrs.forEach((key, val) -> {
            Object oldAttr = oldAttrs.get(key);

            // We only call "containsKey" if "oldAttr" is "null". This way we avoid doing two lookups in the map because
            // that's expensive.
            boolean oldAttrsContainKey = oldAttr != null || oldAttrs.containsKey(key);
            diffState.hasAllKeys &= oldAttrsContainKey;

            if (!oldAttrsContainKey || !Objects.deepEquals(oldAttr, val)) {
                if (diffState.delta == null)
                    diffState.delta = new HashMap<>(4); // Vast majority of attributes never change.

                diffState.delta.put(key, val);
            }
        });

        // Calculate deleted keys. See "DiffState.hasAllKeys".
        if (!diffState.hasAllKeys || oldAttrs.size() != newAttrs.size()) {
            // Here we use "forEach" for the same reason.
            oldAttrs.forEach((k, v) -> {
                if (!newAttrs.containsKey(k)) {
                    if (diffState.delta == null)
                        diffState.delta = new HashMap<>(4); // Vast majority of attributes never change.

                    diffState.delta.put(k, null);
                }
            });
        }

        // TCP discovery node deserialization reads consistent ID from attributes instead of storing it separately. This
        // is why we must put it there. "singletonMap" is cheaper than "HashMap", that's why we use lazy initialization.
        // See "TcpDiscoveryNode.readExternal".
        if (diffState.delta == null)
            diffState.delta = Collections.singletonMap(ATTR_NODE_CONSISTENT_ID, oldNode.consistentId());
        else
            diffState.delta.put(ATTR_NODE_CONSISTENT_ID, oldNode.consistentId());

        TcpDiscoveryNode res = TcpDiscoveryNode.copy(newNode);
        res.setAttributesNoCopy(diffState.delta);
        return res;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        topHist = new TreeMap<>();
        earliestTopVer = in.readLong();

        int size = in.readInt();

        Map<Object, TcpDiscoveryNode> nodesMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            long ver = earliestTopVer + in.readInt();

            int topSize = in.readInt();
            List<ClusterNode> top = new ArrayList<>(topSize);
            for (int j = 0; j < topSize; j++) {
                int type = in.read();

                switch (type) {
                    case NEW_NODE: {
                        TcpDiscoveryNode node = readTcpDiscoveryNode(in);

                        nodesMap.put(node.consistentId(), node);

                        top.add(node);

                        break;
                    }

                    case DELTA_NODE: {
                        TcpDiscoveryNode newNode = readTcpDiscoveryNode(in);

                        TcpDiscoveryNode oldNode = nodesMap.get(newNode.consistentId());
                        assert oldNode != null
                            : "Can't find old node while deserializing compacted topology history [consistentId="
                            + newNode.consistentId() + ']';

                        // Technically, this copying can be avoided if delta only has a consistent ID, but there's not
                        // much of a performance benefit, while the code would become much, much worse.
                        Map<String, Object> newAttrs = new HashMap<>(oldNode.getAttributes());

                        newNode.getAttributes().forEach((k, v) -> {
                            if (v != null)
                                newAttrs.put(k, v);
                            else
                                newAttrs.remove(k);
                        });

                        newNode.setAttributesNoCopy(newAttrs);

                        nodesMap.put(newNode.consistentId(), newNode);

                        top.add(newNode);

                        break;
                    }
                    case SAME_NODE: {
                        Object consistentId = in.readObject();

                        TcpDiscoveryNode oldNode = nodesMap.get(consistentId);
                        assert oldNode != null
                            : "Can't find old node while deserializing compacted topology history [consistentId="
                            + consistentId + ']';

                        top.add(oldNode);

                        break;
                    }

                    default:
                        throw new IOException("Unexpected node serialization type: " + type);
                }
            }

            topHist.put(ver, top);
        }
    }
}
