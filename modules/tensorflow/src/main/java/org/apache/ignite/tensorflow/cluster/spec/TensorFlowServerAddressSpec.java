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

package org.apache.ignite.tensorflow.cluster.spec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;

/**
 * TensorFlow server address specification.
 */
public class TensorFlowServerAddressSpec implements Serializable {
    /** */
    private static final long serialVersionUID = 7883701602323727681L;

    /** Node identifier. */
    private final UUID nodeId;

    /** Port. */
    private final int port;

    /**
     * Constructs a new instance of TensorFlow server address specification.
     *
     * @param nodeId Node identifier.
     * @param port Port.
     */
    TensorFlowServerAddressSpec(UUID nodeId, int port) {
        assert nodeId != null : "Node identifier should not be null";
        assert port >= 0 && port <= 0xFFFF : "Port should be between 0 and 65535";

        this.nodeId = nodeId;
        this.port = port;
    }

    /**
     * Formats Server Address specification so that TensorFlow accepts it.
     *
     * @param ignite Ignite instance.
     * @return Formatted server address specification.
     */
    public String format(Ignite ignite) {
        return calculateAddress(ignite) + ":" + port;
    }

    /**
     * Calculates host address for the {@link #nodeId}.
     *
     * @param ignite Ignite instance.
     * @return Address of the {@link #nodeId}.
     */
    private String calculateAddress(Ignite ignite) {
        List<String> prefixes = calculateAddressPrefixes(ignite);

        if (prefixes == null || prefixes.isEmpty())
            throw new IllegalStateException("Cannot find a common network for Ignite nodes.");

        String desiredPrefix = prefixes.iterator().next();

        for (String name : ignite.cluster().node(nodeId).addresses()) {
            if (name.startsWith(desiredPrefix))
                return name;
        }

        throw new IllegalStateException("Cannot find hostname with desired prefix [prefix=" + desiredPrefix + "]");
    }

    /**
     * Calculates address prefixes comon for all Ignite nodes.
     *
     * @param ignite Ignite instance.
     * @return List of address prefixes.
     */
    private List<String> calculateAddressPrefixes(Ignite ignite) {
        List<String> prefixes = null;

        for (ClusterNode node : ignite.cluster().nodes()) {
            Collection<String> addrs = node.addresses();

            prefixes = prefixes == null ? new ArrayList<>(addrs) :longestPrefixes(prefixes, addrs);
        }

        prefixes.sort(Comparator
            .comparingInt(String::length)
            .reversed()
            .thenComparing(String::compareTo)
        );

        return prefixes;
    }

    /**
     * Calculates list of prefixes which are common for both collections.
     *
     * @param aCol First collection.
     * @param bCol Second collection.
     * @return List of prefixes which are common for both collections.
     */
    private List<String> longestPrefixes(Collection<String> aCol, Collection<String> bCol) {
        List<String> res = new ArrayList<>();

        for (String a : aCol) {
            String longestCommonPrefix = null;
            long longestCommonPrefixLen = 0;

            for (String b : bCol) {
                int commonPrefixLen = calculateLengthOfCommonPrefix(a, b);
                if (commonPrefixLen > longestCommonPrefixLen) {
                    longestCommonPrefix = a.substring(0, commonPrefixLen);
                    longestCommonPrefixLen = commonPrefixLen;
                }
            }

            if (longestCommonPrefix != null)
                res.add(longestCommonPrefix);
        }

        return res;
    }

    /**
     * Calculates length of a common prefix for two strings.
     *
     * @param a First string.
     * @param b Second string.
     * @return Length of a common prefix for two strings.
     */
    private int calculateLengthOfCommonPrefix(String a, String b) {
        int res = 0;

        while (res < a.length() && res < b.length()) {
            if (a.charAt(res) != b.charAt(res))
                break;
            res++;
        }

        return res;
    }

    /** */
    public UUID getNodeId() {
        return nodeId;
    }

    /** */
    public int getPort() {
        return port;
    }
}
