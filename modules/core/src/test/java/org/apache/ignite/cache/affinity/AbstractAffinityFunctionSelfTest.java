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

package org.apache.ignite.cache.affinity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public abstract class AbstractAffinityFunctionSelfTest extends GridCommonAbstractTest {
    /** MAC prefix. */
    private static final String MAC_PREF = "MAC";

    /**
     * Returns affinity function.
     *
     * @return Affinity function.
     */
    protected abstract AffinityFunction affinityFunction();

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRemovedNoBackups() throws Exception {
        checkNodeRemoved(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRemovedOneBackup() throws Exception {
        checkNodeRemoved(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRemovedTwoBackups() throws Exception {
        checkNodeRemoved(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRemovedThreeBackups() throws Exception {
        checkNodeRemoved(3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomReassignmentNoBackups() throws Exception {
        checkRandomReassignment(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomReassignmentOneBackup() throws Exception {
        checkRandomReassignment(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomReassignmentTwoBackups() throws Exception {
        checkRandomReassignment(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomReassignmentThreeBackups() throws Exception {
        checkRandomReassignment(3);
    }

    /**
     */
    @Test
    public void testQQQ() {
        checkRandomReassignment(2);
    }

    /**
     * @param backups Number of backups.
     * @throws Exception If failed.
     */
    @Test
    public void testNullKeyForPartitionCalculation() throws Exception {
        AffinityFunction aff = affinityFunction();

        try {
            aff.partition(null);

            fail("Should throw IllegalArgumentException due to NULL affinity key.");
        } catch (IllegalArgumentException e) {
            e.getMessage().contains("Null key is passed for a partition calculation. " +
                "Make sure that an affinity key that is used is initialized properly.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected void checkNodeRemoved(int backups) throws Exception {
        checkNodeRemoved(backups, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    protected void checkNodeRemoved(int backups, int neighborsPerHost, int neighborsPeriod) throws Exception {

        AffinityFunction aff = affinityFunction();

        int nodesCnt = 50;

        List<ClusterNode> nodes = new ArrayList<>(nodesCnt);

        List<List<ClusterNode>> prev = null;

        for (int i = 0; i < nodesCnt; i++) {
            info("======================================");
            info("Assigning partitions: " + i);
            info("======================================");

            ClusterNode node = new GridTestNode(UUID.randomUUID());

            if (neighborsPerHost > 0)
                node.attribute(MAC_PREF + ((i / neighborsPeriod) % (nodesCnt / neighborsPerHost)));

            nodes.add(node);

            DiscoveryEvent discoEvt = new DiscoveryEvent(node, "", EventType.EVT_NODE_JOINED, node);

            GridAffinityFunctionContextImpl ctx =
                new GridAffinityFunctionContextImpl(nodes, prev, discoEvt, new AffinityTopologyVersion(i), backups);

            List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

            info("Assigned.");

            verifyAssignment(assignment, backups, aff.partitions(), nodes.size());

            prev = assignment;
        }

        info("======================================");
        info("Will remove nodes.");
        info("======================================");

        for (int i = 0; i < nodesCnt - 1; i++) {
            info("======================================");
            info("Assigning partitions: " + i);
            info("======================================");

            ClusterNode rmv = nodes.remove(nodes.size() - 1);

            DiscoveryEvent discoEvt = new DiscoveryEvent(rmv, "", EventType.EVT_NODE_LEFT, rmv);

            List<List<ClusterNode>> assignment = aff.assignPartitions(
                new GridAffinityFunctionContextImpl(nodes, prev, discoEvt, new AffinityTopologyVersion(i),
                    backups));

            info("Assigned");

            verifyAssignment(assignment, backups, aff.partitions(), nodes.size());

            prev = assignment;
        }
    }

    private int rebalancedReplicasCount(List<List<ClusterNode>> olda, List<List<ClusterNode>> newa) {
        if (olda == null) {
            return 0;
        }

        int res = 0;

        for (int p = 0; p < olda.size(); p++) {
            List<ClusterNode> o = olda.get(p);
            List<ClusterNode> n = newa.get(p);

            for (ClusterNode newNode : n) {
                if (!o.contains(newNode)) {
                    res++;
                }
            }


            for (ClusterNode oldNode : o) {
                if (!n.contains(oldNode)) {
                    res++;
                }
            }
        }

        return res;
    }

    /**
     * @param backups Backups.
     */
    protected void checkRandomReassignment(int backups) {
        AffinityFunction aff = affinityFunction();

        Random rnd = new Random();

        int maxNodes = 5;

        List<ClusterNode> nodes = new ArrayList<>(maxNodes);

        List<List<ClusterNode>> prev = null;

        /*try {
            for (int i = 0; i < 5; i++) {
                nodes.add(new GridTestNode(UUID.randomUUID()));
            }
            List<List<ClusterNode>> ass = aff.assignPartitions(
                new GridAffinityFunctionContextImpl(nodes, prev, new DiscoveryEvent(nodes.get(4), "", EventType.EVT_NODE_JOINED, nodes.get(4)), new AffinityTopologyVersion(1),
                    backups));
            printReplicas(ass);
        } catch (Exception e) { }

        System.out.println("---------------------------------------------------------------------");*/

        int state = 0;

        int i = 0;

        while (i < 1000) {
            boolean add;

            if (nodes.size() < 2) {
                // Returned back to one node?
                if (state == 1)
                    return;

                add = true;
            }
            else if (nodes.size() == maxNodes) {
                if (state == 0)
                    state = 1;

                add = false;
            }
            else {
                // Nodes size in [2, maxNodes - 1].
                if (state == 0)
                    add = rnd.nextInt(3) != 0; // 66% to add, 33% to remove.
                else
                    add = rnd.nextInt(3) == 0; // 33% to add, 66% to remove.
            }

            DiscoveryEvent discoEvt;

            List<ClusterNode> oldNodes = new ArrayList<>(nodes);

            if (add) {
                ClusterNode addedNode = new GridTestNode(UUID.randomUUID());

                nodes.add(addedNode);

                discoEvt = new DiscoveryEvent(addedNode, "", EventType.EVT_NODE_JOINED, addedNode);
            }
            else {
                ClusterNode rmvNode = nodes.remove(rnd.nextInt(nodes.size()));

                discoEvt = new DiscoveryEvent(rmvNode, "", EventType.EVT_NODE_LEFT, rmvNode);
            }

            long start = System.currentTimeMillis();
            List<List<ClusterNode>> assignment = aff.assignPartitions(
                new GridAffinityFunctionContextImpl(nodes, prev, discoEvt, new AffinityTopologyVersion(i),
                    backups));
            long duration = System.currentTimeMillis() - start;
            System.out.println("OldNodes: " + (oldNodes == null ? 0 : oldNodes.size()) + ", newNodes: " + nodes.size() + ", duration: " + duration);

            if (oldNodes.size() >= backups + 1 || nodes.size() >= backups + 1) {
                info("======================================");
                info("Assigning partitions [iter=" + i + ", nodesSize=" + nodes.size() + ", oldNodesSize=" + oldNodes.size()
                    + ", rebalancedReplicas=" + rebalancedReplicasCount(prev, assignment) + ", discoEvt=" + discoEvt + ']');
                info("======================================");

                printReplicas(assignment);
            }

            verifyAssignment(assignment, backups, aff.partitions(), nodes.size());

            prev = assignment;

            i++;
        }
    }

    private void printReplicas(List<List<ClusterNode>> assignment) {
        Map<ClusterNode, Integer> replicas = new HashMap<>();

        for (List<ClusterNode> a : assignment) {
            for (ClusterNode node : a) {
                replicas.compute(node, (k, v) -> {
                    if (v == null) {
                        v = 0;
                    }

                    return v + 1;
                });
            }
        }

        replicas.forEach((k, v) -> System.out.println(k.id() + ": " + v));

        System.out.println("min. replicas: " + replicas.values().stream().mapToInt(i -> i).min().orElseThrow(NoSuchElementException::new));
        System.out.println("max. replicas: " + replicas.values().stream().mapToInt(i -> i).max().orElseThrow(NoSuchElementException::new));
    }

    /**
     * @param assignment Assignment to verify.
     */
    private void verifyAssignment(List<List<ClusterNode>> assignment, int keyBackups, int partsCnt, int topSize) {
        Map<UUID, Collection<Integer>> mapping = new HashMap<>();

        int ideal = Math.round((float)partsCnt / topSize * Math.min(keyBackups + 1, topSize));

        for (int part = 0; part < assignment.size(); part++) {
            for (ClusterNode node : assignment.get(part)) {
                assert node != null;

                Collection<Integer> parts = mapping.get(node.id());

                if (parts == null) {
                    parts = new HashSet<>();

                    mapping.put(node.id(), parts);
                }

                assertTrue(parts.add(part));
            }
        }

        int max = -1, min = Integer.MAX_VALUE;

        for (Collection<Integer> parts : mapping.values()) {
            max = Math.max(max, parts.size());
            min = Math.min(min, parts.size());
        }

        log().warning("max=" + max + ", min=" + min + ", ideal=" + ideal + ", minDev=" + deviation(min, ideal) + "%, " +
            "maxDev=" + deviation(max, ideal) + "%");
    }

    /**
     * @param val Value.
     * @param ideal Ideal.
     */
    private static int deviation(int val, int ideal) {
        return Math.round(Math.abs(((float)val - ideal) / ideal * 100));
    }
}
