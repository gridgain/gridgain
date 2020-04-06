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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_ALL;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;

/**
 * Tests partition loss policies working for in-memory groups with multiple caches.
 */
public class IgniteCacheGroupsPartitionLossPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 32;

    /** */
    private boolean client;

    /** */
    private PartitionLossPolicy partLossPlc;

    private int backups;

    /** */
    private static final String GROUP_NAME = "group";

    /** */
    private static final String[] CACHES = new String[]{"cache1", "cache2"};

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES.length];

        for (int i = 0; i < ccfgs.length; i++) {
            ccfgs[i] = new CacheConfiguration(CACHES[i])
                    .setGroupName(GROUP_NAME)
                    .setCacheMode(PARTITIONED)
                    .setBackups(backups)
                    .setWriteSynchronizationMode(FULL_SYNC)
                    .setPartitionLossPolicy(partLossPlc)
                    .setReadFromBackup(false) // Remove to reproduce a bug with reading from deleted partition.
                    .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));
        }

        cfg.setCacheConfiguration(ccfgs);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = 0;

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlySafe() throws Exception {
        partLossPlc = READ_ONLY_SAFE;

        checkLostPartition(false, true, 4, 3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlyAll() throws Exception {
        partLossPlc = READ_ONLY_ALL; // Should be same as testReadOnlySafe.

        checkLostPartition(false, true, 4, 3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafe() throws Exception {
        partLossPlc = IGNORE; // Should use safe policy instead.

        checkLostPartition(false, true, 4, 3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafe_2() throws Exception {
        partLossPlc = READ_ONLY_SAFE;

        checkLostPartition(false, true, 4, 3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteAll() throws Exception {
        partLossPlc = IGNORE; // Should be same as testReadWriteSafe.

        checkLostPartition(false, true, 4, 3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgnore() throws Exception {
        partLossPlc = IGNORE;

        checkLostPartition(true, false, 4, 3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgnore_2() throws Exception {
        partLossPlc = READ_WRITE_SAFE; // Should use safe policy.

        checkLostPartition(true, true, 4, 3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgnore_3() throws Exception {
        partLossPlc = READ_ONLY_SAFE; // Should use safe read-only policy.

        checkLostPartition(true, true, 4, 3);
    }

    @Test
    public void testReadWriteSafeAfterKillTwoNodesWithDelayWithPersistence() throws Exception {
        partLossPlc = READ_ONLY_SAFE;
        backups = 1;

        checkLostPartition(false, true, 4, 3, 2);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkLostPartition(boolean autoAdjust, boolean safe, int nodes, int... stopNodes) throws Exception {
        String cacheName = CACHES[ThreadLocalRandom.current().nextInt(CACHES.length)];

        List<Integer> partEvts = Collections.synchronizedList(new ArrayList<>());

        Set<Integer> expLostParts = prepareTopology(nodes, autoAdjust, new P1<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt.type() == EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

                CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent)evt;

                partEvts.add(cacheEvt.partition());

                return true;
            }
        }, stopNodes);

        int[] stopNodesSorted = Arrays.copyOf(stopNodes, stopNodes.length);
        Arrays.sort(stopNodesSorted);

        for (Ignite ig : G.allGrids()) {
            if (Arrays.binarySearch(stopNodesSorted, getTestIgniteInstanceIndex(ig.name())) >= 0)
                continue;

            verifyCacheOps(cacheName, expLostParts, ig, safe);
        }

        if (safe) {
            assertTrue(partEvts.toString(), GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return expLostParts.size() * CACHES.length * G.allGrids().size() == partEvts.size();
                }
            }, 5_000));

            assertEquals(expLostParts, new HashSet<>(partEvts));
        }

        // Check that partition state does not change after we return nodes.
        for (int i = 0; i < stopNodes.length; i++) {
            int node = stopNodes[i];

            IgniteEx grd = startGrid(node);

            info("Newly started node: " + grd.cluster().localNode().id());
        }

        doSleep(2000);

        for (int i = 0; i < nodes + 1; i++)
            verifyCacheOps(cacheName, expLostParts, grid(i), safe);

        if (safe)
            ignite(0).resetLostPartitions(Arrays.asList(CACHES));

        awaitPartitionMapExchange(true, true, null);

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

            assertTrue(cache.lostPartitions().isEmpty());

            int parts = ig.affinity(cacheName).partitions();

            for (int i = 0; i < parts; i++) {
                cache.get(i);

                cache.put(i, i);
            }
        }
    }

    /**
     * @param cacheName Cache name.
     * @param expLostParts Expected lost parts.
     * @param ig Ignite.
     * @param safe Safe.
     */
    private void verifyCacheOps(String cacheName, Set<Integer> expLostParts, Ignite ig, boolean safe) {
        boolean readOnly = partLossPlc == READ_ONLY_SAFE || partLossPlc == READ_ONLY_ALL;

        IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

        int parts = ig.affinity(cacheName).partitions();

        if (!safe)
            assertTrue(cache.lostPartitions().isEmpty());

        // Check single reads.
        for (int p = 0; p < parts; p++) {
            try {
                Integer actual = cache.get(p);

                if (safe) {
                    assertTrue("Reading from a lost partition should have failed [part=" + p + ']',
                            !cache.lostPartitions().contains(p));

                    assertEquals(p, actual.intValue());
                }
                else
                    assertEquals(expLostParts.contains(p) ? null : p, actual);
            }
            catch (CacheException e) {
                assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));

                assertTrue("Read exception should only be triggered for a lost partition " +
                    "[ex=" + X.getFullStackTrace(e) + ", part=" + p + ']', cache.lostPartitions().contains(p));
            }
        }

        // Check single writes.
        for (int p = 0; p < parts; p++) {
            try {
                cache.put(p, p);

                if (!safe && expLostParts.contains(p))
                    cache.remove(p);

                if (readOnly) {
                    assertTrue(!cache.lostPartitions().contains(p));

                    fail("Writing to a cache containing lost partitions should have failed [part=" + p + ']');
                }

                if (safe) {
                    assertTrue("Writing to a lost partition should have failed [part=" + p + ']',
                            !cache.lostPartitions().contains(p));
                }
            }
            catch (CacheException e) {
                assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));

                assertTrue("Write exception should only be triggered for a lost partition or in read-only mode " +
                        "[ex=" + X.getFullStackTrace(e) + ", part=" + p + ']', readOnly || cache.lostPartitions().contains(p));
            }
        }

        Set<Integer> notLost = IntStream.range(0, parts).boxed().filter(p -> !expLostParts.contains(p)).collect(Collectors.toSet());

        try {
            Map<Integer, Integer> res = cache.getAll(expLostParts);

            assertFalse("Reads from lost partitions should have been allowed only in non-safe mode", safe);
        }
        catch (CacheException e) {
            assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
        }

        try {
            Map<Integer, Integer> res = cache.getAll(notLost);
        }
        catch (Exception e) {
            fail("Reads from non lost partitions should have been always allowed");
        }

        try {
            cache.putAll(expLostParts.stream().collect(Collectors.toMap(k -> k, v -> v)));

            assertFalse("Writes to lost partitions should have been allowed only in non-safe mode", safe);

            cache.removeAll(expLostParts);
        }
        catch (CacheException e) {
            assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
        }

        try {
            cache.putAll(notLost.stream().collect(Collectors.toMap(k -> k, v -> v)));

            assertTrue("Writes to non-lost partitions should have been allowed only in read-write or non-safe mode",
                    !safe || !readOnly);
        }
        catch (CacheException e) {
            assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
        }

        // Check queries.
        for (int p = 0; p < parts; p++) {
            boolean loc = ig.affinity(cacheName).isPrimary(ig.cluster().localNode(), p);

            try {
                runQuery(ig, cacheName, false, p);

                assertTrue("Query over lost partition should have failed: safe=" + safe +
                        ", expLost=" + expLostParts + ", p=" + p, !safe || !expLostParts.contains(p));
            } catch (Exception e) {
                assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
            }

            if (loc) {
                try {
                    runQuery(ig, cacheName, true, p);

                    assertTrue("Query over lost partition should have failed: safe=" + safe +
                            ", expLost=" + expLostParts + ", p=" + p, !safe || !expLostParts.contains(p));
                } catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
                }
            }
        }
    }

    /**
     * @param ig Ignite.
     * @param cacheName Cache name.
     * @param loc Local.
     * @param part Partition.
     */
    protected void runQuery(Ignite ig, String cacheName, boolean loc, int part) {
        IgniteCache cache = ig.cache(cacheName);

        ScanQuery qry = new ScanQuery();
        qry.setPartition(part);

        if (loc)
            qry.setLocal(true);

        cache.query(qry).getAll();
    }

    /**
     * @param autoAdjustTimeout Auto-adjust timeout.
     * @return Lost partition ID.
     * @throws Exception If failed.
     */
    private Set<Integer> prepareTopology(int nodes, boolean autoAdjust, P1<Event> lsnr, int... stopNodes) throws Exception {
        final IgniteEx crd = startGrids(nodes);
        crd.cluster().baselineAutoAdjustEnabled(autoAdjust);
        crd.cluster().active(true);

        Affinity<Object> aff = ignite(0).affinity(CACHES[0]);

        for (int i = 0; i < aff.partitions(); i++) {
            for (String cacheName0 : CACHES)
                ignite(0).cache(cacheName0).put(i, i);
        }

        client = true;

        startGrid(nodes);

        client = false;

        for (int i = 0; i < 5; i++)
            info(">>> Node [idx=" + i + ", nodeId=" + ignite(i).cluster().localNode().id() + ']');

        awaitPartitionMapExchange();

        // TODO streamify
        Set<Integer> expLostParts = new LinkedHashSet<>();

        int[] stopNodesSorted = Arrays.copyOf(stopNodes, stopNodes.length);
        Arrays.sort(stopNodesSorted);

        // Find partitions not owned by any remaining node.
        for (int i = 0; i < PARTS_CNT; i++) {
            int c = 0;

            for (int idx = 0; idx < nodes; idx++) {
                if (Arrays.binarySearch(stopNodesSorted, idx) < 0 && !aff.isPrimary(grid(idx).localNode(), i) && !aff.isBackup(grid(idx).localNode(), i))
                    c++;
            }

            if (c == nodes - stopNodes.length)
                expLostParts.add(i);
        }

        assertFalse("Expecting lost partitions for the test scneario", expLostParts.isEmpty());

        for (Ignite ignite : G.allGrids())
            ignite.events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        for (int i = 0; i < stopNodes.length; i++)
            stopGrid(stopNodes[i], true);

        return expLostParts;
    }
}
