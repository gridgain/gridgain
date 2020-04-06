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
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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

    /** */
    private static final String GROUP_NAME = "group";

    /** */
    private static final String CACHE_1 = "cache1";

    /** */
    private static final String CACHE_2 = "cache2";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        CacheConfiguration ccfg1 = new CacheConfiguration(CACHE_1)
            .setGroupName(GROUP_NAME)
            .setCacheMode(PARTITIONED)
            .setBackups(0)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setPartitionLossPolicy(partLossPlc)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        CacheConfiguration ccfg2 = new CacheConfiguration(ccfg1)
            .setName(CACHE_2);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
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
        partLossPlc = READ_ONLY_SAFE; // Should use safe policy.

        checkLostPartition(true, true, 4, 3);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkLostPartition(boolean autoAdjust, boolean safe, int nodes, int... stopNodes) throws Exception {
        String cacheName = ThreadLocalRandom.current().nextBoolean() ? CACHE_1 : CACHE_2;

        List<Integer> partEvts = Collections.synchronizedList(new ArrayList<>());

        Collection<Integer> expLostParts = prepareTopology(nodes, autoAdjust, new P1<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt.type() == EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

                CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent)evt;

                partEvts.add(cacheEvt.partition());

                return true;
            }
        }, stopNodes);

        for (Ignite ig : G.allGrids()) {
            info("Checking node: " + ig.cluster().localNode().id());

            verifyCacheOps(cacheName, expLostParts, ig, safe);
        }

        if (safe) {
            assertTrue(partEvts.toString(), GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return expLostParts.size() * 2 * G.allGrids().size() == partEvts.size();
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

        for (Ignite ig : G.allGrids())
            verifyCacheOps(cacheName, expLostParts, ig, safe);

        if (safe)
            ignite(0).resetLostPartitions(F.asList(CACHE_1, CACHE_2));

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
    private void verifyCacheOps(String cacheName, Collection<Integer> expLostParts, Ignite ig, boolean safe) {
        boolean readOnly = partLossPlc == READ_ONLY_SAFE || partLossPlc == READ_ONLY_ALL;

        IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

        int parts = ig.affinity(cacheName).partitions();

        if (!safe)
            assertTrue(cache.lostPartitions().isEmpty());

        // Check read.
        for (int p = 0; p < parts; p++) {
            try {
                Integer actual = cache.get(p);

                if (safe) {
                    assertTrue("Reading from a lost partition should have failed [part=" + p + ']',
                            !cache.lostPartitions().contains(p));

                    if (actual == null)
                        System.out.println();

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

        // Check write.
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
    }

    /**
     * @param autoAdjustTimeout Auto-adjust timeout.
     * @return Lost partition ID.
     * @throws Exception If failed.
     */
    private Collection<Integer> prepareTopology(int nodes, boolean autoAdjust, P1<Event> lsnr, int... stopNodes) throws Exception {
        final IgniteEx crd = startGrids(nodes);
        crd.cluster().baselineAutoAdjustEnabled(autoAdjust);
        crd.cluster().active(true);

        final String cacheName = ThreadLocalRandom.current().nextBoolean() ? CACHE_1 : CACHE_2;

        Affinity<Object> aff = ignite(0).affinity(cacheName);

        for (int i = 0; i < aff.partitions(); i++) {
            ignite(0).cache(CACHE_1).put(i, i);
            ignite(0).cache(CACHE_2).put(i, i);
        }

        client = true;

        startGrid(nodes);

        client = false;

        for (int i = 0; i < 5; i++)
            info(">>> Node [idx=" + i + ", nodeId=" + ignite(i).cluster().localNode().id() + ']');

        awaitPartitionMapExchange();

        // TODO streamify
        Set<Integer> expLostParts = new LinkedHashSet<>();

        // Find partitions not owned by any alive nodes.
        for (int i = 0; i < PARTS_CNT; i++) {
            int c = 0;

            for (int idx = 0; idx < nodes; idx++) {
                if (Arrays.binarySearch(stopNodes, idx) < 0 && !aff.isPrimary(grid(idx).localNode(), i) && !aff.isBackup(grid(idx).localNode(), i))
                    c++;
            }

            if (c == nodes - stopNodes.length)
                expLostParts.add(i);
        }

        assertFalse("No partitions on the nodes", expLostParts.isEmpty());

        for (Ignite ignite : G.allGrids())
            ignite.events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        for (int i = 0; i < stopNodes.length; i++)
            stopGrid(stopNodes[i], true);

        return expLostParts;
    }
}
