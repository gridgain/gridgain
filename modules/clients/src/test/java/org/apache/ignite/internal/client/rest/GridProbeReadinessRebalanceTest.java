/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.rest;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Multi-node HTTP-level test for the core behaviour of
 * {@code cmd=probe&kind=readiness}: a (re)joining node that is still
 * <em>demanding</em> (inbound-rebalancing) its partitions reports 503, and only
 * reports 200 once its initial rebalance completes — then stays 200 (latched)
 * across later topology rebalances.
 *
 * <p>This is the regression that the bare {@code cmd=probe} (kernel-started only)
 * misses: a (re)joining node has its join PME done while it is still pulling its
 * partitions, so a kernel-only check reports ready — exactly the
 * bounce-the-next-pod trigger for Lost Partitions.</p>
 *
 * <h3>Rebalance-stall recipe (read before editing)</h3>
 * <ul>
 *   <li><b>3 servers + 1 joiner</b>, persistence on, {@code backups=1} so the
 *       joiner has a real demand exchange.</li>
 *   <li><b>{@link CacheRebalanceMode#ASYNC}</b> (not {@code SYNC}) so the joiner's
 *       {@code onKernalStart} does not block on rebalance and its REST endpoint
 *       comes up while it is still demanding.</li>
 *   <li><b>Baseline auto-adjust</b> so the joiner is added to the baseline (and
 *       thus assigned partitions to demand) as soon as it joins.</li>
 *   <li><b>Block the joiner's own {@link GridDhtPartitionDemandMessage}</b> for the
 *       test cache group via {@link TestRecordingCommunicationSpi} (installed in
 *       {@link #getConfiguration}). Only the test group is blocked so system
 *       caches finish and the joiner completes start.</li>
 *   <li>Each node's Jetty REST binds the first free port from 8080, so by start
 *       order node {@code i} → port {@code 8080 + i}; the joiner (node 3) → 8083.</li>
 * </ul>
 */
public class GridProbeReadinessRebalanceTest extends GridCommonAbstractTest {
    /** Cache name / group used for the rebalance-controlled cache group. */
    private static final String CACHE = "test";

    /** Jetty REST port of the joiner node (node 3, 4th node started → 8083). */
    private static final int JOINER_PORT = 8083;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        // Joiner nodes (index >= 3): hold their inbound demand for the test group so
        // they stay mid-rebalance until the test releases them.
        if (trailingIdx(igniteInstanceName) >= 3) {
            spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionDemandMessage
                && ((GridDhtPartitionDemandMessage)msg).groupId() == CU.cacheId(CACHE));
        }

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * Demander mid-rebalance → 503; after rebalance → 200; latched stays 200
     * during a later rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReadinessGatesInboundRebalanceAndLatches() throws Exception {
        seedCluster();

        // --- Stage 1: joiner mid inbound-rebalance -> 503 ---------------------------------
        // ASYNC rebalance: startGrid returns after kernel start; demand is held by the SPI.
        startGrid(3);

        TestRecordingCommunicationSpi.spi(grid(3)).waitForBlocked();

        GridRestHttpClient.Response demanding = GridRestHttpClient.get(JOINER_PORT, "/ignite?cmd=probe&kind=readiness");

        log.info("joiner readiness while demanding: code=" + demanding.code + " body=" + demanding.body);

        assertEquals(503, demanding.code);
        assertEquals(503, demanding.body.get("successStatus"));
        assertEquals("rebalance in progress", demanding.body.get("error"));

        // BC pin: bare cmd=probe stays 200 on the demander (kernel-only check).
        GridRestHttpClient.Response bare = GridRestHttpClient.get(JOINER_PORT, "/ignite?cmd=probe");
        assertEquals("bare cmd=probe must remain 200 while demanding (BC): " + bare.body, 200, bare.code);
        assertEquals("grid has started", bare.body.get("response"));

        // Liveness stays 200 too — a rebalancing node is alive.
        GridRestHttpClient.Response live = GridRestHttpClient.get(JOINER_PORT, "/ignite?cmd=probe&kind=liveness");
        assertEquals("liveness must remain 200 while demanding: " + live.body, 200, live.code);
        assertEquals("grid has started", live.body.get("response"));

        // --- Stage 2: release rebalance -> joiner becomes ready ---------------------------
        TestRecordingCommunicationSpi.spi(grid(3)).stopBlock();

        awaitPartitionMapExchange();

        GridRestHttpClient.Response ready = GridRestHttpClient.get(JOINER_PORT, "/ignite?cmd=probe&kind=readiness");

        log.info("joiner readiness after rebalance: code=" + ready.code + " body=" + ready.body);

        assertEquals(200, ready.code);
        assertEquals(0, ready.body.get("successStatus"));
        assertEquals("ready", ready.body.get("response"));

        // --- Stage 3: a LATER rebalance must NOT flip the joiner back to 503 (latched) ----
        // Start node 4 with its demand blocked: the cluster is no longer fully rebalanced,
        // yet node 3 is already latched and must stay 200.
        startGrid(4);

        TestRecordingCommunicationSpi.spi(grid(4)).waitForBlocked();

        for (int i = 0; i < 3; i++) {
            GridRestHttpClient.Response latched = GridRestHttpClient.get(JOINER_PORT, "/ignite?cmd=probe&kind=readiness");

            assertEquals("latched readiness must stay 200 during a later rebalance: " + latched.body,
                200, latched.code);
            assertEquals("ready", latched.body.get("response"));
        }

        TestRecordingCommunicationSpi.spi(grid(4)).stopBlock();

        awaitPartitionMapExchange();

        GridRestHttpClient.Response afterAll = GridRestHttpClient.get(JOINER_PORT, "/ignite?cmd=probe&kind=readiness");
        assertEquals(200, afterAll.code);
        assertEquals("ready", afterAll.body.get("response"));
    }

    /**
     * Bring up nodes 0/1/2, activate, enable baseline auto-adjust, create a
     * partitioned cache with backups configured for ASYNC rebalance, seed it, and
     * let the initial rebalance settle.
     *
     * @throws Exception If failed.
     */
    private void seedCluster() throws Exception {
        IgniteEx g0 = startGrid(0);
        startGrid(1);
        startGrid(2);

        g0.cluster().state(ClusterState.ACTIVE);

        g0.cluster().baselineAutoAdjustEnabled(true);
        g0.cluster().baselineAutoAdjustTimeout(0);

        IgniteCache<Integer, Integer> cache = g0.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(CACHE)
                .setBackups(1)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setRebalanceBatchSize(256));

        for (int i = 0; i < 5_000; i++)
            cache.put(i, i);

        awaitPartitionMapExchange();
    }

    /**
     * @param name Ignite instance name.
     * @return Trailing integer index of the instance name, or {@code -1} if none.
     */
    private static int trailingIdx(String name) {
        int i = name.length();

        while (i > 0 && Character.isDigit(name.charAt(i - 1)))
            i--;

        return i == name.length() ? -1 : Integer.parseInt(name.substring(i));
    }
}
