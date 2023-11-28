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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MIN_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 *
 */
public class AffinityHistoryCleanupTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** */
    private static ListeningTestLogger srvTestLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] ccfgs = new CacheConfiguration[4];

        for (int i = 0; i < ccfgs.length; i++) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setName("static-cache-" + i);
            ccfg.setAffinity(new RendezvousAffinityFunction());

            ccfgs[i] = ccfg;
        }

        cfg.setCacheConfiguration(ccfgs);

        cfg.setClientMode(client);

        if (srvTestLog != null)
            cfg.setGridLogger(srvTestLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (srvTestLog != null)
            srvTestLog.clearListeners();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_AFFINITY_HISTORY_SIZE, value = "5")
    public void testAffinityHistoryCleanup() throws Exception {
        Ignite ignite = startGrid(0);

        checkHistory(ignite, F.asList(topVer(1, 0)), 1); //fullHistSize = 1

        startGrid(1);

        checkHistory(ignite, F.asList(
            topVer(1, 0), // FullHistSize = 1.
            topVer(2, 0), // FullHistSize = 2.
            topVer(2, 1)), // FullHistSize = 3.
            3);

        startGrid(2);

        checkHistory(ignite, F.asList(
            topVer(1, 0), // FullHistSize = 1.
            topVer(2, 0), // FullHistSize = 2.
            topVer(2, 1), // FullHistSize = 3.
            topVer(3, 0), // FullHistSize = 4.
            topVer(3, 1)), // FullHistSize = 5.
            5);

        startGrid(3);

        checkHistory(ignite, F.asList(
            topVer(2, 1), // FullHistSize = 3.
            topVer(3, 0), // FullHistSize = 4.
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1)), // FullHistSize = 6 - 1 = 5.
            5);

        client = true;

        startGrid(4);

        stopGrid(4);

        checkHistory(ignite, F.asList(
            topVer(2, 1), // FullHistSize = 3.
            topVer(3, 0), // FullHistSize = 4.
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1), // FullHistSize = 6 - 1 = 5.
            topVer(5, 0), // Client event -> FullHistSize = 5.
            topVer(6, 0)), // Client event -> FullHistSize = 5.
            5);

        startGrid(4);

        stopGrid(4);

        checkHistory(ignite, F.asList(
            topVer(2, 1), // FullHistSize = 3.
            topVer(3, 0), // FullHistSize = 4.
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1), // FullHistSize = 6 - 1 = 5.
            topVer(5, 0), // Client event -> FullHistSize = 5.
            topVer(6, 0), // Client event -> FullHistSize = 5.
            topVer(7, 0), // Client event -> FullHistSize = 5.
            topVer(8, 0)), // Client event -> FullHistSize = 5.
            5);

        startGrid(4);

        stopGrid(4);

        checkHistory(ignite, F.asList(
            topVer(2, 1), // FullHistSize = 3.
            topVer(3, 0), // FullHistSize = 4.
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1), // FullHistSize = 6 - 1 = 5.
            topVer(5, 0), // Client event -> FullHistSize = 5.
            topVer(6, 0), // Client event -> FullHistSize = 5.
            topVer(7, 0), // Client event -> FullHistSize = 5.
            topVer(8, 0), // Client event -> FullHistSize = 5.
            topVer(9, 0), // Client event -> FullHistSize = 5.
            topVer(10, 0)), // Client event -> FullHistSize = 5.
            5);

        client = false;

        startGrid(4);

        checkHistory(ignite, F.asList(
            topVer(3, 1), // FullHistSize = 5.
            topVer(4, 0), // FullHistSize = 6 - 1 = 5.
            topVer(4, 1), // FullHistSize = 6 - 1 = 5.
            topVer(5, 0), // Client event -> FullHistSize = 5.
            topVer(6, 0), // Client event -> FullHistSize = 5.
            topVer(7, 0), // Client event -> FullHistSize = 5.
            topVer(8, 0), // Client event -> FullHistSize = 5.
            topVer(9, 0), // Client event -> FullHistSize = 5.
            topVer(10, 0), // Client event -> FullHistSize = 5.
            topVer(11, 0), // FullHistSize = 6 - 1 = 5.
            topVer(11, 1)), // FullHistSize = 6 - 1 = 5.
            5);

        stopGrid(4);

        startGrid(4);

        checkHistory(ignite, F.asList(
            topVer(11, 0), // FullHistSize = 5.
            topVer(11, 1), // FullHistSize = 5.
            topVer(12, 0), // FullHistSize = 6 - 1 = 5.
            topVer(13, 0), // FullHistSize = 5.
            topVer(13, 1)), // FullHistSize = 6 - 1 = 5.
            5);
    }

    /**
     * @param ignite Node.
     * @param expHist Expected history.
     * @param expSize Expected 'non client events' history size.
     * @throws Exception If failed.
     */
    private void checkHistory(Ignite ignite, List<AffinityTopologyVersion> expHist, int expSize) throws Exception {
        awaitPartitionMapExchange();

        GridCacheProcessor proc = ((IgniteKernal)ignite).context().cache();

        int cnt = 0;

        for (GridCacheContext cctx : proc.context().cacheContexts()) {
            GridAffinityAssignmentCache aff = GridTestUtils.getFieldValue(cctx.affinity(), "aff");

            AtomicInteger fullHistSize = GridTestUtils.getFieldValue(aff, "nonShallowHistSize");

            assertEquals(expSize, fullHistSize.get());

            Map<AffinityTopologyVersion, Object> cache = GridTestUtils.getFieldValue(aff, "affCache");

            assertEquals("Unexpected history: " + cache.keySet(), expHist.size(), cache.size());

            for (AffinityTopologyVersion topVer : expHist)
                assertTrue("No history [ver=" + topVer + ", hist=" + cache.keySet() + ']', cache.containsKey(topVer));

            cnt++;
        }

        assert cnt > 4;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_AFFINITY_HISTORY_SIZE, value = "1")
    @WithSystemProperty(key = IGNITE_MIN_AFFINITY_HISTORY_SIZE, value = "2")
    public void testOldNonShallowAffinityHistoryIsRemoved() throws Exception {
        testNonShallowAffinityHistory(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_AFFINITY_HISTORY_SIZE, value = "1")
    @WithSystemProperty(key = IGNITE_MIN_AFFINITY_HISTORY_SIZE, value = "3")
    public void testNonShallowAffinityHistoryIsNotRemoved() throws Exception {
        testNonShallowAffinityHistory(false);
    }

    private void testNonShallowAffinityHistory(boolean isRemoved) throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1)));

        GridCacheProcessor proc = ignite.context().cache();

        GridCacheContext<Object, Object> cctx = proc.context().cacheContext(CU.cacheId(DEFAULT_CACHE_NAME));

        GridCacheAffinityManager affMgr = cctx.affinity();

        AffinityTopologyVersion topVer = affMgr.affinityTopologyVersion();

        stopGrid(1);
        startGrid(1);

        ignite.createCache("cache1");

        if (isRemoved)
            assertThrowsWithCause(() -> affMgr.nodesByPartition(0, topVer), IllegalStateException.class);
        else
            assertEquals(1, affMgr.nodesByPartition(0, topVer).size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_AFFINITY_HISTORY_SIZE, value = "1")
    public void testDebugLoggingOnRemovingAffinityVersions() throws Exception {
        setRootLoggerDebugLevel();

        srvTestLog = new ListeningTestLogger(log);

        LogListener logLsnr0 = LogListener.matches(
                "Removed affinity assignments for group [name=default, nonShallowSize=3, totalSize=14," +
                    " version=[topVer=3, minorTopVer=0]]"
            )
            .times(1)
            .build();

        LogListener logLsnr1 = LogListener.matches(
                "Removed affinity assignments for group [name=default, nonShallowSize=3, totalSize=14," +
                    " from=[topVer=4, minorTopVer=0], to=[topVer=4, minorTopVer=11]]"
            )
            .times(1)
            .build();

        srvTestLog.registerListener(logLsnr0);
        srvTestLog.registerListener(logLsnr1);

        IgniteEx ignite = startGrids(2);

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1)));

        stopGrid(1);
        startGrid(1);

        for (int i = 0; i < 5; i++) {
            ignite.createCache("cache1");
            ignite.destroyCache("cache1");
        }

        stopGrid(1);
        startGrid(1);

        assertTrue(logLsnr0.check());
        assertTrue(logLsnr1.check());
    }

    /**
     * @param major Major version.
     * @param minor Minor version.
     * @return Version.
     */
    private static AffinityTopologyVersion topVer(int major, int minor) {
        return new AffinityTopologyVersion(major, minor);
    }
}
