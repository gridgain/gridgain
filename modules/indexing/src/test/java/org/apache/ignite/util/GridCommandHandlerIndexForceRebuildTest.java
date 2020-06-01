/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.complexIndexEntity;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillThreeFieldsEntryCache;

/**
 * Test for --cache indexes_force_rebuild command. Uses single cluster per suite.
 */
public class GridCommandHandlerIndexForceRebuildTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String CACHE_NAME_1_1 = "cache_1_1";
    /** */
    private static final String CACHE_NAME_1_2 = "cache_1_2";
    /** */
    private static final String CACHE_NAME_2_1 = "cache_2_1";
    /** */
    private static final String CACHE_NAME_NO_GRP = "cache_no_group";
    /** */
    private static final String CACHE_NAME_NON_EXISTING = "non_existing_cache";

    /** */
    private static final String GRP_NAME_1 = "group_1";
    /** */
    private static final String GRP_NAME_2 = "group_2";

    /** */
    private static final String GRP_NAME_NON_EXISTING = "non_existing_group";

    /** */
    private static final int GRIDS_NUM = 3;

    /** */
    private static final int LAST_NODE_NUM = GRIDS_NUM - 1;

    /**
     * Set containing names of caches for which index rebuild should be blocked.
     * See {@link BlockingIndexing}.
     */
    private static Set<String> cacheNamesBlockedIdxRebuild = new GridConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startupTestCluster();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cacheNamesBlockedIdxRebuild.clear();
    }

    /** */
    private void startupTestCluster() throws Exception {
        for (int i = 0; i < GRIDS_NUM; i++ ) {
            GridQueryProcessor.idxCls = BlockingIndexing.class;
            startGrid(i);
        }

        IgniteEx ignite = grid(0);

        ignite.cluster().active(true);

        createAndFillCache(ignite, CACHE_NAME_1_1, GRP_NAME_1);
        createAndFillCache(ignite, CACHE_NAME_1_2, GRP_NAME_1);
        createAndFillCache(ignite, CACHE_NAME_2_1, GRP_NAME_2);

        createAndFillThreeFieldsEntryCache(ignite, CACHE_NAME_NO_GRP, null, Collections.singletonList(complexIndexEntity()));
    }

    /**
     * Checks error messages when trying to rebuild indexes for
     * non-existent cache of group.
     */
    @Test
    public void testEmptyResult() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
            "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
            "--cache-names", CACHE_NAME_NON_EXISTING));

        String cacheNamesOutputStr = testOut.toString();

        assertTrue(cacheNamesOutputStr.contains("WARNING: Indexes rebuild was not started for any cache. Check command input."));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
            "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
            "--group-names", GRP_NAME_NON_EXISTING));

        String grpNamesOutputStr = testOut.toString();

        assertTrue(grpNamesOutputStr.contains("WARNING: Indexes rebuild was not started for any cache. Check command input."));
    }

    /**
     * Checks that index on 2 fields is rebuilt correctly.
     */
    @Test
    public void testComplexIndexRebuild() throws IgniteInterruptedCheckedException {
        injectTestSystemOut();

        LogListener lsnr = installRebuildCheckListener(grid(LAST_NODE_NUM), CACHE_NAME_NO_GRP);

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
            "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
            "--cache-names", CACHE_NAME_NO_GRP));

        assertTrue(waitForIndexesRebuild(grid(LAST_NODE_NUM)));

        assertTrue(lsnr.check());

        removeLogListener(grid(LAST_NODE_NUM), lsnr);
    }

    /**
     * Checks --node-id and --cache-names options,
     * correctness of utility output and the fact that indexes were actually rebuilt.
     */
    @Test
    public void testCacheNamesArg() throws Exception {
        cacheNamesBlockedIdxRebuild.add(CACHE_NAME_2_1);

        injectTestSystemOut();

        LogListener[] cache1Listeners = new LogListener[GRIDS_NUM];
        LogListener[] cache2Listeners = new LogListener[GRIDS_NUM];

        try {
            triggerIndexRebuild(LAST_NODE_NUM, Collections.singletonList(CACHE_NAME_2_1));

            for (int i = 0; i < GRIDS_NUM; i++) {
                cache1Listeners[i] = installRebuildCheckListener(grid(i), CACHE_NAME_1_1);
                cache2Listeners[i] = installRebuildCheckListener(grid(i), CACHE_NAME_1_2);
            }

            assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
                "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
                "--cache-names", CACHE_NAME_1_1 + "," + CACHE_NAME_2_1 + "," + CACHE_NAME_NON_EXISTING));

            cacheNamesBlockedIdxRebuild.remove(CACHE_NAME_2_1);

            waitForIndexesRebuild(grid(LAST_NODE_NUM));

            validateTestCacheNamesArgOutput();

            // Index rebuild must be triggered only for cache1_1 and only on node3.
            assertFalse(cache1Listeners[0].check());
            assertFalse(cache1Listeners[1].check());
            assertTrue(cache1Listeners[LAST_NODE_NUM].check());

            for (LogListener cache2Lsnr: cache2Listeners)
                assertFalse(cache2Lsnr.check());
        }
        finally {
            cacheNamesBlockedIdxRebuild.remove(CACHE_NAME_2_1);

            for (int i = 0; i < GRIDS_NUM; i++) {
                removeLogListener(grid(i), cache1Listeners[i]);
                removeLogListener(grid(i), cache2Listeners[i]);
            }

            assertTrue(waitForIndexesRebuild(grid(LAST_NODE_NUM)));
        }
    }

    /**
     * Checks --node-id and --group-names options,
     * correctness of utility output and the fact that indexes were actually rebuilt.
     */
    @Test
    public void testGroupNamesArg() throws Exception {
        cacheNamesBlockedIdxRebuild.add(CACHE_NAME_1_2);

        injectTestSystemOut();

        LogListener[] cache1Listeners = new LogListener[GRIDS_NUM];
        LogListener[] cache2Listeners = new LogListener[GRIDS_NUM];

        try {
            triggerIndexRebuild(LAST_NODE_NUM, Collections.singletonList(CACHE_NAME_1_2));

            for (int i = 0; i < GRIDS_NUM; i++) {
                cache1Listeners[i] = installRebuildCheckListener(grid(i), CACHE_NAME_1_1);
                cache2Listeners[i] = installRebuildCheckListener(grid(i), CACHE_NAME_NO_GRP);
            }

            assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_force_rebuild",
                "--node-id", grid(LAST_NODE_NUM).localNode().id().toString(),
                "--group-names", GRP_NAME_1 + "," + GRP_NAME_2 + "," + GRP_NAME_NON_EXISTING));

            cacheNamesBlockedIdxRebuild.remove(CACHE_NAME_1_2);

            waitForIndexesRebuild(grid(LAST_NODE_NUM));

            validateTestCacheGroupArgOutput();

            assertFalse(cache1Listeners[0].check());
            assertFalse(cache1Listeners[1].check());
            assertTrue(cache1Listeners[LAST_NODE_NUM].check());

            for (LogListener cache2Lsnr: cache2Listeners)
                assertFalse(cache2Lsnr.check());
        }
        finally {
            cacheNamesBlockedIdxRebuild.remove(CACHE_NAME_1_2);

            for (int i = 0; i < GRIDS_NUM; i++) {
                removeLogListener(grid(i), cache1Listeners[i]);
                removeLogListener(grid(i), cache2Listeners[i]);
            }

            assertTrue(waitForIndexesRebuild(grid(LAST_NODE_NUM)));
        }
    }

    /**
     * Validated control.sh utility output for {@link #testCacheNamesArg()}.
     */
    private void validateTestCacheNamesArgOutput() {
        String outputStr = testOut.toString();

        assertTrue(outputStr.contains("WARNING: These caches were not found:\n" +
            "  " + CACHE_NAME_NON_EXISTING));

        assertTrue(outputStr.contains("WARNING: These caches have indexes rebuilding in progress:\n" +
            "  groupName=" + GRP_NAME_2 + ", cacheName=" + CACHE_NAME_2_1));

        assertTrue(outputStr.contains("Indexes rebuild was started for these caches:\n" +
            "  groupName=" + GRP_NAME_1 + ", cacheName=" + CACHE_NAME_1_1));

        assertEquals("Unexpected number of lines in output.", 19, outputStr.split("\n").length);
    }

    /**
     * Validated control.sh utility output for {@link #testGroupNamesArg()}.
     */
    private void validateTestCacheGroupArgOutput() {
        String outputStr = testOut.toString();

        assertTrue(outputStr.contains("WARNING: These cache groups were not found:\n" +
            "  " + GRP_NAME_NON_EXISTING));

        assertTrue(outputStr.contains("WARNING: These caches have indexes rebuilding in progress:\n" +
            "  groupName=" + GRP_NAME_1 + ", cacheName=" + CACHE_NAME_1_2));

        assertTrue(outputStr.contains("Indexes rebuild was started for these caches:\n" +
            "  groupName=" + GRP_NAME_1 + ", cacheName=" + CACHE_NAME_1_1 + "\n" +
            "  groupName=" + GRP_NAME_2 + ", cacheName=" + CACHE_NAME_2_1));

        assertEquals("Unexpected number of lines in output.", 20, outputStr.split("\n").length);
    }

    /**
     * Triggers indexes rebuild for ALL caches on grid node with index {@code igniteIdx}.
     *
     * @param igniteIdx Node index.
     * @param excludedCacheNames Collection of cache names for which
     *  end of index rebuilding is not awaited.
     * @throws Exception if failed.
     */
    private void triggerIndexRebuild(int igniteIdx, Collection<String> excludedCacheNames) throws Exception {
        stopGrid(igniteIdx);

        GridTestUtils.deleteIndexBin(getTestIgniteInstanceName(2));

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        final IgniteEx ignite = startGrid(igniteIdx);

        resetBaselineTopology();
        awaitPartitionMapExchange();
        waitForIndexesRebuild(ignite, 30_000, excludedCacheNames);
    }

    /** */
    private boolean waitForIndexesRebuild(IgniteEx ignite) throws IgniteInterruptedCheckedException {
        return waitForIndexesRebuild(ignite, 30_000, Collections.emptySet());
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout timeout
     * @param excludedCacheNames Collection of cache names for which
     *  end of index rebuilding is not awaited.
     * @return {@code True} if index rebuild was completed before {@code timeout} was reached.
     * @throws IgniteInterruptedCheckedException if failed.
     */
    private boolean waitForIndexesRebuild(IgniteEx ignite, long timeout, Collection<String> excludedCacheNames)
        throws IgniteInterruptedCheckedException
    {
        return GridTestUtils.waitForCondition(
            () -> ignite.context().cache().publicCaches()
                .stream()
                .filter(c -> !excludedCacheNames.contains(c.getName()))
                .allMatch(c -> c.indexReadyFuture().isDone()),
            timeout);
    }

    /**
     * @param ignite IgniteEx instance.
     * @param cacheName Name of checked cache.
     * @return newly installed LogListener.
     */
    private LogListener installRebuildCheckListener(IgniteEx ignite, String cacheName) {
        final MessageOrderLogListener lsnr = new MessageOrderLogListener(
            new MessageOrderLogListener.MessageGroup(true)
                .add("Started indexes rebuilding for cache \\[name=" + cacheName + ".*")
                .add("Finished indexes rebuilding for cache \\[name=" + cacheName + ".*")
        );

        ListeningTestLogger impl = GridTestUtils.getFieldValue(ignite.log(), "impl");
        assertNotNull(impl);

        impl.registerListener(lsnr);

        return lsnr;
    }

    /** */
    private void removeLogListener(IgniteEx ignite, LogListener lsnr) {
        ListeningTestLogger impl = GridTestUtils.getFieldValue(ignite.log(), "impl");
        assertNotNull(impl);

        impl.unregisterListener(lsnr);
    }

    /**
     * Indexing that blocks index rebuild until status request is completed.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override protected void rebuildIndexesFromHash0(GridCacheContext cctx, SchemaIndexCacheVisitorClosure clo, GridFutureAdapter<Void> rebuildIdxFut)
        {
            super.rebuildIndexesFromHash0(cctx, clo, new BlockingRebuildIdxFuture(rebuildIdxFut, cctx));
        }
    }

    /**
     * Modified rebuild indexes future which is blocked right before finishing for specific caches.
     */
    private static class BlockingRebuildIdxFuture extends GridFutureAdapter<Void> {
        /** */
        private final GridFutureAdapter<Void> original;

        /** */
        private final GridCacheContext cctx;

        /** */
        BlockingRebuildIdxFuture(GridFutureAdapter<Void> original, GridCacheContext cctx) {
            this.original = original;
            this.cctx = cctx;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            try {
                assertTrue("Failed to wait for indexes rebuild unblocking",
                    GridTestUtils.waitForCondition(() -> !cacheNamesBlockedIdxRebuild.contains(cctx.name()), 60_000));
            }
            catch (IgniteInterruptedCheckedException e) {
                fail("Waiting for indexes rebuild unblocking was interrupted");
            }

            return original.onDone(res, err);
        }
    }
}
