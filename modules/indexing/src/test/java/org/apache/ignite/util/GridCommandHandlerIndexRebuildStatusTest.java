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

package org.apache.ignite.util;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusInfoContainer;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.deleteIndexBin;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillThreeFieldsEntryCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.simpleIndexEntry;

/**
 * Test for --cache index_status command. Uses single cluster per suite.
 */
public class GridCommandHandlerIndexRebuildStatusTest extends GridCommandHandlerAbstractTest {
    /** Number of indexes that are being rebuilt. */
    private static AtomicInteger idxRebuildsStartedNum = new AtomicInteger();

    /** Is set to {@code True} when connamd was completed. */
    private static AtomicBoolean statusRequestingFinished = new AtomicBoolean();

    /** Is set to {@code True} if cluster should be resterted before next test*/
    private static boolean clusterRestartRequeired;

    /** Grids number. */
    public static final int GRIDS_NUM = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startupTestCluster();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        shutdownTestCluster();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (clusterRestartRequeired) {
            shutdownTestCluster();

            startupTestCluster();

            clusterRestartRequeired = false;
        }

        idxRebuildsStartedNum.set(0);
        statusRequestingFinished.set(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        boolean allIndexesRebuilt = GridTestUtils.waitForCondition(() -> idxRebuildsStartedNum.get() == 0, 30_000);

        super.afterTest();

        if (!allIndexesRebuilt) {
            clusterRestartRequeired = true;

            fail("Failed to wait for index rebuild");
        }
    }

    /** */
    private void startupTestCluster() throws Exception {
        cleanPersistenceDir();

        Ignite ignite = startGrids(GRIDS_NUM);

        ignite.cluster().active(true);

        createAndFillCache(ignite, "cache1", "group2");
        createAndFillCache(ignite, "cache2", "group1");
        createAndFillThreeFieldsEntryCache(ignite, "cache_no_group", null, asList(simpleIndexEntry()));
    }

    /** */
    private void shutdownTestCluster() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Basic check.
     */
    @Test
    public void testCommandOutput() throws Exception {
        injectTestSystemOut();
        idxRebuildsStartedNum.set(0);

        final CommandHandler handler = new CommandHandler(createTestLogger());

        stopGrid(GRIDS_NUM - 1);
        stopGrid(GRIDS_NUM - 2);

        deleteIndexBin(getTestIgniteInstanceName(GRIDS_NUM - 1));
        deleteIndexBin(getTestIgniteInstanceName(GRIDS_NUM - 2));

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        IgniteEx ignite1 = startGrid(GRIDS_NUM - 1);

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        IgniteEx ignite2 = startGrid(GRIDS_NUM - 2);

        final UUID id1 = ignite1.localNode().id();
        final UUID id2 = ignite2.localNode().id();

        boolean allRebuildsStarted = GridTestUtils.waitForCondition(() -> idxRebuildsStartedNum.get() == 6, 30_000);
        assertTrue("Failed to wait for all indexes to start being rebuilt", allRebuildsStarted);

        assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_rebuild_status"));

        statusRequestingFinished.set(true);

        checkResult(handler, id1, id2);
    }

    /** */
    @Test
    public void testNodeIdOption() throws Exception {
        injectTestSystemOut();
        idxRebuildsStartedNum.set(0);

        final CommandHandler handler = new CommandHandler(createTestLogger());

        stopGrid(GRIDS_NUM - 1);
        stopGrid(GRIDS_NUM - 2);

        deleteIndexBin(getTestIgniteInstanceName(GRIDS_NUM - 1));
        deleteIndexBin(getTestIgniteInstanceName(GRIDS_NUM - 2));

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        IgniteEx ignite1 = startGrid(GRIDS_NUM - 1);

        GridQueryProcessor.idxCls = BlockingIndexing.class;
        startGrid(GRIDS_NUM - 2);

        final UUID id1 = ignite1.localNode().id();

        boolean allRebuildsStarted = GridTestUtils.waitForCondition(() -> idxRebuildsStartedNum.get() == 6, 30_000);
        assertTrue("Failed to wait for all indexes to start being rebuilt", allRebuildsStarted);

        assertEquals(EXIT_CODE_OK, execute(handler, "--cache", "indexes_rebuild_status", "--node-id", id1.toString()));

        statusRequestingFinished.set(true);

        checkResult(handler, id1);
    }

    /**
     * Checks command output when no indexes are being rebuilt.
     */
    @Test
    public void testEmptyResult() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "indexes_rebuild_status"));

        String output = testOut.toString();

        assertTrue("Expected message not found in output",
            output.contains("There are no caches that have index rebuilding in progress."));
    }

    /**
     * Checks that info about all {@code nodeIds} and only about them is present
     * in {@code handler} last operetion result and in {@code testOut}.
     *
     * @param handler CommandHandler used to run command.
     * @param nodeIds Ids to check.
     */
    private void checkResult(CommandHandler handler, UUID... nodeIds) {
        String output = testOut.toString();

        Map<UUID, Set<IndexRebuildStatusInfoContainer>> cmdResult = handler.getLastOperationResult();
        assertNotNull(cmdResult);
        assertEquals("Unexpected number of nodes in result", nodeIds.length, cmdResult.size());

        for (UUID nodeId: nodeIds) {
            Set<IndexRebuildStatusInfoContainer> cacheIndos = cmdResult.get(nodeId);
            assertNotNull(cacheIndos);
            assertEquals("Unexpected number of cacheIndos in result", 3, cacheIndos.size());

            final String nodeStr = "node_id=" + nodeId + ", groupName=group1, cacheName=cache2\n" +
                "node_id=" + nodeId + ", groupName=group2, cacheName=cache1\n" +
                "node_id=" + nodeId + ", groupName=no_group, cacheName=cache_no_group";

            assertTrue("Unexpected output", output.contains(nodeStr));
        }
    }

    /**
     * Indexing that blocks index rebuild until starus request is completed.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        /** {@inheritDoc} */
        @Override protected void rebuildIndexesFromHash0(GridCacheContext cctx,SchemaIndexCacheVisitorClosure clo)
            throws IgniteCheckedException
        {
            idxRebuildsStartedNum.incrementAndGet();

            GridTestUtils.waitForCondition(() -> statusRequestingFinished.get(), 60_000);

            super.rebuildIndexesFromHash0(cctx, clo);

            idxRebuildsStartedNum.decrementAndGet();
        }
    }
}
