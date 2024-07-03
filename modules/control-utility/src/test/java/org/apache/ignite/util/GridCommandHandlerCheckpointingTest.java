/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Checks --checkpoint command.
 */
public class GridCommandHandlerCheckpointingTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** */
    @Before
    public void init() {
        injectTestSystemOut();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // This check is needed because one of the tests checks that checkpoint happenes only on one node.
        assertTrue("Need at least 2 nodes to test checkpoint command", SERVER_NODE_CNT >= 2);

        super.beforeTestsStarted();

        createCacheAndPreload(crd, 1);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration()
            .setCheckpointFrequency(Integer.MAX_VALUE);

        return cfg;
    }

    /**
     * Check the command '--checkpointing help'.
     */

    @Test
    public void testForceCheckpointing() {
        final GridCacheDatabaseSharedManager gridDb = (GridCacheDatabaseSharedManager)crd.context().cache().context().database();
        final CheckpointHistory checkpointHist = gridDb.checkpointHistory();
        assert null != checkpointHist;

        final int numOfCheckpointsBefore = checkpointHist.checkpoints().size();
        final CheckpointEntry lastCheckpointBefore = checkpointHist.lastCheckpoint();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint"));

        final int numOfCheckpointsAfter = checkpointHist.checkpoints().size();
        final CheckpointEntry lastCheckpointAfter = checkpointHist.lastCheckpoint();

        assertTrue(numOfCheckpointsAfter > numOfCheckpointsBefore);
        assertNotSame(lastCheckpointBefore, lastCheckpointAfter);

        final String out = testOut.toString();
        final int numOfNodes = crd.cluster().forServers().nodes().size();
        assertContains(log, out, "Checkpointing completed successfully on " + numOfNodes + " nodes.");
    }

    /**
     * Checks --node-id option
     */
    @Test
    public void testForceCheckpointingForNode() {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        CheckpointHistory checkpointHist0 = getCheckpointHistory(ignite0);
        CheckpointHistory checkpointHist1 = getCheckpointHistory(ignite1);

        int numOfCheckpointsBefore0 = checkpointHist0.checkpoints().size();
        int numOfCheckpointsBefore1 = checkpointHist1.checkpoints().size();

        CheckpointEntry lastCheckpointBefore0 = checkpointHist0.lastCheckpoint();
        CheckpointEntry lastCheckpointBefore1 = checkpointHist1.lastCheckpoint();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--node-id", crd.localNode().id().toString()));

        int numOfCheckpointsAfter0 = checkpointHist0.checkpoints().size();
        int numOfCheckpointsAfter1 = checkpointHist1.checkpoints().size();

        CheckpointEntry lastCheckpointAfter0 = checkpointHist0.lastCheckpoint();
        CheckpointEntry lastCheckpointAfter1 = checkpointHist1.lastCheckpoint();

        // check that checkpoint happened only on one node
        assertTrue(numOfCheckpointsAfter0 > numOfCheckpointsBefore0);
        assertNotSame(lastCheckpointBefore0, lastCheckpointAfter0);

        assertTrue(numOfCheckpointsAfter1 == numOfCheckpointsBefore1);
        assertSame(lastCheckpointBefore1, lastCheckpointAfter1);

        String out = testOut.toString();
        assertContains(log, out, "Checkpointing completed successfully on " + 1 + " node.");
    }

    /**  */
    private CheckpointHistory getCheckpointHistory(IgniteEx ignite) {
        GridCacheDatabaseSharedManager gridDb0 = (GridCacheDatabaseSharedManager)ignite.context().cache().context().database();

        CheckpointHistory checkpointHist0 = gridDb0.checkpointHistory();

         assertNotNull("Checkpoint history object for " + ignite.name(), checkpointHist0);

        return checkpointHist0;
    }
}
